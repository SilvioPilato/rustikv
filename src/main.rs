use rustikv::bffp::{Command, ResponseStatus, decode_input_frame, encode_frame};
use rustikv::engine::{RangeScan, StorageEngine};
use rustikv::kvengine::KVEngine;
use rustikv::leveled::Leveled;
use rustikv::lsmengine::LsmEngine;
use rustikv::record::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use rustikv::settings::{BlockCompression, EngineType, Settings, StorageStrategy};
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::env;
use std::io::{self};
use std::time::Duration;
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, atomic::Ordering},
    thread,
    time::Instant,
};

fn verbose_logging_enabled() -> bool {
    matches!(env::var("RUSTIKV_VERBOSE"), Ok(value) if value == "1")
}

fn log_verbose(message: impl AsRef<str>) {
    if verbose_logging_enabled() {
        println!("{}", message.as_ref());
    }
}

fn main() -> io::Result<()> {
    let settings = Settings::get_from_args();

    let database: Arc<dyn StorageEngine> = match settings.engine {
        EngineType::KV => match KVEngine::from_dir(
            &settings.db_file_path,
            &settings.db_name,
            settings.max_segment_bytes,
            settings.sync_strategy,
        )? {
            Some(db) => Arc::new(db),
            None => Arc::new(KVEngine::new(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes,
                settings.sync_strategy,
            )?),
        },
        EngineType::Lsm => {
            let block_size_bytes = (settings.block_size_kb * 1024) as usize;
            let block_compression_enabled =
                matches!(settings.block_compression, BlockCompression::Lz77);
            let strategy: Box<dyn rustikv::storage_strategy::StorageStrategy> =
                match settings.storage_strategy {
                    StorageStrategy::SizeTiered => Box::new(SizeTiered::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        4,  // min_threshold
                        32, // max_threshold
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                    StorageStrategy::Leveled => Box::new(Leveled::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        settings.leveled_num_levels,
                        settings.leveled_l0_threshold,
                        settings.leveled_l1_max_bytes,
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                };

            Arc::new(LsmEngine::from_dir(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes as usize,
                strategy,
                block_size_bytes,
                block_compression_enabled,
            )?)
        }
    };

    let stats = Arc::new(Stats::new());
    let listener = TcpListener::bind(&settings.tcp_addr)?;
    let actual_addr = listener.local_addr()?;

    // Convert 0.0.0.0 to 127.0.0.1 for client connections
    let connect_addr = match actual_addr {
        std::net::SocketAddr::V4(addr) if addr.ip().is_unspecified() => std::net::SocketAddr::V4(
            std::net::SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, addr.port()),
        ),
        std::net::SocketAddr::V6(addr) if addr.ip().is_unspecified() => std::net::SocketAddr::V6(
            std::net::SocketAddrV6::new(std::net::Ipv6Addr::LOCALHOST, addr.port(), 0, 0),
        ),
        _ => actual_addr,
    };

    let addr_file = format!("{}/server.addr", &settings.db_file_path);
    std::fs::write(&addr_file, connect_addr.to_string())?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if settings.read_timeout_secs > 0 {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(settings.read_timeout_secs)))?;
                }
                log_verbose(format!("New connection: {}", stream.peer_addr().unwrap()));
                if settings.max_connections > 0
                    && stats.active_connections.load(Ordering::Relaxed)
                        >= settings.max_connections as i64
                {
                    let error_text = format!(
                        "Rejecting client, max connections reached: {}",
                        stats.active_connections.load(Ordering::Relaxed)
                    );
                    log_verbose(&error_text);
                    let _ = stream.write_all(&encode_frame(ResponseStatus::Error, &[error_text]));
                    continue;
                }
                let shared_db = Arc::clone(&database);
                let shared_stats = Arc::clone(&stats);
                thread::spawn(move || {
                    shared_stats
                        .active_connections
                        .fetch_add(1, Ordering::Relaxed);
                    handle_stream(
                        &mut stream,
                        shared_db,
                        &shared_stats,
                        settings.compaction_ratio,
                        settings.compaction_max_segment,
                    );
                    shared_stats
                        .active_connections
                        .fetch_sub(1, Ordering::Relaxed);
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_stream(
    stream: &mut TcpStream,
    database: Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    compaction_ratio: f32,
    compaction_max_segment: usize,
) {
    handle_stream_inner(
        stream,
        database.clone(),
        stats,
        compaction_ratio,
        compaction_max_segment,
    )
    .unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
    });
}

fn handle_stream_inner(
    stream: &mut TcpStream,
    database: Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    compaction_ratio: f32,
    compaction_max_segment: usize,
) -> io::Result<()> {
    const MAX_REQUEST_BYTES: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 1024;
    let mut buf_stream: BufReader<&mut TcpStream> = BufReader::new(&mut *stream);

    loop {
        let mut len_buf = [0u8; 4];

        match buf_stream.read_exact(&mut len_buf) {
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                ) =>
            {
                break;
            } // client disconnected or timed out
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len > MAX_REQUEST_BYTES {
            let response = encode_frame(
                ResponseStatus::Error,
                &[format!("Frame too large: {} bytes", frame_len)],
            );
            buf_stream.get_mut().write_all(&response)?;
            continue;
        }
        let mut payload = vec![0u8; frame_len];
        buf_stream.read_exact(&mut payload)?;
        let mut buf = Vec::with_capacity(4 + frame_len);
        buf.extend_from_slice(&len_buf);
        buf.extend_from_slice(&payload);

        let cmd = decode_input_frame(&buf)?;

        let response = match cmd {
            Command::Write(key, value) => {
                log_verbose(format!(
                    "Parsed WRITE command: key='{}', value='{}'",
                    key, value
                ));
                if stats.compacting.load(Ordering::Relaxed) {
                    stats.write_blocked_attempts.fetch_add(1, Ordering::Relaxed);
                }
                let lock_start = Instant::now();
                let result = { database.set(&key, &value) };
                let lock_elapsed = lock_start.elapsed().as_millis() as u64;
                stats
                    .write_blocked_total_ms
                    .fetch_add(lock_elapsed, Ordering::Relaxed);
                match result {
                    Ok(_) => {
                        stats.writes.fetch_add(1, Ordering::Relaxed);
                        maybe_trigger_compaction(
                            database.clone(),
                            stats,
                            compaction_ratio,
                            compaction_max_segment,
                        );
                        encode_frame(ResponseStatus::Ok, &[])
                    }
                    Err(err) => encode_frame(ResponseStatus::Error, &[err.to_string()]),
                }
            }
            Command::Read(key) => {
                log_verbose(format!("Parsed READ command: key='{}'", key));
                stats.reads.fetch_add(1, Ordering::Relaxed);
                match database.get(&key) {
                    Ok(Some((_, v))) => encode_frame(ResponseStatus::Ok, &[v]),
                    Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                }
            }
            Command::Delete(key) => {
                log_verbose(format!("Parsed DELETE command: key='{}'", key));
                let result = { database.delete(&key) };
                match result {
                    Ok(Some(())) => {
                        stats.deletes.fetch_add(1, Ordering::Relaxed);
                        maybe_trigger_compaction(
                            database.clone(),
                            stats,
                            compaction_ratio,
                            compaction_max_segment,
                        );
                        encode_frame(ResponseStatus::Ok, &[])
                    }
                    Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                }
            }
            Command::Compact => {
                log_verbose("Parsed COMPACT command");
                if stats
                    .compacting
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    buf_stream
                        .get_mut()
                        .write_all(&encode_frame(ResponseStatus::Noop, &[]))?;
                    continue;
                }
                stats
                    .last_compact_start_ms
                    .store(Stats::now_ms(), Ordering::Relaxed);
                let db_clone = Arc::clone(&database);
                let stats_clone = Arc::clone(stats);
                thread::spawn(move || {
                    db_clone.compact().unwrap();

                    stats_clone
                        .last_compact_end_ms
                        .store(Stats::now_ms(), Ordering::Relaxed);
                    stats_clone.compacting.store(false, Ordering::Release);
                    stats_clone.compaction_count.fetch_add(1, Ordering::Relaxed);
                });
                encode_frame(ResponseStatus::Ok, &[])
            }
            Command::Stats => encode_frame(ResponseStatus::Ok, &[stats.snapshot()]),
            Command::Invalid(op_code) => {
                log_verbose(format!("Invalid op code: {}", op_code));
                encode_frame(
                    ResponseStatus::Error,
                    &[format!("Invalid op code: {}", op_code)],
                )
            }
            Command::List => match database.list_keys() {
                Ok(keys) => encode_frame(ResponseStatus::Ok, &keys),
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            },
            Command::Exists(key) => {
                if database.exists(&key) {
                    encode_frame(ResponseStatus::Ok, &[])
                } else {
                    encode_frame(ResponseStatus::NotFound, &[])
                }
            }
            Command::Ping => encode_frame(ResponseStatus::Ok, &["PONG".to_string()]),
            Command::Mget(keys) => {
                log_verbose("Parsed MGET command");
                match database.mget(keys) {
                    Ok(items) => {
                        let key_count = items.len() as u64;

                        let flat: Vec<String> = items
                            .into_iter()
                            .flat_map(|(k, v)| match v {
                                Some(value) => [k, value],
                                None => [k, String::from("\0")],
                            })
                            .collect();
                        stats.reads.fetch_add(key_count, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &flat)
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                }
            }
            Command::Mset(items) => {
                log_verbose("Parsed MSET command");
                if stats.compacting.load(Ordering::Relaxed) {
                    stats.write_blocked_attempts.fetch_add(1, Ordering::Relaxed);
                }
                let item_count = items.len() as u64;

                let lock_start = Instant::now();
                let result = { database.mset(items) };
                let lock_elapsed = lock_start.elapsed().as_millis() as u64;
                stats
                    .write_blocked_total_ms
                    .fetch_add(lock_elapsed, Ordering::Relaxed);

                match result {
                    Ok(_) => {
                        stats.writes.fetch_add(item_count, Ordering::Relaxed);
                        maybe_trigger_compaction(
                            database.clone(),
                            stats,
                            compaction_ratio,
                            compaction_max_segment,
                        );
                        encode_frame(ResponseStatus::Ok, &[])
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                }
            }
            Command::Range(start, end) => {
                log_verbose(format!(
                    "Parsed RANGE command: start='{}' end={}",
                    start, end
                ));
                match database.as_any().downcast_ref::<LsmEngine>() {
                    Some(lsm) => match lsm.range(&start, &end) {
                        Ok(results) => {
                            let results_count = results.len();
                            let res: Vec<String> =
                                results.into_iter().flat_map(|(k, v)| [k, v]).collect();
                            stats
                                .reads
                                .fetch_add(results_count as u64, Ordering::Relaxed);

                            encode_frame(ResponseStatus::Ok, &res)
                        }
                        Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                    },
                    None => encode_frame(
                        ResponseStatus::Error,
                        &["RANGE not supported by KV engine".to_string()],
                    ),
                }
            }
        };
        buf_stream.get_mut().write_all(&response)?;
    }

    Ok(())
}

fn maybe_trigger_compaction(
    database: Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    compaction_ratio: f32,
    compaction_max_segment: usize,
) {
    let should_compact = {
        (compaction_ratio > 0.0
            && database.total_bytes() > 0
            && database.dead_bytes() as f32 / database.total_bytes() as f32 > compaction_ratio)
            || (compaction_max_segment > 0 && database.segment_count() > compaction_max_segment)
    };

    let stats_clone = Arc::clone(stats);
    if should_compact {
        if stats
            .compacting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        stats
            .last_compact_start_ms
            .store(Stats::now_ms(), Ordering::Relaxed);
        thread::spawn(move || {
            database.compact().unwrap();

            stats_clone
                .last_compact_end_ms
                .store(Stats::now_ms(), Ordering::Relaxed);
            stats_clone.compacting.store(false, Ordering::Release);
            stats_clone.compaction_count.fetch_add(1, Ordering::Relaxed);
        });
    }
}
