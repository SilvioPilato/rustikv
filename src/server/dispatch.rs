use std::{
    env,
    sync::{Arc, atomic::Ordering},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    bffp::{Command, ResponseStatus, encode_frame},
    engine::{RangeScan, StorageEngine, TtlOutcome},
    lsmengine::LsmEngine,
    stats::Stats,
};

#[derive(Clone, Copy)]
pub struct CompactionCfg {
    pub ratio: f32,
    pub max_segment: usize,
}

pub fn dispatch(
    cmd: Command,
    database: &Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8> {
    match cmd {
        Command::Write(key, value, ttl_seconds) => {
            log_verbose(format!(
                "Parsed WRITE command: key='{}', value='{}'",
                key, value
            ));
            if stats.compacting.load(Ordering::Relaxed) {
                stats.write_blocked_attempts.fetch_add(1, Ordering::Relaxed);
            }
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_millis() as u64;
            let ttl_ms = ttl_seconds.and_then(|s| {
                if s == 0 {
                    None
                } else {
                    Some(now_ms + (s as u64) * 1000)
                }
            });

            let lock_start = Instant::now();
            let result = database.set_with_ttl(&key, &value, ttl_ms);
            let lock_elapsed = lock_start.elapsed().as_millis() as u64;
            stats
                .write_blocked_total_ms
                .fetch_add(lock_elapsed, Ordering::Relaxed);
            match result {
                Ok(_) => {
                    stats.writes.fetch_add(1, Ordering::Relaxed);
                    maybe_trigger_compaction(database.clone(), stats, cfg);
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
            let result = database.delete(&key);
            match result {
                Ok(Some(())) => {
                    stats.deletes.fetch_add(1, Ordering::Relaxed);
                    maybe_trigger_compaction(database.clone(), stats, cfg);
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
                return encode_frame(ResponseStatus::Noop, &[]);
            }
            stats
                .last_compact_start_ms
                .store(Stats::now_ms(), Ordering::Relaxed);
            let db_clone = Arc::clone(database);
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
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_millis() as u64;
            let items_ms = items
                .into_iter()
                .map(|(k, v, ttl_seconds)| {
                    let ttl_ms = ttl_seconds.and_then(|s| {
                        if s == 0 {
                            None
                        } else {
                            Some(now_ms + (s as u64) * 1000)
                        }
                    });
                    (k, v, ttl_ms)
                })
                .collect::<Vec<(String, String, Option<u64>)>>();
            let lock_start = Instant::now();
            let result = database.mset_with_ttl(items_ms);
            let lock_elapsed = lock_start.elapsed().as_millis() as u64;
            stats
                .write_blocked_total_ms
                .fetch_add(lock_elapsed, Ordering::Relaxed);

            match result {
                Ok(_) => {
                    stats.writes.fetch_add(item_count, Ordering::Relaxed);
                    maybe_trigger_compaction(database.clone(), stats, cfg);
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
        Command::Ttl(key, seconds) => {
            log_verbose(format!(
                "Parsed TTL command: key='{}' seconds={}",
                key, seconds
            ));
            let expiry_ms = if seconds == 0 {
                None
            } else {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("time went backwards")
                    .as_millis() as u64;
                Some(now_ms + (seconds as u64) * 1000)
            };
            match database.ttl(&key, expiry_ms) {
                Ok(outcome) => match outcome {
                    TtlOutcome::Set => encode_frame(ResponseStatus::Ok, &[]),
                    TtlOutcome::Persisted => encode_frame(ResponseStatus::Ok, &[]),
                    TtlOutcome::NotFound => encode_frame(ResponseStatus::NotFound, &[]),
                },
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            }
        }
        Command::Incr(key) => {
            log_verbose(format!("Parsed INCR command: key='{}'", key));
            match database.incr(&key) {
                Ok(value) => {
                    stats.writes.fetch_add(1, Ordering::Relaxed);
                    maybe_trigger_compaction(database.clone(), stats, cfg);
                    encode_frame(ResponseStatus::Ok, &[value.to_string()])
                }
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            }
        }
        Command::Prefix(prefix) => {
            log_verbose(format!("Parsed PREFIX command: prefix='{}'", prefix));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.prefix(&prefix) {
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
                    &["PREFIX not supported by KV engine".to_string()],
                ),
            }
        }
        Command::CountPrefix(prefix) => {
            log_verbose(format!(
                "Parsed COUNT <prefix> command: prefix='{}'",
                prefix
            ));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.count_prefix(&prefix) {
                    Ok(count) => {
                        stats.reads.fetch_add(1, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &[count.to_string()])
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                },
                None => encode_frame(
                    ResponseStatus::Error,
                    &["COUNT not supported by KV engine".to_string()],
                ),
            }
        }
        Command::CountRange(start, end) => {
            log_verbose(format!(
                "Parsed COUNT <start> <end> command: start='{}' end='{}'",
                start, end
            ));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.count_range(&start, &end) {
                    Ok(count) => {
                        stats.reads.fetch_add(1, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &[count.to_string()])
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                },
                None => encode_frame(
                    ResponseStatus::Error,
                    &["COUNT not supported by KV engine".to_string()],
                ),
            }
        }
        Command::SumPrefix(prefix) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.sum_prefix(&prefix) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["SUM not supported by KV engine".to_string()],
            ),
        },
        Command::SumRange(start, end) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.sum_range(&start, &end) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["SUM not supported by KV engine".to_string()],
            ),
        },
        Command::AvgPrefix(prefix) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.avg_prefix(&prefix) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["AVG not supported by KV engine".to_string()],
            ),
        },
        Command::AvgRange(start, end) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.avg_range(&start, &end) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["AVG not supported by KV engine".to_string()],
            ),
        },
        Command::MinPrefix(prefix) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.min_prefix(&prefix) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["MIN not supported by KV engine".to_string()],
            ),
        },
        Command::MinRange(start, end) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.min_range(&start, &end) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["MIN not supported by KV engine".to_string()],
            ),
        },
        Command::MaxPrefix(prefix) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.max_prefix(&prefix) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["MAX not supported by KV engine".to_string()],
            ),
        },
        Command::MaxRange(start, end) => match database.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.max_range(&start, &end) {
                Ok(Some(v)) => {
                    stats.reads.fetch_add(1, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
                }
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["MAX not supported by KV engine".to_string()],
            ),
        },
    }
}

pub fn maybe_trigger_compaction(
    database: Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) {
    let should_compact = (cfg.ratio > 0.0
        && database.total_bytes() > 0
        && database.dead_bytes() as f32 / database.total_bytes() as f32 > cfg.ratio)
        || (cfg.max_segment > 0 && database.segment_count() > cfg.max_segment);

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

fn log_verbose(message: impl AsRef<str>) {
    if verbose_logging_enabled() {
        println!("{}", message.as_ref());
    }
}

fn verbose_logging_enabled() -> bool {
    matches!(env::var("RUSTIKV_VERBOSE"), Ok(value) if value == "1")
}
