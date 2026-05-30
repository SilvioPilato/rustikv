use std::{
    env,
    sync::{Arc, atomic::Ordering},
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    bffp::{Command, ResponseStatus, encode_frame},
    collections::Collections,
    engine::{RangeScan, StorageEngine, TtlOutcome},
    lsmengine::LsmEngine,
    stats::Stats,
};

#[derive(Clone, Copy)]
pub struct CompactionCfg {
    pub ratio: f32,
    pub max_segment: usize,
}

/// Where the server's data lives: either a single KV engine (no collections)
/// or an LSM collection registry. Collections are LSM-only by design, so the
/// KV/LSM distinction is honestly modeled here rather than hidden behind a
/// pretend-generic registry.
pub enum Backend {
    Kv(Arc<dyn StorageEngine>),
    Lsm(Arc<Collections>),
}

impl Backend {
    /// The collection a fresh connection starts in. Meaningless for KV (which
    /// has no collections), so any placeholder works there.
    pub fn default_collection(&self) -> String {
        match self {
            Backend::Kv(_) => "default".to_string(),
            Backend::Lsm(c) => c.default_name().to_string(),
        }
    }

    /// Resolve the current collection to its engine + default TTL (seconds).
    /// `None` means the named collection does not exist (LSM only).
    fn resolve(&self, current: &str) -> Option<(Arc<dyn StorageEngine>, u32)> {
        match self {
            Backend::Kv(engine) => Some((engine.clone(), 0)),
            Backend::Lsm(c) => {
                let engine: Arc<dyn StorageEngine> = c.get(current)?;
                let default_ttl = c.default_ttl(current).unwrap_or(0);
                Some((engine, default_ttl))
            }
        }
    }
}

/// Top-level entry point: handle collection-management commands against the
/// registry, and forward every data command to the resolved engine.
pub fn route(
    cmd: Command,
    backend: &Backend,
    current: &mut String,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8> {
    match cmd {
        Command::Use(name) => use_collection(backend, current, name),
        Command::CreateCollection(name, default_ttl) => {
            create_collection(backend, name, default_ttl)
        }
        Command::DropCollection(name) => drop_collection(backend, name),
        Command::ShowCollections => show_collections(backend),
        data_cmd => match backend.resolve(current) {
            Some((engine, default_ttl)) => {
                dispatch_with_default_ttl(data_cmd, &engine, default_ttl, stats, cfg)
            }
            None => encode_frame(
                ResponseStatus::Error,
                &[format!("no such collection: {current}")],
            ),
        },
    }
}

fn lsm(backend: &Backend) -> Result<&Arc<Collections>, Vec<u8>> {
    match backend {
        Backend::Lsm(c) => Ok(c),
        Backend::Kv(_) => Err(encode_frame(
            ResponseStatus::Error,
            &["collections are not supported by the KV engine".to_string()],
        )),
    }
}

fn use_collection(backend: &Backend, current: &mut String, name: String) -> Vec<u8> {
    let c = match lsm(backend) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if c.get(&name).is_some() {
        *current = name;
        encode_frame(ResponseStatus::Ok, &[])
    } else {
        encode_frame(
            ResponseStatus::Error,
            &[format!("no such collection: {name}")],
        )
    }
}

fn create_collection(backend: &Backend, name: String, default_ttl: Option<u32>) -> Vec<u8> {
    let c = match lsm(backend) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match c.create_named(&name, default_ttl.unwrap_or(0)) {
        Ok(()) => encode_frame(ResponseStatus::Ok, &[]),
        Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
    }
}

fn drop_collection(backend: &Backend, name: String) -> Vec<u8> {
    let c = match lsm(backend) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    match c.remove(&name) {
        Ok(()) => encode_frame(ResponseStatus::Ok, &[]),
        Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
    }
}

fn show_collections(backend: &Backend) -> Vec<u8> {
    let c = match lsm(backend) {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let rows: Vec<String> = c
        .show()
        .into_iter()
        .map(|m| format!("{}\t{}", m.name, m.default_ttl_secs))
        .collect();
    encode_frame(ResponseStatus::Ok, &rows)
}

/// Resolve a client-supplied TTL against a collection default.
///   - `None`     → apply the collection default (0 default = no expiry)
///   - `Some(0)`  → explicit "no expiry"
///   - `Some(n)`  → expire in n seconds
fn resolve_expiry(now_ms: u64, ttl_seconds: Option<u32>, default_secs: u32) -> Option<u64> {
    match ttl_seconds {
        None => {
            if default_secs == 0 {
                None
            } else {
                Some(now_ms + (default_secs as u64) * 1000)
            }
        }
        Some(0) => None,
        Some(n) => Some(now_ms + (n as u64) * 1000),
    }
}

/// Backward-compatible entry point: dispatch a data command with no collection
/// default TTL (equivalent to the pre-collections behavior).
pub fn dispatch(
    cmd: Command,
    database: &Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8> {
    dispatch_with_default_ttl(cmd, database, 0, stats, cfg)
}

pub fn dispatch_with_default_ttl(
    cmd: Command,
    database: &Arc<dyn StorageEngine>,
    default_ttl_secs: u32,
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
            let ttl_ms = resolve_expiry(now_ms, ttl_seconds, default_ttl_secs);

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
                    let ttl_ms = resolve_expiry(now_ms, ttl_seconds, default_ttl_secs);
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
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_millis() as u64;
            // INCR carries no client TTL, so `None` means "apply the default".
            let default_expiry_ms = resolve_expiry(now_ms, None, default_ttl_secs);
            match database.incr(&key, default_expiry_ms) {
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
        // Collection-management commands are handled in `route` before reaching
        // here. If one arrives, it's a routing bug — fail loudly rather than act.
        Command::Use(_)
        | Command::CreateCollection(_, _)
        | Command::DropCollection(_)
        | Command::ShowCollections => encode_frame(
            ResponseStatus::Error,
            &["collection command reached dispatch (routing bug)".to_string()],
        ),
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
