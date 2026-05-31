use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::settings::FSyncStrategy;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_kv_incr_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn make_engine(suffix: &str) -> KVEngine {
    let dir = temp_dir(suffix);
    KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap()
}

// --- basic semantics ---

#[test]
fn incr_creates_missing_key_at_one() {
    let engine = make_engine("create");
    assert_eq!(engine.incr("counter", None).unwrap(), 1);
    assert_eq!(engine.get("counter").unwrap().unwrap().1, "1");
}

#[test]
fn incr_increments_existing() {
    let engine = make_engine("existing");
    engine.set("c", "41").unwrap();
    assert_eq!(engine.incr("c", None).unwrap(), 42);
    assert_eq!(engine.incr("c", None).unwrap(), 43);
    assert_eq!(engine.get("c").unwrap().unwrap().1, "43");
}

#[test]
fn incr_handles_negative_values() {
    let engine = make_engine("negative");
    engine.set("c", "-5").unwrap();
    assert_eq!(engine.incr("c", None).unwrap(), -4);
}

// --- error cases (must not mutate) ---

#[test]
fn incr_on_non_integer_errors_and_leaves_value_intact() {
    let engine = make_engine("non_int");
    engine.set("c", "abc").unwrap();
    assert!(engine.incr("c", None).is_err());
    // value unchanged — the failed parse happens before any write
    assert_eq!(engine.get("c").unwrap().unwrap().1, "abc");
}

#[test]
fn incr_overflow_errors_and_leaves_value_intact() {
    let engine = make_engine("overflow");
    engine.set("c", &i64::MAX.to_string()).unwrap();
    assert!(engine.incr("c", None).is_err());
    assert_eq!(engine.get("c").unwrap().unwrap().1, i64::MAX.to_string());
}

// --- byte accounting ---

// Incrementing an existing key appends a new record and marks the old one dead,
// so dead_bytes must grow (otherwise the auto-compaction trigger under-counts).
#[test]
fn incr_overwrite_accumulates_dead_bytes() {
    let engine = make_engine("dead_bytes");
    engine.set("c", "1").unwrap();
    let dead_before = engine.dead_bytes();
    engine.incr("c", None).unwrap();
    assert!(
        engine.dead_bytes() > dead_before,
        "overwriting via incr should mark the previous record dead"
    );
}

// --- TTL preservation (Redis-compatible) ---

#[test]
fn incr_preserves_existing_ttl() {
    let engine = make_engine("preserve_ttl");
    let expiry_ms = now_ms() + 500;
    engine.set_with_ttl("c", "5", Some(expiry_ms)).unwrap();
    assert_eq!(engine.incr("c", None).unwrap(), 6);
    // The original expiry survives the increment, so the key still expires.
    thread::sleep(Duration::from_millis(750));
    assert!(
        engine.get("c").unwrap().is_none(),
        "incr should preserve the existing TTL, not make the key permanent"
    );
}

// --- atomicity ---

// The whole point of incr being a locked read-modify-write: concurrent
// increments must not lose updates. 8 threads × 500 = 4000.
#[test]
fn incr_is_atomic_under_concurrency() {
    let engine = Arc::new(make_engine("atomic"));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let e = Arc::clone(&engine);
            thread::spawn(move || {
                for _ in 0..500 {
                    e.incr("counter", None).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(engine.get("counter").unwrap().unwrap().1, "4000");
}
