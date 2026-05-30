use rustikv::engine::StorageEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::size_tiered::SizeTiered;
use rustikv::utils::now_ms;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};
use std::{env, fs};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_lsm_incr_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn make_engine(suffix: &str) -> LsmEngine {
    let dir = temp_dir(suffix);
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap()
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
    assert_eq!(engine.get("c").unwrap().unwrap().1, "abc");
}

#[test]
fn incr_overflow_errors_and_leaves_value_intact() {
    let engine = make_engine("overflow");
    engine.set("c", &i64::MAX.to_string()).unwrap();
    assert!(engine.incr("c", None).is_err());
    assert_eq!(engine.get("c").unwrap().unwrap().1, i64::MAX.to_string());
}

// --- TTL preservation (Redis-compatible) ---

// Reads resolve through memtable → immutable → SSTable, all expiry-aware, so a
// preserved TTL must still expire the incremented value.
#[test]
fn incr_preserves_existing_ttl() {
    let engine = make_engine("preserve_ttl");
    let expiry_ms = now_ms() + 500;
    engine.set_with_ttl("c", "5", Some(expiry_ms)).unwrap();
    assert_eq!(engine.incr("c", None).unwrap(), 6);
    thread::sleep(Duration::from_millis(750));
    assert!(
        engine.get("c").unwrap().is_none(),
        "incr should preserve the existing TTL, not make the key permanent"
    );
}

// --- atomicity ---

// Locked read-modify-write: concurrent increments must not lose updates.
// 8 threads × 500 = 4000.
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
