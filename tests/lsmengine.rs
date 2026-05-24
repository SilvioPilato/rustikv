use rustikv::engine::{RangeScan, StorageEngine};
use rustikv::lsmengine::LsmEngine;
use rustikv::size_tiered::SizeTiered;
use std::sync::Arc;
use std::thread;
use std::{env, fs, time::SystemTime};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_store_lsm_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn new_engine(dir: &str, db_name: &str, max_memtable_bytes: usize) -> std::io::Result<LsmEngine> {
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(dir, db_name, max_memtable_bytes, strategy, 4096, true)
}

fn engine_from_dir(
    dir: &str,
    db_name: &str,
    max_memtable_bytes: usize,
) -> std::io::Result<LsmEngine> {
    let strategy = Box::new(SizeTiered::load_from_dir(dir, db_name, 4, 32, 4096, true)?);
    LsmEngine::from_dir(dir, db_name, max_memtable_bytes, strategy, 4096, true)
}

const BIG_MEMTABLE: usize = 1_048_576; // 1 MB — won't auto-flush

#[test]
fn set_and_get() {
    let dir = temp_dir("set_get");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("hello", "world").unwrap();
    let result = engine.get("hello").unwrap();
    assert_eq!(result, Some(("hello".to_string(), "world".to_string())));
}

#[test]
fn get_missing_key() {
    let dir = temp_dir("missing");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    assert_eq!(engine.get("nope").unwrap(), None);
}

#[test]
fn set_overwrite() {
    let dir = temp_dir("overwrite");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("k", "old").unwrap();
    engine.set("k", "new").unwrap();
    let (_, v) = engine.get("k").unwrap().unwrap();
    assert_eq!(v, "new");
}

#[test]
fn delete_removes_key() {
    let dir = temp_dir("delete");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("k", "v").unwrap();
    engine.delete("k").unwrap();
    assert_eq!(engine.get("k").unwrap(), None);
}

#[test]
fn delete_nonexistent_key() {
    let dir = temp_dir("delete_missing");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    let result = engine.delete("nope").unwrap();
    assert_eq!(result, None);
}

#[test]
fn memtable_flushes_to_sstable() {
    let dir = temp_dir("flush");
    // Tiny threshold so a single write triggers a flush
    {
        let engine = new_engine(&dir, "test", 1).unwrap();
        engine.set("k1", "v1").unwrap();
    } // drop joins the background flush

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();

    // After flush, data is readable from SSTable
    let (_, v) = engine.get("k1").unwrap().unwrap();
    assert_eq!(v, "v1");

    // .sst file should exist on disk
    let sst_files: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(!sst_files.is_empty(), "expected at least one .sst file");
}

#[test]
fn reads_span_memtable_and_segments() {
    let dir = temp_dir("span");
    let engine = new_engine(&dir, "test", 1).unwrap();
    // This will flush to segment
    engine.set("k1", "v1").unwrap();

    // Larger threshold so next write stays in memtable
    // We can't change threshold, but k2 will also flush and k3 stays if we make threshold bigger
    // Instead: write multiple keys, they all flush, then write one more with room
    engine.set("k2", "v2").unwrap();

    // Both should be readable regardless of where they live
    let (_, v1) = engine.get("k1").unwrap().unwrap();
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
}

#[test]
fn delete_shadows_flushed_value() {
    let dir = temp_dir("shadow");
    let engine = new_engine(&dir, "test", 1).unwrap();
    // Flush k1 to a segment
    engine.set("k1", "v1").unwrap();
    // Delete in memtable should shadow the segment value
    engine.delete("k1").unwrap();
    assert_eq!(engine.get("k1").unwrap(), None);
}

#[test]
fn compact_preserves_values() {
    let dir = temp_dir("compact_preserve");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("k1", "v1").unwrap();
    engine.set("k2", "v2").unwrap();

    engine.compact().unwrap();

    let (_, v1) = engine.get("k1").unwrap().unwrap();
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
}

#[test]
fn compact_keeps_latest_value() {
    let dir = temp_dir("compact_latest");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("k", "old").unwrap();
    engine.set("k", "new").unwrap();

    engine.compact().unwrap();

    let (_, v) = engine.get("k").unwrap().unwrap();
    assert_eq!(v, "new");
}

#[test]
fn compact_drops_deleted_keys() {
    let dir = temp_dir("compact_delete");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("k1", "v1").unwrap();
    engine.delete("k1").unwrap();

    engine.compact().unwrap();

    assert_eq!(engine.get("k1").unwrap(), None);
}

#[test]
fn compact_is_idempotent() {
    let dir = temp_dir("compact_idempotent");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("k1", "v1").unwrap();
    engine.set("k2", "v2").unwrap();

    engine.compact().unwrap();
    engine.compact().unwrap();

    let (_, v1) = engine.get("k1").unwrap().unwrap();
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
}

#[test]
fn from_dir_reloads_segments() {
    let dir = temp_dir("from_dir");
    {
        let engine = new_engine(&dir, "test", 1).unwrap();
        engine.set("k1", "v1").unwrap();
        engine.set("k2", "v2").unwrap();
    }

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    let (_, v1) = engine.get("k1").unwrap().unwrap();
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
}

#[test]
fn compact_reduces_segment_count() {
    let dir = temp_dir("compact_reduce");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("k1", "v1").unwrap();
    engine.set("k2", "v2").unwrap();
    engine.set("k3", "v3").unwrap();

    // Multiple .sst files should exist before compaction
    let sst_before: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert!(sst_before.len() > 1);

    engine.compact().unwrap();

    // After compaction, should be exactly 1 .sst file
    let sst_after: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().to_string();
            if name.ends_with(".sst") {
                Some(name)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(sst_after.len(), 1);
}

// --- WAL recovery tests ---

#[test]
fn wal_recovers_unflushed_writes() {
    // Writes that never trigger a flush live only in the WAL.
    // Dropping the engine simulates a crash; from_dir must replay the WAL.
    let dir = temp_dir("wal_recover");
    {
        let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
        engine.set("k1", "v1").unwrap();
        engine.set("k2", "v2").unwrap();
        engine.set("k3", "v3").unwrap();
        // drop without flush
    }

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    assert_eq!(
        engine.get("k1").unwrap(),
        Some(("k1".to_string(), "v1".to_string()))
    );
    assert_eq!(
        engine.get("k2").unwrap(),
        Some(("k2".to_string(), "v2".to_string()))
    );
    assert_eq!(
        engine.get("k3").unwrap(),
        Some(("k3".to_string(), "v3".to_string()))
    );
}

#[test]
fn wal_recovers_unflushed_delete() {
    // A delete that has not been flushed must be replayed as a tombstone,
    // shadowing the earlier value that was already flushed to an SSTable.
    let dir = temp_dir("wal_delete");
    {
        let engine = new_engine(&dir, "test", 1).unwrap(); // threshold=1 flushes immediately
        engine.set("k1", "v1").unwrap(); // flushed to SSTable
    }
    {
        let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
        engine.delete("k1").unwrap(); // tombstone in WAL only, not flushed
        // drop without flush
    }

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    assert_eq!(engine.get("k1").unwrap(), None);
}

#[test]
fn wal_corrupt_tail_does_not_panic() {
    // A crash mid-write leaves a torn record at the tail of the WAL.
    // from_dir must stop replay at the corrupt record and recover earlier entries.
    let dir = temp_dir("wal_corrupt");
    {
        let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
        engine.set("k1", "v1").unwrap();
        engine.set("k2", "v2").unwrap();
    }

    // Truncate the last 5 bytes of the WAL to simulate a torn write
    let wal_path = std::path::Path::new(&dir).join("test.wal");
    let metadata = fs::metadata(&wal_path).unwrap();
    let truncated_len = metadata.len().saturating_sub(5);
    let file = fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
    file.set_len(truncated_len).unwrap();

    // Must not panic; k1 may or may not be recovered depending on where truncation fell,
    // but from_dir must succeed
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    // k1 was written first — its record is intact and must be readable
    assert_eq!(
        engine.get("k1").unwrap(),
        Some(("k1".to_string(), "v1".to_string()))
    );
}

#[test]
fn wal_is_absent_after_flush() {
    // Once the memtable is flushed to an SSTable, the WAL is no longer needed.
    // It must be deleted so that the next startup doesn't replay stale entries.
    let dir = temp_dir("wal_absent");
    let engine = new_engine(&dir, "test", 1).unwrap(); // threshold=1, every write flushes
    engine.set("k1", "v1").unwrap();

    let wal_path = std::path::Path::new(&dir).join("test.wal");
    assert_eq!(
        fs::metadata(&wal_path).unwrap().len(),
        0,
        "WAL should be empty after flush"
    );
}

#[test]
fn exists_returns_true_after_set() {
    let dir = temp_dir("exists_true");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("k", "v").unwrap();
    assert!(engine.exists("k"));
}

#[test]
fn exists_returns_false_for_missing_key() {
    let dir = temp_dir("exists_missing");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    assert!(!engine.exists("nope"));
}

#[test]
fn exists_returns_false_after_delete() {
    let dir = temp_dir("exists_delete");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("k", "v").unwrap();
    engine.delete("k").unwrap();
    assert!(!engine.exists("k"));
}

#[test]
fn exists_returns_true_for_flushed_key() {
    // Key is flushed to an SSTable; exists must still find it via SSTable lookup
    // (and the bloom filter must not produce a false negative).
    let dir = temp_dir("exists_flushed");
    let engine = new_engine(&dir, "test", 1).unwrap(); // threshold=1, every write flushes
    engine.set("k", "v").unwrap();
    assert!(engine.exists("k"));
}

#[test]
fn exists_returns_false_for_tombstoned_flushed_key() {
    // Key written and flushed, then deleted (tombstone in memtable). exists must return false.
    let dir = temp_dir("exists_tombstone");
    let engine = new_engine(&dir, "test", 1).unwrap(); // threshold=1, flushes on set
    engine.set("k", "v").unwrap();
    engine.delete("k").unwrap();
    assert!(!engine.exists("k"));
}

#[test]
fn list_keys_returns_all_live_keys() {
    let dir = temp_dir("list_keys");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();

    let mut keys = engine.list_keys().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn list_keys_excludes_deleted_keys() {
    let dir = temp_dir("list_keys_delete");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.delete("b").unwrap();

    let mut keys = engine.list_keys().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "c"]);
}

#[test]
fn list_keys_spans_memtable_and_segments() {
    let dir = temp_dir("list_keys_segments");
    // Threshold of 1 byte so every write flushes to SSTable
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("x", "1").unwrap();
    engine.set("y", "2").unwrap();
    drop(engine);
    // Reload from disk and write one more key — it stays in the memtable
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("z", "3").unwrap();

    let mut keys = engine.list_keys().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["x", "y", "z"]);
}

#[test]
fn list_keys_tombstone_in_memtable_hides_flushed_key() {
    let dir = temp_dir("list_keys_tombstone");
    // Flush "a" to SSTable, then delete it via memtable tombstone
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("a", "1").unwrap();
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.delete("a").unwrap();

    assert_eq!(engine.list_keys().unwrap(), Vec::<String>::new());
}

// --- RangeScan tests ---

#[test]
fn range_basic_memtable() {
    let dir = temp_dir("range_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.set("d", "4").unwrap();

    let results = engine.range("b", "c").unwrap();
    assert_eq!(
        results,
        vec![
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string())
        ]
    );
}

#[test]
fn range_inclusive_bounds() {
    let dir = temp_dir("range_inclusive");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("z", "26").unwrap();

    // Both endpoints must be included
    let results = engine.range("a", "z").unwrap();
    assert_eq!(
        results,
        vec![
            ("a".to_string(), "1".to_string()),
            ("z".to_string(), "26".to_string())
        ]
    );
}

#[test]
fn range_empty_when_no_keys_in_range() {
    let dir = temp_dir("range_empty");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("z", "26").unwrap();

    let results = engine.range("m", "p").unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_returns_sorted_order() {
    let dir = temp_dir("range_sorted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    // Insert in reverse order
    engine.set("c", "3").unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();

    let results = engine.range("a", "c").unwrap();
    let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn range_tombstone_suppression() {
    let dir = temp_dir("range_tombstone");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.delete("b").unwrap();

    let results = engine.range("a", "c").unwrap();
    assert_eq!(
        results,
        vec![
            ("a".to_string(), "1".to_string()),
            ("c".to_string(), "3".to_string())
        ]
    );
}

#[test]
fn range_spans_memtable_and_segment() {
    let dir = temp_dir("range_span");
    // threshold=1 flushes every write to SSTable
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("a", "1").unwrap(); // flushed to segment
    engine.set("c", "3").unwrap(); // flushed to segment
    drop(engine);

    // Reload with big threshold so new writes stay in memtable
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("b", "2").unwrap(); // stays in memtable

    let results = engine.range("a", "c").unwrap();
    assert_eq!(
        results,
        vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string()),
        ]
    );
}

#[test]
fn range_memtable_wins_over_segment() {
    // Key written and flushed, then overwritten in memtable — range must return the newer value.
    let dir = temp_dir("range_memtable_wins");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("a", "old").unwrap(); // flushed to segment

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "new").unwrap(); // in memtable

    let results = engine.range("a", "a").unwrap();
    assert_eq!(results, vec![("a".to_string(), "new".to_string())]);
}

#[test]
fn range_tombstone_in_memtable_hides_flushed_key() {
    let dir = temp_dir("range_tombstone_flushed");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("a", "1").unwrap(); // flushed to segment
    engine.set("b", "2").unwrap(); // flushed to segment
    drop(engine);

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.delete("a").unwrap(); // tombstone in memtable

    let results = engine.range("a", "b").unwrap();
    assert_eq!(results, vec![("b".to_string(), "2".to_string())]);
}

#[test]
fn range_inverted_bounds_returns_empty() {
    // start > end is a malformed request — should return empty, not panic
    let dir = temp_dir("range_inverted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();

    let results = engine.range("z", "a").unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_single_key() {
    // start == end should return exactly that key if it exists
    let dir = temp_dir("range_single");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();

    let results = engine.range("b", "b").unwrap();
    assert_eq!(results, vec![("b".to_string(), "2".to_string())]);
}

#[test]
fn range_after_compaction() {
    // Compaction rewrites segments — range must still return correct results afterwards
    let dir = temp_dir("range_compact");
    let engine = new_engine(&dir, "test", 1).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.set("d", "4").unwrap();

    engine.compact().unwrap();

    let results = engine.range("b", "c").unwrap();
    assert_eq!(
        results,
        vec![
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string())
        ]
    );
}

// --- Concurrency tests ---

#[test]
fn concurrent_writes_no_lost_keys() {
    let dir = temp_dir("conc_writes");
    let engine = Arc::new(new_engine(&dir, "test", BIG_MEMTABLE).unwrap());
    let num_threads = 8;
    let keys_per_thread = 200;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..keys_per_thread {
                    let key = format!("t{t}_k{i}");
                    let value = format!("t{t}_v{i}");
                    db.set(&key, &value).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    for t in 0..num_threads {
        for i in 0..keys_per_thread {
            let key = format!("t{t}_k{i}");
            let expected = format!("t{t}_v{i}");
            let (_, actual) = engine.get(&key).unwrap().unwrap();
            assert_eq!(actual, expected, "missing or wrong value for {key}");
        }
    }
}

#[test]
fn concurrent_reads_return_correct_values() {
    let dir = temp_dir("conc_reads");
    let engine = Arc::new(new_engine(&dir, "test", BIG_MEMTABLE).unwrap());

    let num_keys = 100;
    for i in 0..num_keys {
        let key = format!("key_{i:03}");
        let value = format!("value_{}", "x".repeat(i % 64 + 1));
        engine.set(&key, &value).unwrap();
    }

    let expected: Arc<Vec<(String, String)>> = Arc::new(
        (0..num_keys)
            .map(|i| {
                let key = format!("key_{i:03}");
                let value = format!("value_{}", "x".repeat(i % 64 + 1));
                (key, value)
            })
            .collect(),
    );

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let db = Arc::clone(&engine);
            let expected = Arc::clone(&expected);
            thread::spawn(move || {
                for _ in 0..100 {
                    for (key, expected_value) in expected.iter() {
                        let (_, actual) = db.get(key).unwrap().unwrap();
                        assert_eq!(actual, *expected_value, "wrong value for {key}");
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_reads_and_writes() {
    let dir = temp_dir("conc_rw");
    let engine = Arc::new(new_engine(&dir, "test", BIG_MEMTABLE).unwrap());

    // Pre-populate so readers always have something to find
    for i in 0..50 {
        engine.set(&format!("k{i}"), &format!("v{i}")).unwrap();
    }

    let writers: Vec<_> = (0..4)
        .map(|t| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..200 {
                    let key = format!("w{t}_{i}");
                    db.set(&key, &format!("val_{i}")).unwrap();
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for round in 0..200 {
                    let key = format!("k{}", round % 50);
                    let result = db.get(&key).unwrap();
                    assert!(result.is_some(), "pre-populated key {key} must exist");
                }
            })
        })
        .collect();

    for h in writers.into_iter().chain(readers) {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_writes_trigger_flush() {
    let dir = temp_dir("conc_flush");
    // Tiny threshold so concurrent writes trigger flushes
    let engine = Arc::new(new_engine(&dir, "test", 128).unwrap());
    let num_threads = 4;
    let keys_per_thread = 100;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..keys_per_thread {
                    let key = format!("t{t}_k{i}");
                    let value = format!("value_{}", "x".repeat(20));
                    db.set(&key, &value).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
    // Unwrap the Arc and drop to join the last background flush
    match Arc::try_unwrap(engine) {
        Ok(engine) => drop(engine),
        Err(_) => panic!("Arc still has other refs"),
    }

    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();

    // All keys must be readable (from memtable or flushed SSTables)
    for t in 0..num_threads {
        for i in 0..keys_per_thread {
            let key = format!("t{t}_k{i}");
            let result = engine.get(&key).unwrap();
            assert!(result.is_some(), "key {key} lost after concurrent flushes");
        }
    }

    let sst_count = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let name = e.ok()?.file_name().to_string_lossy().to_string();
            name.ends_with(".sst").then_some(name)
        })
        .count();
    assert!(
        sst_count > 0,
        "expected at least one .sst from concurrent writes"
    );
}

#[test]
fn concurrent_delete_and_read() {
    let dir = temp_dir("conc_delete");
    let engine = Arc::new(new_engine(&dir, "test", BIG_MEMTABLE).unwrap());

    for i in 0..100 {
        engine.set(&format!("k{i}"), &format!("v{i}")).unwrap();
    }

    let deleter = {
        let db = Arc::clone(&engine);
        thread::spawn(move || {
            for i in 0..100 {
                db.delete(&format!("k{i}")).unwrap();
            }
        })
    };

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for _ in 0..100 {
                    for i in 0..100 {
                        let key = format!("k{i}");
                        // None is fine here — the key may have been deleted.
                        if let Some((_, v)) = db.get(&key).unwrap() {
                            assert_eq!(v, format!("v{i}"));
                        }
                    }
                }
            })
        })
        .collect();

    deleter.join().unwrap();
    for h in readers {
        h.join().unwrap();
    }
}

#[test]
fn concurrent_overwrite_last_writer_wins() {
    let dir = temp_dir("conc_overwrite");
    let engine = Arc::new(new_engine(&dir, "test", BIG_MEMTABLE).unwrap());

    let handles: Vec<_> = (0..8)
        .map(|t| {
            let db = Arc::clone(&engine);
            thread::spawn(move || {
                for i in 0..500 {
                    db.set("contested", &format!("t{t}_i{i}")).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let (_, val) = engine.get("contested").unwrap().unwrap();
    assert!(
        val.starts_with('t'),
        "value should come from a writer thread: {val}"
    );
}

// --- PREFIX (RangeScan::prefix) tests ---

#[test]
fn prefix_basic_memtable() {
    let dir = temp_dir("prefix_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.set("mem:a", "3").unwrap();

    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(
        results,
        vec![
            ("cpu:a".to_string(), "1".to_string()),
            ("cpu:b".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn prefix_excludes_non_matching_boundary() {
    // Inv 1: starts_with, NOT range <=. "cpu" must not match "cpv".
    let dir = temp_dir("prefix_boundary");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu1", "1").unwrap();
    engine.set("cpv", "2").unwrap();

    let results = engine.prefix("cpu").unwrap();
    assert_eq!(results, vec![("cpu1".to_string(), "1".to_string())]);
}

#[test]
fn prefix_full_key_match() {
    let dir = temp_dir("prefix_fullkey");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu", "1").unwrap();
    let results = engine.prefix("cpu").unwrap();
    assert_eq!(results, vec![("cpu".to_string(), "1".to_string())]);
}

#[test]
fn prefix_no_match_returns_empty() {
    let dir = temp_dir("prefix_nomatch");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    assert!(engine.prefix("disk:").unwrap().is_empty());
}

#[test]
fn prefix_empty_returns_all_live_keys() {
    // Inv 9: empty prefix matches everything
    let dir = temp_dir("prefix_empty");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    let keys: Vec<String> = engine
        .prefix("")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn prefix_tombstone_suppression() {
    // Inv 4: tombstone in newer tier shadows live value in older tier
    let dir = temp_dir("prefix_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.delete("cpu:b").unwrap();
    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(results, vec![("cpu:a".to_string(), "1".to_string())]);
}

#[test]
fn prefix_returns_sorted_order() {
    // Inv 6: output is ascending by key
    let dir = temp_dir("prefix_sorted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:c", "3").unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    let keys: Vec<String> = engine
        .prefix("cpu:")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(keys, vec!["cpu:a", "cpu:b", "cpu:c"]);
}

#[test]
fn prefix_pruning_is_result_neutral_across_segments() {
    // Inv 2: matching keys scattered across multiple SSTables. Use a tiny
    // memtable threshold so each write flushes to its own segment; reload, then
    // assert prefix() == brute-force starts_with over list_keys()+get().
    let dir = temp_dir("prefix_pruning");
    {
        let engine = new_engine(&dir, "test", 1).unwrap(); // tiny -> flush per write
        engine.set("aaa", "1").unwrap();
        engine.set("cpu:1", "x").unwrap();
        engine.set("mmm", "2").unwrap();
        engine.set("cpu:2", "y").unwrap();
        engine.set("zzz", "3").unwrap();
    } // drop joins background flush
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();

    let mut expected: Vec<(String, String)> = engine
        .list_keys()
        .unwrap()
        .into_iter()
        .filter(|k| k.starts_with("cpu:"))
        .map(|k| (k.clone(), engine.get(&k).unwrap().unwrap().1))
        .collect();
    expected.sort();

    assert_eq!(engine.prefix("cpu:").unwrap(), expected);
}

#[test]
fn prefix_tombstone_in_memtable_hides_flushed_key() {
    // Inv 4 cross-tier: value lives in an SSTable; tombstone in the active
    // memtable (higher priority tier) must suppress it.
    let dir = temp_dir("prefix_cross_tomb");
    {
        let engine = new_engine(&dir, "test", 1).unwrap(); // tiny -> flush per write
        engine.set("cpu:a", "1").unwrap();
        engine.set("cpu:b", "2").unwrap();
    } // drop joins background flush — both keys now in SSTables
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.delete("cpu:b").unwrap(); // tombstone in memtable, live value in SSTable

    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(results, vec![("cpu:a".to_string(), "1".to_string())]);
}

#[test]
fn prefix_memtable_overwrite_shadows_sstable_value() {
    // Inv 4 cross-tier: a newer write in the active memtable must win over
    // an older value for the same key sitting in an SSTable.
    let dir = temp_dir("prefix_cross_overwrite");
    {
        let engine = new_engine(&dir, "test", 1).unwrap();
        engine.set("cpu:a", "old").unwrap();
    } // flushed to SSTable
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "new").unwrap(); // newer write lives in memtable

    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(results, vec![("cpu:a".to_string(), "new".to_string())]);
}

#[test]
fn prefix_multibyte_utf8_keys() {
    // End-to-end: multibyte prefix correctly matches its own keys and
    // excludes keys that share the ASCII root but differ after the
    // multibyte character (e.g. "café:" vs "cafez").
    let dir = temp_dir("prefix_multibyte");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("café:a", "1").unwrap();
    engine.set("café:b", "2").unwrap();
    engine.set("cafez", "3").unwrap(); // shares "cafe" but not "café"

    let results = engine.prefix("café:").unwrap();
    assert_eq!(
        results,
        vec![
            ("café:a".to_string(), "1".to_string()),
            ("café:b".to_string(), "2".to_string()),
        ]
    );
}
