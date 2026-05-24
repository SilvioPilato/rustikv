use rustikv::engine::{RangeScan, StorageEngine};
use rustikv::lsmengine::LsmEngine;
use rustikv::size_tiered::SizeTiered;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_lsm_count_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn new_engine(dir: &str, db_name: &str, max_memtable_bytes: usize) -> LsmEngine {
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(dir, db_name, max_memtable_bytes, strategy, 4096, true).unwrap()
}

fn engine_from_dir(dir: &str, db_name: &str, max_memtable_bytes: usize) -> LsmEngine {
    let strategy = Box::new(SizeTiered::load_from_dir(dir, db_name, 4, 32, 4096, true).unwrap());
    LsmEngine::from_dir(dir, db_name, max_memtable_bytes, strategy, 4096, true).unwrap()
}

fn past_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 10_000
}

const BIG_MEMTABLE: usize = 1_048_576;

// ── count_prefix ──────────────────────────────────────────────────────────────

#[test]
fn count_prefix_basic_memtable() {
    let dir = temp_dir("cp_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.set("mem:a", "3").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 2);
}

#[test]
fn count_prefix_no_match_returns_zero() {
    let dir = temp_dir("cp_nomatch");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();

    assert_eq!(engine.count_prefix("disk:").unwrap(), 0);
}

#[test]
fn count_prefix_tombstone_excluded() {
    let dir = temp_dir("cp_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.delete("cpu:b").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_expired_excluded() {
    let dir = temp_dir("cp_exp");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set_with_ttl("cpu:b", "2", Some(past_ms())).unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_empty_counts_all_live() {
    let dir = temp_dir("cp_empty");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();

    assert_eq!(engine.count_prefix("").unwrap(), 3);
}

#[test]
fn count_prefix_full_key_match() {
    // prefix == key should count that one key
    let dir = temp_dir("cp_fullkey");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu", "1").unwrap();

    assert_eq!(engine.count_prefix("cpu").unwrap(), 1);
}

#[test]
fn count_prefix_boundary_not_substring_range() {
    // "cpu:" must NOT count "cpuz" — starts_with, not <= comparison
    let dir = temp_dir("cp_boundary");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpuz", "2").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_tombstone_in_memtable_hides_flushed() {
    let dir = temp_dir("cp_cross_tomb");
    {
        let engine = new_engine(&dir, "test", 1); // tiny → flush per write
        engine.set("cpu:a", "1").unwrap();
        engine.set("cpu:b", "2").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("cpu:b").unwrap(); // tombstone in memtable, value in SSTable

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_memtable_overwrite_shadows_sstable() {
    let dir = temp_dir("cp_overwrite");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("cpu:a", "old").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "new").unwrap();

    // Still 1 key (not 2) — overwrite, not a new key
    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_multi_segment_spread() {
    let dir = temp_dir("cp_multiseg");
    {
        let engine = new_engine(&dir, "test", 1); // flush per write
        engine.set("aaa", "x").unwrap();
        engine.set("cpu:1", "1").unwrap();
        engine.set("mmm", "x").unwrap();
        engine.set("cpu:2", "2").unwrap();
        engine.set("zzz", "x").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 2);
}

// ── count_range ───────────────────────────────────────────────────────────────

#[test]
fn count_range_basic_memtable() {
    let dir = temp_dir("cr_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.set("d", "4").unwrap();

    assert_eq!(engine.count_range("b", "c").unwrap(), 2);
}

#[test]
fn count_range_inverted_bounds_returns_zero() {
    let dir = temp_dir("cr_inverted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();

    assert_eq!(engine.count_range("z", "a").unwrap(), 0);
}

#[test]
fn count_range_tombstone_excluded() {
    let dir = temp_dir("cr_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.delete("b").unwrap();

    assert_eq!(engine.count_range("a", "c").unwrap(), 2);
}

#[test]
fn count_range_expired_excluded() {
    let dir = temp_dir("cr_exp");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set_with_ttl("b", "2", Some(past_ms())).unwrap();
    engine.set("c", "3").unwrap();

    assert_eq!(engine.count_range("a", "c").unwrap(), 2);
}

#[test]
fn count_range_spans_memtable_and_segment() {
    let dir = temp_dir("cr_span");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("c", "3").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.set("b", "2").unwrap(); // stays in memtable

    assert_eq!(engine.count_range("a", "c").unwrap(), 3);
}

#[test]
fn count_range_tombstone_in_memtable_hides_flushed() {
    let dir = temp_dir("cr_cross_tomb");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("b", "2").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("a").unwrap();

    assert_eq!(engine.count_range("a", "b").unwrap(), 1);
}

// ── Differential equivalence (invariant 5) ───────────────────────────────────

#[test]
fn count_prefix_equals_prefix_len() {
    // count_prefix(p) must equal prefix(p).len() for any engine state
    let dir = temp_dir("cp_diff");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("aaa", "x").unwrap();
        engine.set("cpu:1", "v1").unwrap();
        engine.set("cpu:2", "v2").unwrap();
        engine.set("mmm", "x").unwrap();
        engine.set("cpu:3", "v3").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("cpu:2").unwrap(); // tombstone in memtable

    for prefix in &["cpu:", "aaa", "mmm", "disk:", ""] {
        let via_count = engine.count_prefix(prefix).unwrap();
        let via_prefix = engine.prefix(prefix).unwrap().len();
        assert_eq!(
            via_count, via_prefix,
            "count_prefix({prefix:?}) = {via_count} but prefix({prefix:?}).len() = {via_prefix}"
        );
    }
}

#[test]
fn count_range_equals_range_len() {
    let dir = temp_dir("cr_diff");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("b", "2").unwrap();
        engine.set("c", "3").unwrap();
        engine.set("d", "4").unwrap();
        engine.set("e", "5").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("c").unwrap();

    for (start, end) in &[("a", "e"), ("b", "d"), ("z", "a"), ("c", "c"), ("a", "a")] {
        let via_count = engine.count_range(start, end).unwrap();
        let via_range = engine.range(start, end).unwrap().len();
        assert_eq!(
            via_count, via_range,
            "count_range({start:?},{end:?}) = {via_count} but range({start:?},{end:?}).len() = {via_range}"
        );
    }
}
