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
    path.push(format!("kv_lsm_agg_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn new_engine(dir: &str) -> LsmEngine {
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(dir, "test", 1_048_576, strategy, 4096, true).unwrap()
}

fn past_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 10_000
}

// ── sum_prefix ────────────────────────────────────────────────────────────────

#[test]
fn sum_prefix_basic() {
    let engine = new_engine(&temp_dir("sp_basic"));
    engine.set("cpu:a", "1.0").unwrap();
    engine.set("cpu:b", "2.0").unwrap();
    engine.set("mem:a", "99.0").unwrap();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(3.0));
}

#[test]
fn sum_prefix_empty_match_returns_none() {
    let engine = new_engine(&temp_dir("sp_empty"));
    engine.set("cpu:a", "1.0").unwrap();
    assert_eq!(engine.sum_prefix("disk:").unwrap(), None);
}

#[test]
fn sum_prefix_tombstone_excluded() {
    let engine = new_engine(&temp_dir("sp_tomb"));
    engine.set("cpu:a", "1.0").unwrap();
    engine.set("cpu:b", "2.0").unwrap();
    engine.delete("cpu:b").unwrap();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
}

#[test]
fn sum_prefix_expired_excluded() {
    let engine = new_engine(&temp_dir("sp_exp"));
    engine.set("cpu:a", "1.0").unwrap();
    engine
        .set_with_ttl("cpu:b", "2.0", Some(past_ms()))
        .unwrap();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
}

#[test]
fn sum_prefix_non_numeric_live_value_errors() {
    let engine = new_engine(&temp_dir("sp_err"));
    engine.set("cpu:a", "1.0").unwrap();
    engine.set("cpu:b", "not-a-number").unwrap();
    let err = engine.sum_prefix("cpu:").unwrap_err();
    assert!(
        err.to_string().contains("SUM") && err.to_string().contains("cpu:b"),
        "error should mention SUM and the key, got: {}",
        err
    );
}

#[test]
fn sum_prefix_non_numeric_tombstoned_does_not_error() {
    let engine = new_engine(&temp_dir("sp_tomb_nn"));
    engine.set("cpu:a", "1.0").unwrap();
    engine.set("cpu:b", "not-a-number").unwrap();
    engine.delete("cpu:b").unwrap();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
}

#[test]
fn sum_prefix_non_numeric_expired_does_not_error() {
    let engine = new_engine(&temp_dir("sp_exp_nn"));
    engine.set("cpu:a", "1.0").unwrap();
    engine
        .set_with_ttl("cpu:b", "not-a-number", Some(past_ms()))
        .unwrap();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
}

// ── sum_range ─────────────────────────────────────────────────────────────────

#[test]
fn sum_range_basic() {
    let engine = new_engine(&temp_dir("sr_basic"));
    engine.set("a", "1.0").unwrap();
    engine.set("b", "2.0").unwrap();
    engine.set("c", "3.0").unwrap();
    engine.set("z", "99.0").unwrap();
    assert_eq!(engine.sum_range("a", "c").unwrap(), Some(6.0));
}

#[test]
fn sum_range_inverted_returns_none() {
    let engine = new_engine(&temp_dir("sr_inv"));
    engine.set("a", "1.0").unwrap();
    assert_eq!(engine.sum_range("z", "a").unwrap(), None);
}

#[test]
fn sum_range_empty_match_returns_none() {
    let engine = new_engine(&temp_dir("sr_empty"));
    engine.set("a", "1.0").unwrap();
    assert_eq!(engine.sum_range("x", "z").unwrap(), None);
}

// ── avg ───────────────────────────────────────────────────────────────────────

#[test]
fn avg_prefix_basic() {
    let engine = new_engine(&temp_dir("ap_basic"));
    engine.set("cpu:a", "1.0").unwrap();
    engine.set("cpu:b", "3.0").unwrap();
    assert_eq!(engine.avg_prefix("cpu:").unwrap(), Some(2.0));
}

#[test]
fn avg_prefix_single_key_equals_value() {
    let engine = new_engine(&temp_dir("ap_single"));
    engine.set("cpu:a", "7.5").unwrap();
    assert_eq!(engine.avg_prefix("cpu:").unwrap(), Some(7.5));
}

#[test]
fn avg_range_basic() {
    let engine = new_engine(&temp_dir("ar_basic"));
    engine.set("a", "2.0").unwrap();
    engine.set("b", "4.0").unwrap();
    assert_eq!(engine.avg_range("a", "b").unwrap(), Some(3.0));
}

// ── min/max ───────────────────────────────────────────────────────────────────

#[test]
fn min_prefix_basic() {
    let engine = new_engine(&temp_dir("mp_basic"));
    engine.set("cpu:a", "3.0").unwrap();
    engine.set("cpu:b", "1.0").unwrap();
    engine.set("cpu:c", "2.0").unwrap();
    assert_eq!(engine.min_prefix("cpu:").unwrap(), Some(1.0));
}

#[test]
fn max_prefix_basic() {
    let engine = new_engine(&temp_dir("mx_basic"));
    engine.set("cpu:a", "3.0").unwrap();
    engine.set("cpu:b", "1.0").unwrap();
    engine.set("cpu:c", "2.0").unwrap();
    assert_eq!(engine.max_prefix("cpu:").unwrap(), Some(3.0));
}

#[test]
fn min_max_identical_values() {
    let engine = new_engine(&temp_dir("mm_ident"));
    engine.set("cpu:a", "5.0").unwrap();
    engine.set("cpu:b", "5.0").unwrap();
    assert_eq!(engine.min_prefix("cpu:").unwrap(), Some(5.0));
    assert_eq!(engine.max_prefix("cpu:").unwrap(), Some(5.0));
}

#[test]
fn min_range_basic() {
    let engine = new_engine(&temp_dir("mr_basic"));
    engine.set("a", "10.0").unwrap();
    engine.set("b", "2.0").unwrap();
    engine.set("c", "7.0").unwrap();
    assert_eq!(engine.min_range("a", "c").unwrap(), Some(2.0));
}

#[test]
fn max_range_basic() {
    let engine = new_engine(&temp_dir("mxr_basic"));
    engine.set("a", "10.0").unwrap();
    engine.set("b", "2.0").unwrap();
    engine.set("c", "7.0").unwrap();
    assert_eq!(engine.max_range("a", "c").unwrap(), Some(10.0));
}

// ── multi-segment spread ──────────────────────────────────────────────────────

#[test]
fn sum_prefix_multi_segment() {
    let dir = temp_dir("sp_multi");
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    let engine = LsmEngine::new(&dir, "test", 64, strategy, 4096, true).unwrap();
    for i in 0..20 {
        engine
            .set(&format!("cpu:{:03}", i), &format!("{}.0", i))
            .unwrap();
    }
    let expected: f64 = (0..20).map(|i| i as f64).sum();
    assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(expected));
}

// ── differential equivalence ─────────────────────────────────────────────────

#[test]
fn sum_prefix_differential_equivalence() {
    let dir = temp_dir("sp_diff");
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    let engine = LsmEngine::new(&dir, "test", 64, strategy, 4096, true).unwrap();
    for i in 0..10 {
        engine
            .set(&format!("k:{:02}", i), &format!("{}.0", i))
            .unwrap();
    }
    engine.set("k:03", "30.0").unwrap();
    engine.delete("k:05").unwrap();
    let via_prefix: f64 = engine
        .prefix("k:")
        .unwrap()
        .iter()
        .map(|(_, v)| v.parse::<f64>().unwrap())
        .sum();
    assert_eq!(engine.sum_prefix("k:").unwrap(), Some(via_prefix));
}
