use rustikv::bffp::{
    Command, ResponseStatus, decode_input_frame, decode_response_frame, encode_command,
};
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::server::{CompactionCfg, dispatch};
use rustikv::settings::FSyncStrategy;
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_incr_cmd_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn kv_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir(suffix);
    Arc::new(KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap())
}

fn lsm_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir(suffix);
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    Arc::new(LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap())
}

fn no_compact() -> CompactionCfg {
    CompactionCfg {
        ratio: 0.0,
        max_segment: 0,
    }
}

fn stats() -> Arc<Stats> {
    Arc::new(Stats::new())
}

fn incr(
    engine: &Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    key: &str,
) -> rustikv::bffp::DecodedResponse {
    let resp = dispatch(Command::Incr(key.to_string()), engine, stats, &no_compact());
    decode_response_frame(&resp).unwrap()
}

// ---------------------------------------------------------------------------
// dispatch end-to-end — both engines
// ---------------------------------------------------------------------------

#[test]
fn incr_command_creates_and_returns_one_kv() {
    let engine = kv_engine("create_kv");
    let decoded = incr(&engine, &stats(), "c");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["1".to_string()]);
}

#[test]
fn incr_command_creates_and_returns_one_lsm() {
    let engine = lsm_engine("create_lsm");
    let decoded = incr(&engine, &stats(), "c");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["1".to_string()]);
}

#[test]
fn incr_command_increments_existing_kv() {
    let engine = kv_engine("incr_kv");
    let stats = stats();
    assert_eq!(incr(&engine, &stats, "c").payload, vec!["1".to_string()]);
    assert_eq!(incr(&engine, &stats, "c").payload, vec!["2".to_string()]);
    assert_eq!(incr(&engine, &stats, "c").payload, vec!["3".to_string()]);
}

#[test]
fn incr_command_increments_existing_lsm() {
    let engine = lsm_engine("incr_lsm");
    let stats = stats();
    assert_eq!(incr(&engine, &stats, "c").payload, vec!["1".to_string()]);
    assert_eq!(incr(&engine, &stats, "c").payload, vec!["2".to_string()]);
}

#[test]
fn incr_command_on_non_integer_returns_error_kv() {
    let engine = kv_engine("nonint_kv");
    let stats = stats();
    dispatch(
        Command::Write("c".to_string(), "abc".to_string(), None),
        &engine,
        &stats,
        &no_compact(),
    );
    assert!(matches!(
        incr(&engine, &stats, "c").status,
        ResponseStatus::Error
    ));
}

#[test]
fn incr_command_on_non_integer_returns_error_lsm() {
    let engine = lsm_engine("nonint_lsm");
    let stats = stats();
    dispatch(
        Command::Write("c".to_string(), "abc".to_string(), None),
        &engine,
        &stats,
        &no_compact(),
    );
    assert!(matches!(
        incr(&engine, &stats, "c").status,
        ResponseStatus::Error
    ));
}

// ---------------------------------------------------------------------------
// wire round-trip — INCR is a single-key frame (op | key_len | key)
// ---------------------------------------------------------------------------

#[test]
fn incr_command_roundtrips() {
    match decode_input_frame(&encode_command(Command::Incr("counter".into()))).unwrap() {
        Command::Incr(key) => assert_eq!(key, "counter"),
        _ => panic!("expected Command::Incr"),
    }
}

#[test]
fn incr_command_empty_key_roundtrips() {
    match decode_input_frame(&encode_command(Command::Incr(String::new()))).unwrap() {
        Command::Incr(key) => assert_eq!(key, ""),
        _ => panic!("expected Command::Incr"),
    }
}
