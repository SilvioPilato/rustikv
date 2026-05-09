use rustikv::bffp::{Command, ResponseStatus, decode_response_frame};
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::server::{CompactionCfg, dispatch};
use rustikv::settings::FSyncStrategy;
use rustikv::stats::Stats;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::{env, fs, thread};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tid = thread::current().id();
    let mut path = env::temp_dir();
    path.push(format!("kv_dispatch_{}_{}_{:?}", suffix, nanos, tid));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn make_engine(suffix: &str) -> (Arc<dyn StorageEngine>, String) {
    let dir = temp_dir(suffix);
    let engine = KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).expect("engine");
    (Arc::new(engine), dir)
}

fn cfg() -> CompactionCfg {
    CompactionCfg {
        ratio: 0.0,
        max_segment: 0,
    }
}

#[test]
fn dispatch_ping_returns_pong() {
    let (engine, _dir) = make_engine("ping");
    let stats = Arc::new(Stats::new());

    let response = dispatch(Command::Ping, &engine, &stats, &cfg());

    let decoded = decode_response_frame(&response).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["PONG".to_string()]);
}

#[test]
fn dispatch_write_then_read_round_trips() {
    let (engine, _dir) = make_engine("write_read");
    let stats = Arc::new(Stats::new());

    let write_resp = dispatch(
        Command::Write("k1".to_string(), "v1".to_string()),
        &engine,
        &stats,
        &cfg(),
    );
    let decoded = decode_response_frame(&write_resp).expect("decode write");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(stats.writes.load(Ordering::Relaxed), 1);

    let read_resp = dispatch(Command::Read("k1".to_string()), &engine, &stats, &cfg());
    let decoded = decode_response_frame(&read_resp).expect("decode read");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["v1".to_string()]);
    assert_eq!(stats.reads.load(Ordering::Relaxed), 1);
}

#[test]
fn dispatch_read_missing_returns_not_found() {
    let (engine, _dir) = make_engine("read_missing");
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::Read("nope".to_string()), &engine, &stats, &cfg());

    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::NotFound));
    assert_eq!(stats.reads.load(Ordering::Relaxed), 1);
}

#[test]
fn dispatch_delete_existing_key_then_read_misses() {
    let (engine, _dir) = make_engine("delete");
    let stats = Arc::new(Stats::new());

    let _ = dispatch(
        Command::Write("k".to_string(), "v".to_string()),
        &engine,
        &stats,
        &cfg(),
    );

    let del_resp = dispatch(Command::Delete("k".to_string()), &engine, &stats, &cfg());
    let decoded = decode_response_frame(&del_resp).expect("decode delete");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(stats.deletes.load(Ordering::Relaxed), 1);

    let read_resp = dispatch(Command::Read("k".to_string()), &engine, &stats, &cfg());
    let decoded = decode_response_frame(&read_resp).expect("decode read");
    assert!(matches!(decoded.status, ResponseStatus::NotFound));
}

#[test]
fn dispatch_delete_missing_returns_not_found() {
    let (engine, _dir) = make_engine("delete_missing");
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::Delete("nope".to_string()), &engine, &stats, &cfg());

    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::NotFound));
    assert_eq!(stats.deletes.load(Ordering::Relaxed), 0);
}

#[test]
fn dispatch_list_returns_all_keys() {
    let (engine, _dir) = make_engine("list");
    let stats = Arc::new(Stats::new());

    let _ = dispatch(
        Command::Write("a".to_string(), "1".to_string()),
        &engine,
        &stats,
        &cfg(),
    );
    let _ = dispatch(
        Command::Write("b".to_string(), "2".to_string()),
        &engine,
        &stats,
        &cfg(),
    );

    let resp = dispatch(Command::List, &engine, &stats, &cfg());
    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    let mut keys = decoded.payload.clone();
    keys.sort();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn dispatch_exists_present_returns_ok() {
    let (engine, _dir) = make_engine("exists_present");
    let stats = Arc::new(Stats::new());

    let _ = dispatch(
        Command::Write("k".to_string(), "v".to_string()),
        &engine,
        &stats,
        &cfg(),
    );

    let resp = dispatch(Command::Exists("k".to_string()), &engine, &stats, &cfg());
    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
}

#[test]
fn dispatch_exists_missing_returns_not_found() {
    let (engine, _dir) = make_engine("exists_missing");
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::Exists("nope".to_string()), &engine, &stats, &cfg());

    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::NotFound));
}

#[test]
fn dispatch_invalid_op_code_returns_error() {
    let (engine, _dir) = make_engine("invalid");
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::Invalid(0xFF), &engine, &stats, &cfg());

    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::Error));
    assert!(decoded.payload[0].contains("Invalid op code"));
}

#[test]
fn dispatch_stats_returns_snapshot() {
    let (engine, _dir) = make_engine("stats");
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::Stats, &engine, &stats, &cfg());

    let decoded = decode_response_frame(&resp).expect("decode");
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert!(decoded.payload[0].contains("compacting"));
}
