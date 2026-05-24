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
    path.push(format!("kv_prefix_cmd_{}_{}", suffix, nanos));
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

// --- Round-trip (codec only) ---

#[test]
fn prefix_command_round_trips() {
    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::Prefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected Command::Prefix"),
    }
}

#[test]
fn prefix_command_round_trips_empty() {
    let frame = encode_command(Command::Prefix(String::new()));
    match decode_input_frame(&frame).unwrap() {
        Command::Prefix(p) => assert_eq!(p, ""),
        _ => panic!("expected Command::Prefix"),
    }
}

// --- End-to-end dispatch ---

#[test]
fn dispatch_prefix_on_lsm_returns_matching_pairs() {
    let db = lsm_engine("disp_ok");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("mem:a", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    let cmd = decode_input_frame(&frame).unwrap();
    let resp = dispatch(cmd, &db, &stats, &no_compact());

    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    // payload flattens to [k, v, k, v] in sorted key order
    assert_eq!(decoded.payload, vec!["cpu:a", "1", "cpu:b", "2"]);
}

#[test]
fn dispatch_prefix_on_kv_is_not_supported() {
    let db = kv_engine("disp_kv");
    let stats = Arc::new(Stats::new());
    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    let cmd = decode_input_frame(&frame).unwrap();
    let resp = dispatch(cmd, &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}
