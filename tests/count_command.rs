use rustikv::bffp::{Command, decode_input_frame, decode_response_frame, encode_command};
use rustikv::cli::{ParseResult, parse_command};

// --- CountPrefix round-trips ---

#[test]
fn count_prefix_command_round_trips() {
    let frame = encode_command(Command::CountPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected Command::CountPrefix"),
    }
}

#[test]
fn count_prefix_command_round_trips_empty() {
    let frame = encode_command(Command::CountPrefix(String::new()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountPrefix(p) => assert_eq!(p, ""),
        _ => panic!("expected Command::CountPrefix"),
    }
}

// --- CountRange round-trips ---

#[test]
fn count_range_command_round_trips() {
    let frame = encode_command(Command::CountRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected Command::CountRange"),
    }
}

#[test]
fn count_range_command_round_trips_equal_bounds() {
    let frame = encode_command(Command::CountRange("k".to_string(), "k".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountRange(s, e) => {
            assert_eq!(s, "k");
            assert_eq!(e, "k");
        }
        _ => panic!("expected Command::CountRange"),
    }
}

// --- CLI parse tests ---

fn count_cmd(line: &str) -> Command {
    match parse_command(line) {
        ParseResult::Cmd(c) => c,
        ParseResult::Quit => panic!("expected Cmd, got Quit"),
        ParseResult::InvalidInput(m) => panic!("expected Cmd, got InvalidInput: {m}"),
    }
}

fn count_is_invalid(line: &str) -> bool {
    matches!(parse_command(line), ParseResult::InvalidInput(_))
}

#[test]
fn cli_count_arity_2_parses_as_prefix() {
    match count_cmd("COUNT cpu:") {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected CountPrefix"),
    }
}

#[test]
fn cli_count_arity_3_parses_as_range() {
    match count_cmd("COUNT a z") {
        Command::CountRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected CountRange"),
    }
}

#[test]
fn cli_count_arity_1_is_invalid() {
    assert!(count_is_invalid("COUNT"));
}

#[test]
fn cli_count_arity_4_is_invalid() {
    assert!(count_is_invalid("COUNT a b c"));
}

#[test]
fn cli_count_case_insensitive() {
    match count_cmd("count cpu:") {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected CountPrefix"),
    }
}

use rustikv::bffp::ResponseStatus;
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::server::{CompactionCfg, dispatch};
use rustikv::settings::FSyncStrategy;
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn temp_dir_d(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_count_cmd_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn kv_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    Arc::new(KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap())
}

fn lsm_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    Arc::new(LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap())
}

fn no_compact() -> CompactionCfg {
    CompactionCfg {
        ratio: 0.0,
        max_segment: 0,
    }
}

// --- End-to-end dispatch ---

#[test]
fn dispatch_count_prefix_on_lsm_returns_correct_count() {
    let db = lsm_engine("disp_cp");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("mem:a", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let resp = dispatch(
        Command::CountPrefix("cpu:".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let decoded = decode_response_frame(&resp).unwrap();

    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["2".to_string()]);
}

#[test]
fn dispatch_count_range_on_lsm_returns_correct_count() {
    let db = lsm_engine("disp_cr");
    db.set("a", "1").unwrap();
    db.set("b", "2").unwrap();
    db.set("c", "3").unwrap();
    db.set("d", "4").unwrap();
    let stats = Arc::new(Stats::new());

    let resp = dispatch(
        Command::CountRange("b".to_string(), "c".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let decoded = decode_response_frame(&resp).unwrap();

    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["2".to_string()]);
}

#[test]
fn dispatch_count_prefix_on_kv_is_not_supported() {
    let db = kv_engine("disp_cp_kv");
    let stats = Arc::new(Stats::new());
    let resp = dispatch(
        Command::CountPrefix("cpu:".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}

#[test]
fn dispatch_count_range_on_kv_is_not_supported() {
    let db = kv_engine("disp_cr_kv");
    let stats = Arc::new(Stats::new());
    let resp = dispatch(
        Command::CountRange("a".to_string(), "z".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}

#[test]
fn count_command_bumps_reads_by_one_not_by_match_count() {
    // The dispatch arms must bump stats.reads by 1 per call (not by the number of
    // matched keys). This pins the deliberate divergence from RANGE/PREFIX.
    let db = lsm_engine("disp_stats");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("cpu:c", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let before = stats.reads.load(Ordering::Relaxed);
    dispatch(
        Command::CountPrefix("cpu:".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let after = stats.reads.load(Ordering::Relaxed);

    assert_eq!(
        after - before,
        1,
        "COUNT must bump stats.reads by 1, not by the 3 matched keys"
    );

    let before = stats.reads.load(Ordering::Relaxed);
    dispatch(
        Command::CountRange("cpu:a".to_string(), "cpu:c".to_string()),
        &db,
        &stats,
        &no_compact(),
    );
    let after = stats.reads.load(Ordering::Relaxed);

    assert_eq!(
        after - before,
        1,
        "COUNT <start> <end> must also bump reads by 1"
    );
}
