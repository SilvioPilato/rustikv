use rustikv::bffp::{
    Command, ResponseStatus, decode_input_frame, decode_response_frame, encode_command,
};
use rustikv::cli::{ParseResult, parse_command};

fn agg_cmd(line: &str) -> Command {
    match parse_command(line) {
        ParseResult::Cmd(c) => c,
        ParseResult::Quit => panic!("expected Cmd, got Quit"),
        ParseResult::InvalidInput(m) => panic!("expected Cmd, got InvalidInput: {m}"),
    }
}

fn agg_is_invalid(line: &str) -> bool {
    matches!(parse_command(line), ParseResult::InvalidInput(_))
}

// --- SUM ---

#[test]
fn cli_sum_arity_2_parses_as_prefix() {
    match agg_cmd("SUM cpu:") {
        Command::SumPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected SumPrefix"),
    }
}

#[test]
fn cli_sum_arity_3_parses_as_range() {
    match agg_cmd("SUM a z") {
        Command::SumRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected SumRange"),
    }
}

#[test]
fn cli_sum_wrong_arity_is_invalid() {
    assert!(agg_is_invalid("SUM"));
    assert!(agg_is_invalid("SUM a b c"));
}

#[test]
fn cli_sum_case_insensitive() {
    match agg_cmd("sum cpu:") {
        Command::SumPrefix(_) => {}
        _ => panic!("expected SumPrefix"),
    }
}

// --- AVG ---

#[test]
fn cli_avg_arity_2_parses_as_prefix() {
    match agg_cmd("AVG cpu:") {
        Command::AvgPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected AvgPrefix"),
    }
}

#[test]
fn cli_avg_arity_3_parses_as_range() {
    match agg_cmd("AVG a z") {
        Command::AvgRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected AvgRange"),
    }
}

#[test]
fn cli_avg_wrong_arity_is_invalid() {
    assert!(agg_is_invalid("AVG"));
    assert!(agg_is_invalid("AVG a b c"));
}

// --- MIN ---

#[test]
fn cli_min_arity_2_parses_as_prefix() {
    match agg_cmd("MIN cpu:") {
        Command::MinPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected MinPrefix"),
    }
}

#[test]
fn cli_min_arity_3_parses_as_range() {
    match agg_cmd("MIN a z") {
        Command::MinRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected MinRange"),
    }
}

#[test]
fn cli_min_wrong_arity_is_invalid() {
    assert!(agg_is_invalid("MIN"));
    assert!(agg_is_invalid("MIN a b c"));
}

// --- MAX ---

#[test]
fn cli_max_arity_2_parses_as_prefix() {
    match agg_cmd("MAX cpu:") {
        Command::MaxPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected MaxPrefix"),
    }
}

#[test]
fn cli_max_arity_3_parses_as_range() {
    match agg_cmd("MAX a z") {
        Command::MaxRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected MaxRange"),
    }
}

#[test]
fn cli_max_wrong_arity_is_invalid() {
    assert!(agg_is_invalid("MAX"));
    assert!(agg_is_invalid("MAX a b c"));
}

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
    path.push(format!("kv_agg_cmd_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn lsm(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    Arc::new(LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap())
}

fn kv(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    Arc::new(KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap())
}

fn no_compact() -> CompactionCfg {
    CompactionCfg {
        ratio: 0.0,
        max_segment: 0,
    }
}

fn ok_payload(db: &Arc<dyn StorageEngine>, cmd: Command) -> String {
    let stats = Arc::new(Stats::new());
    let resp = dispatch(cmd, db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(
        matches!(decoded.status, ResponseStatus::Ok),
        "expected Ok, got status byte={} payload={:?}",
        decoded.status as u8,
        decoded.payload
    );
    decoded.payload.into_iter().next().unwrap()
}

fn expect_not_found(db: &Arc<dyn StorageEngine>, cmd: Command) {
    let stats = Arc::new(Stats::new());
    let resp = dispatch(cmd, db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::NotFound));
}

fn expect_error(db: &Arc<dyn StorageEngine>, cmd: Command) -> String {
    let stats = Arc::new(Stats::new());
    let resp = dispatch(cmd, db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
    decoded.payload.into_iter().next().unwrap_or_default()
}

// --- BFFP round-trips ---

#[test]
fn sum_prefix_round_trips() {
    let frame = encode_command(Command::SumPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::SumPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected SumPrefix"),
    }
}

#[test]
fn sum_range_round_trips() {
    let frame = encode_command(Command::SumRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::SumRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected SumRange"),
    }
}

#[test]
fn avg_prefix_round_trips() {
    let frame = encode_command(Command::AvgPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::AvgPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected AvgPrefix"),
    }
}

#[test]
fn avg_range_round_trips() {
    let frame = encode_command(Command::AvgRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::AvgRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected AvgRange"),
    }
}

#[test]
fn min_prefix_round_trips() {
    let frame = encode_command(Command::MinPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::MinPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected MinPrefix"),
    }
}

#[test]
fn min_range_round_trips() {
    let frame = encode_command(Command::MinRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::MinRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected MinRange"),
    }
}

#[test]
fn max_prefix_round_trips() {
    let frame = encode_command(Command::MaxPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::MaxPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected MaxPrefix"),
    }
}

#[test]
fn max_range_round_trips() {
    let frame = encode_command(Command::MaxRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::MaxRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected MaxRange"),
    }
}

// --- Dispatch: correct values ---

#[test]
fn dispatch_sum_prefix_returns_correct_value() {
    let db = lsm("d_sp");
    db.set("cpu:a", "1.0").unwrap();
    db.set("cpu:b", "2.0").unwrap();
    db.set("mem:a", "99.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::SumPrefix("cpu:".to_string())),
        "3.0"
    );
}

#[test]
fn dispatch_sum_range_returns_correct_value() {
    let db = lsm("d_sr");
    db.set("a", "1.0").unwrap();
    db.set("b", "2.0").unwrap();
    db.set("c", "3.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::SumRange("a".to_string(), "b".to_string())),
        "3.0"
    );
}

#[test]
fn dispatch_avg_prefix_returns_correct_value() {
    let db = lsm("d_ap");
    db.set("cpu:a", "2.0").unwrap();
    db.set("cpu:b", "4.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::AvgPrefix("cpu:".to_string())),
        "3.0"
    );
}

#[test]
fn dispatch_avg_range_returns_correct_value() {
    let db = lsm("d_ar");
    db.set("a", "1.0").unwrap();
    db.set("b", "3.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::AvgRange("a".to_string(), "b".to_string())),
        "2.0"
    );
}

#[test]
fn dispatch_min_prefix_returns_correct_value() {
    let db = lsm("d_mp");
    db.set("cpu:a", "5.0").unwrap();
    db.set("cpu:b", "1.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::MinPrefix("cpu:".to_string())),
        "1.0"
    );
}

#[test]
fn dispatch_max_prefix_returns_correct_value() {
    let db = lsm("d_mx");
    db.set("cpu:a", "5.0").unwrap();
    db.set("cpu:b", "1.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::MaxPrefix("cpu:".to_string())),
        "5.0"
    );
}

#[test]
fn dispatch_min_range_returns_correct_value() {
    let db = lsm("d_mr");
    db.set("a", "3.0").unwrap();
    db.set("b", "1.0").unwrap();
    db.set("c", "2.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::MinRange("a".to_string(), "c".to_string())),
        "1.0"
    );
}

#[test]
fn dispatch_max_range_returns_correct_value() {
    let db = lsm("d_mxr");
    db.set("a", "3.0").unwrap();
    db.set("b", "1.0").unwrap();
    db.set("c", "2.0").unwrap();
    assert_eq!(
        ok_payload(&db, Command::MaxRange("a".to_string(), "c".to_string())),
        "3.0"
    );
}

// --- Dispatch: empty match → NotFound ---

#[test]
fn dispatch_sum_prefix_empty_is_not_found() {
    let db = lsm("d_sp_nf");
    expect_not_found(&db, Command::SumPrefix("cpu:".to_string()));
}

#[test]
fn dispatch_avg_range_inverted_is_not_found() {
    let db = lsm("d_ar_nf");
    db.set("a", "1.0").unwrap();
    expect_not_found(&db, Command::AvgRange("z".to_string(), "a".to_string()));
}

// --- Dispatch: non-numeric → Error ---

#[test]
fn dispatch_sum_prefix_non_numeric_is_error() {
    let db = lsm("d_sp_err");
    db.set("cpu:a", "bad").unwrap();
    let msg = expect_error(&db, Command::SumPrefix("cpu:".to_string()));
    assert!(msg.contains("SUM") && msg.contains("cpu:a"), "got: {msg}");
}

#[test]
fn dispatch_min_range_non_numeric_is_error() {
    let db = lsm("d_mr_err");
    db.set("a", "bad").unwrap();
    let msg = expect_error(&db, Command::MinRange("a".to_string(), "z".to_string()));
    assert!(msg.contains("MIN"), "got: {msg}");
}

// --- Dispatch: KV engine returns not-supported ---

#[test]
fn dispatch_sum_prefix_kv_not_supported() {
    expect_error(&kv("kv_sp"), Command::SumPrefix("cpu:".to_string()));
}

#[test]
fn dispatch_sum_range_kv_not_supported() {
    expect_error(
        &kv("kv_sr"),
        Command::SumRange("a".to_string(), "z".to_string()),
    );
}

#[test]
fn dispatch_avg_prefix_kv_not_supported() {
    expect_error(&kv("kv_ap"), Command::AvgPrefix("cpu:".to_string()));
}

#[test]
fn dispatch_avg_range_kv_not_supported() {
    expect_error(
        &kv("kv_ar"),
        Command::AvgRange("a".to_string(), "z".to_string()),
    );
}

#[test]
fn dispatch_min_prefix_kv_not_supported() {
    expect_error(&kv("kv_mp"), Command::MinPrefix("cpu:".to_string()));
}

#[test]
fn dispatch_min_range_kv_not_supported() {
    expect_error(
        &kv("kv_mr"),
        Command::MinRange("a".to_string(), "z".to_string()),
    );
}

#[test]
fn dispatch_max_prefix_kv_not_supported() {
    expect_error(&kv("kv_mxp"), Command::MaxPrefix("cpu:".to_string()));
}

#[test]
fn dispatch_max_range_kv_not_supported() {
    expect_error(
        &kv("kv_mxr"),
        Command::MaxRange("a".to_string(), "z".to_string()),
    );
}

// --- stats.reads bumped by exactly 1 ---

#[test]
fn aggregation_commands_bump_reads_by_one() {
    let db = lsm("d_stats");
    db.set("cpu:a", "1.0").unwrap();
    db.set("cpu:b", "2.0").unwrap();
    let stats = Arc::new(Stats::new());

    let cmds: Vec<Command> = vec![
        Command::SumPrefix("cpu:".to_string()),
        Command::SumRange("cpu:a".to_string(), "cpu:b".to_string()),
        Command::AvgPrefix("cpu:".to_string()),
        Command::AvgRange("cpu:a".to_string(), "cpu:b".to_string()),
        Command::MinPrefix("cpu:".to_string()),
        Command::MinRange("cpu:a".to_string(), "cpu:b".to_string()),
        Command::MaxPrefix("cpu:".to_string()),
        Command::MaxRange("cpu:a".to_string(), "cpu:b".to_string()),
    ];

    for cmd in cmds {
        let before = stats.reads.load(Ordering::Relaxed);
        dispatch(cmd, &db, &stats, &no_compact());
        let after = stats.reads.load(Ordering::Relaxed);
        assert_eq!(
            after - before,
            1,
            "each aggregation command must bump reads by 1"
        );
    }
}
