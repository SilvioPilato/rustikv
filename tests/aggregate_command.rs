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
