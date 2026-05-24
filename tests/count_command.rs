use rustikv::bffp::{Command, decode_input_frame, encode_command};
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
