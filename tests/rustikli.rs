use rustikv::bffp::Command;
use rustikv::cli::{ParseResult, parse_command};

fn as_cmd(result: ParseResult) -> Command {
    match result {
        ParseResult::Cmd(cmd) => cmd,
        _ => panic!("expected ParseResult::Cmd"),
    }
}

fn as_error(result: ParseResult) -> String {
    match result {
        ParseResult::InvalidInput(msg) => msg,
        _ => panic!("expected ParseResult::InvalidInput"),
    }
}

// --- WRITE ---

#[test]
fn write_parses_key_and_value() {
    let cmd = as_cmd(parse_command("WRITE foo bar"));
    assert!(matches!(cmd, Command::Write(k, v, _) if k == "foo" && v == "bar"));
}

#[test]
fn write_joins_multi_word_value() {
    let cmd = as_cmd(parse_command("WRITE key hello world"));
    assert!(matches!(cmd, Command::Write(_, v, _) if v == "hello world"));
}

#[test]
fn write_is_case_insensitive() {
    let cmd = as_cmd(parse_command("write foo bar"));
    assert!(matches!(cmd, Command::Write(_, _, _)));
}

#[test]
fn write_missing_value_returns_error() {
    let msg = as_error(parse_command("WRITE foo"));
    assert!(!msg.is_empty());
}

#[test]
fn write_missing_key_and_value_returns_error() {
    let msg = as_error(parse_command("WRITE"));
    assert!(!msg.is_empty());
}

// --- READ ---

#[test]
fn read_parses_key() {
    let cmd = as_cmd(parse_command("READ foo"));
    assert!(matches!(cmd, Command::Read(k) if k == "foo"));
}

#[test]
fn read_missing_key_returns_error() {
    let msg = as_error(parse_command("READ"));
    assert!(!msg.is_empty());
}

#[test]
fn read_extra_args_returns_error() {
    let msg = as_error(parse_command("READ foo bar"));
    assert!(!msg.is_empty());
}

// --- DELETE ---

#[test]
fn delete_parses_key() {
    let cmd = as_cmd(parse_command("DELETE foo"));
    assert!(matches!(cmd, Command::Delete(k) if k == "foo"));
}

#[test]
fn delete_missing_key_returns_error() {
    let msg = as_error(parse_command("DELETE"));
    assert!(!msg.is_empty());
}

// --- EXISTS ---

#[test]
fn exists_parses_key() {
    let cmd = as_cmd(parse_command("EXISTS foo"));
    assert!(matches!(cmd, Command::Exists(k) if k == "foo"));
}

#[test]
fn exists_missing_key_returns_error() {
    let msg = as_error(parse_command("EXISTS"));
    assert!(!msg.is_empty());
}

// --- No-arg commands ---

#[test]
fn compact_parses() {
    assert!(matches!(as_cmd(parse_command("COMPACT")), Command::Compact));
}

#[test]
fn stats_parses() {
    assert!(matches!(as_cmd(parse_command("STATS")), Command::Stats));
}

#[test]
fn list_parses() {
    assert!(matches!(as_cmd(parse_command("LIST")), Command::List));
}

// --- QUIT ---

#[test]
fn quit_returns_quit() {
    assert!(matches!(parse_command("QUIT"), ParseResult::Quit));
}

#[test]
fn quit_is_case_insensitive() {
    assert!(matches!(parse_command("quit"), ParseResult::Quit));
}

// --- PREFIX ---

#[test]
fn parses_prefix_command() {
    let cmd = as_cmd(parse_command("PREFIX cpu:"));
    assert!(matches!(cmd, Command::Prefix(p) if p == "cpu:"));
}

#[test]
fn prefix_wrong_arity_is_invalid() {
    assert!(matches!(
        parse_command("PREFIX"),
        ParseResult::InvalidInput(_)
    ));
    assert!(matches!(
        parse_command("PREFIX a b"),
        ParseResult::InvalidInput(_)
    ));
}

// --- Edge cases ---

#[test]
fn empty_line_returns_invalid_input() {
    assert!(matches!(parse_command(""), ParseResult::InvalidInput(_)));
}

#[test]
fn whitespace_only_returns_invalid_input() {
    assert!(matches!(parse_command("   "), ParseResult::InvalidInput(_)));
}

#[test]
fn unknown_command_returns_error() {
    let msg = as_error(parse_command("FOOBAR"));
    assert!(!msg.is_empty());
}
