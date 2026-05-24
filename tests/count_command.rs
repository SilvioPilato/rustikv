use rustikv::bffp::{Command, decode_input_frame, encode_command};

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
