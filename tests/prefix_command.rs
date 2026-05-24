use rustikv::bffp::{Command, decode_input_frame, encode_command};

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
