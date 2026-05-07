use rustikv::bffp::{Command, encode_command};
use rustikv::server::{ConnectionAction, FrameParser};

#[test]
fn feed_complete_ping_yields_one_command_continue() {
    let bytes = encode_command(Command::Ping);
    let mut parser = FrameParser::new();

    let (cmds, action) = parser.feed(&bytes).expect("feed");

    assert_eq!(cmds.len(), 1);
    assert!(matches!(cmds[0], Command::Ping));
    assert!(matches!(action, ConnectionAction::Continue));
}

#[test]
fn feed_complete_write_yields_command_with_payload() {
    let bytes = encode_command(Command::Write("k".to_string(), "v".to_string()));
    let mut parser = FrameParser::new();

    let (cmds, action) = parser.feed(&bytes).expect("feed");

    assert_eq!(cmds.len(), 1);
    match &cmds[0] {
        Command::Write(k, v) => {
            assert_eq!(k, "k");
            assert_eq!(v, "v");
        }
        other => panic!(
            "expected Command::Write, got {:?}",
            std::mem::discriminant(other)
        ),
    }
    assert!(matches!(action, ConnectionAction::Continue));
}

#[test]
fn feed_split_into_three_chunks_yields_command_after_last_chunk() {
    let bytes = encode_command(Command::Read("hello".to_string()));
    let mut parser = FrameParser::new();

    let split_a = 2;
    let split_b = bytes.len() - 1;

    let (cmds, _) = parser.feed(&bytes[..split_a]).expect("feed 1");
    assert!(
        cmds.is_empty(),
        "shouldn't yield with only {} bytes",
        split_a
    );

    let (cmds, _) = parser.feed(&bytes[split_a..split_b]).expect("feed 2");
    assert!(cmds.is_empty(), "shouldn't yield while still mid-frame");

    let (cmds, action) = parser.feed(&bytes[split_b..]).expect("feed 3");
    assert_eq!(cmds.len(), 1);
    assert!(matches!(action, ConnectionAction::Continue));
}

#[test]
fn feed_two_frames_in_one_chunk_yields_both() {
    let mut bytes = encode_command(Command::Ping);
    bytes.extend(encode_command(Command::List));
    let mut parser = FrameParser::new();

    let (cmds, action) = parser.feed(&bytes).expect("feed");

    assert_eq!(cmds.len(), 2);
    assert!(matches!(cmds[0], Command::Ping));
    assert!(matches!(cmds[1], Command::List));
    assert!(matches!(action, ConnectionAction::Continue));
}

#[test]
fn feed_two_frames_across_two_calls_decodes_both() {
    let mut parser = FrameParser::new();

    let first = encode_command(Command::Ping);
    let (cmds, _) = parser.feed(&first).expect("feed 1");
    assert_eq!(cmds.len(), 1);
    assert!(matches!(cmds[0], Command::Ping));

    let second = encode_command(Command::Stats);
    let (cmds, _) = parser.feed(&second).expect("feed 2");
    assert_eq!(cmds.len(), 1);
    assert!(matches!(cmds[0], Command::Stats));
}

#[test]
fn feed_byte_at_a_time_eventually_yields_command() {
    let bytes = encode_command(Command::Ping);
    let mut parser = FrameParser::new();

    let mut total_yielded = 0;
    for (i, byte) in bytes.iter().enumerate() {
        let (cmds, _) = parser.feed(&[*byte]).expect("feed byte");
        total_yielded += cmds.len();
        if i < bytes.len() - 1 {
            assert!(cmds.is_empty(), "yielded too early at byte {}", i);
        }
    }

    assert_eq!(total_yielded, 1, "exactly one command should be emitted");
}

#[test]
fn feed_oversized_length_prefix_signals_close() {
    // Length prefix claiming u32::MAX bytes — well above MAX_REQUEST_BYTES.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    let mut parser = FrameParser::new();

    let (cmds, action) = parser.feed(&bytes).expect("feed");

    assert!(cmds.is_empty());
    assert!(matches!(action, ConnectionAction::Close));
}

#[test]
fn feed_empty_chunk_is_a_noop() {
    let mut parser = FrameParser::new();

    let (cmds, action) = parser.feed(&[]).expect("feed empty");

    assert!(cmds.is_empty());
    assert!(matches!(action, ConnectionAction::Continue));
}

#[test]
fn feed_partial_then_complete_frame_decodes_correctly() {
    // Verifies state survives across calls: AwaitingLength -> AwaitingBody -> AwaitingLength
    let bytes = encode_command(Command::Write("k".to_string(), "v".to_string()));
    let mut parser = FrameParser::new();

    // Feed just the 4-byte length prefix
    let (cmds, _) = parser.feed(&bytes[..4]).expect("feed prefix");
    assert!(cmds.is_empty(), "length alone shouldn't decode");

    // Feed the rest — body should now decode
    let (cmds, action) = parser.feed(&bytes[4..]).expect("feed body");
    assert_eq!(cmds.len(), 1);
    assert!(matches!(action, ConnectionAction::Continue));
}
