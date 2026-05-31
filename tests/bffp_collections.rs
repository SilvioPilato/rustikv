//! Wire round-trip tests for the collection op codes (25–28): a command
//! encoded by the client must decode back to the same command on the server.

use rustikv::bffp::{Command, decode_input_frame, encode_command};

fn roundtrip(cmd: Command) -> Command {
    let frame = encode_command(cmd);
    decode_input_frame(&frame).expect("decode")
}

#[test]
fn use_roundtrips() {
    match roundtrip(Command::Use("metrics".into())) {
        Command::Use(name) => assert_eq!(name, "metrics"),
        other => panic!(
            "expected Use, got something else: {:?}",
            discriminant(&other)
        ),
    }
}

#[test]
fn create_collection_without_ttl_roundtrips() {
    match roundtrip(Command::CreateCollection("metrics".into(), None)) {
        Command::CreateCollection(name, ttl) => {
            assert_eq!(name, "metrics");
            assert_eq!(ttl, None);
        }
        other => panic!("expected CreateCollection, got {:?}", discriminant(&other)),
    }
}

#[test]
fn create_collection_with_ttl_roundtrips() {
    match roundtrip(Command::CreateCollection("metrics".into(), Some(86400))) {
        Command::CreateCollection(name, ttl) => {
            assert_eq!(name, "metrics");
            assert_eq!(ttl, Some(86400));
        }
        other => panic!("expected CreateCollection, got {:?}", discriminant(&other)),
    }
}

#[test]
fn drop_collection_roundtrips() {
    match roundtrip(Command::DropCollection("metrics".into())) {
        Command::DropCollection(name) => assert_eq!(name, "metrics"),
        other => panic!("expected DropCollection, got {:?}", discriminant(&other)),
    }
}

#[test]
fn show_collections_roundtrips() {
    assert!(matches!(
        roundtrip(Command::ShowCollections),
        Command::ShowCollections
    ));
}

// `Command` doesn't derive Debug; this gives panics something printable.
fn discriminant(cmd: &Command) -> std::mem::Discriminant<Command> {
    std::mem::discriminant(cmd)
}
