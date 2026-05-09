use rustikv::bffp::{Command, ResponseStatus, decode_response_frame, encode_command};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

fn temp_db_path(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_graceful_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn start_server(db_path: &str) -> std::process::Child {
    ProcessCommand::new(env!("CARGO_BIN_EXE_rustikv"))
        .arg(db_path)
        .arg("--tcp")
        .arg("0.0.0.0:0")
        .spawn()
        .expect("failed to start server")
}

fn read_server_addr(db_path: &str) -> String {
    let addr_file = format!("{}/server.addr", db_path);
    let start = Instant::now();
    loop {
        if Path::new(&addr_file).exists()
            && let Ok(content) = fs::read_to_string(&addr_file)
        {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Server did not provide address within timeout");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_server(addr: &str) {
    let start = Instant::now();
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Server did not start within timeout");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn ping_round_trip(addr: &str) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let frame = encode_command(Command::Ping);
    stream.write_all(&frame).expect("write ping");
    stream.shutdown(Shutdown::Write).expect("shutdown write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read");
    let response = decode_response_frame(&buf).expect("decode ping");
    assert!(matches!(response.status, ResponseStatus::Ok));
    assert_eq!(response.payload, vec!["PONG"]);
}

#[test]
fn partial_length_prefix_then_close_does_not_break_server() {
    // Send only 2 bytes of the 4-byte length prefix, then close the TCP write half.
    // The worker is in ParseState::AwaitingLength with 2 bytes accumulated. The
    // socket EOF should trigger ConnectionAction::Close cleanly.
    let db_path = temp_db_path("half_prefix");
    let mut child = start_server(&db_path);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    {
        let mut stream = TcpStream::connect(&addr).expect("connect");
        stream.write_all(&[0xAB, 0xCD]).expect("write 2 bytes");
        // Drop the stream: client sends FIN, server sees Ok(0) on next read
    }

    // Server should still be healthy
    thread::sleep(Duration::from_millis(100));
    ping_round_trip(&addr);

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}

#[test]
fn partial_body_then_close_does_not_break_server() {
    // Send the 4-byte length prefix claiming a 100-byte body, then 10 bytes of body,
    // then close. The worker is in ParseState::AwaitingBody(100) with 10 bytes
    // accumulated.
    let db_path = temp_db_path("half_body");
    let mut child = start_server(&db_path);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    {
        let mut stream = TcpStream::connect(&addr).expect("connect");
        let mut frame = Vec::new();
        frame.extend_from_slice(&100u32.to_be_bytes()); // claims 100-byte body
        frame.extend(std::iter::repeat_n(0xEEu8, 10)); // only 10 bytes provided
        stream.write_all(&frame).expect("write partial frame");
        // Drop: server sees EOF mid-frame
    }

    thread::sleep(Duration::from_millis(100));
    ping_round_trip(&addr);

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}

#[test]
fn complete_frame_response_arrives_before_close() {
    // Verifies the graceful close path: client sends one complete Ping, then closes
    // its write half. Server should still send the PONG before closing the connection.
    let db_path = temp_db_path("complete_then_close");
    let mut child = start_server(&db_path);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    let frame = encode_command(Command::Ping);
    stream.write_all(&frame).expect("write");
    stream.shutdown(Shutdown::Write).expect("shutdown write");
    // Now read until server closes — we should get the PONG response back even
    // though we already half-closed.
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read to EOF");
    let response = decode_response_frame(&buf).expect("decode");
    assert!(matches!(response.status, ResponseStatus::Ok));
    assert_eq!(response.payload, vec!["PONG"]);

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}
