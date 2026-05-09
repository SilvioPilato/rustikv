use rustikv::bffp::{Command, ResponseStatus, decode_response_frame, encode_command};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

fn temp_db_path(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_multi_worker_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn start_server(db_path: &str, workers: usize) -> std::process::Child {
    ProcessCommand::new(env!("CARGO_BIN_EXE_rustikv"))
        .arg(db_path)
        .arg("--tcp")
        .arg("0.0.0.0:0")
        .arg("--workers")
        .arg(workers.to_string())
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

fn send_one_command(addr: &str, cmd: Command) -> Vec<u8> {
    let frame = encode_command(cmd);
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(&frame).expect("write");
    stream.shutdown(Shutdown::Write).expect("shutdown");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read");
    buf
}

#[test]
fn many_concurrent_writes_distributed_across_workers() {
    let db_path = temp_db_path("many_writes");
    let mut child = start_server(&db_path, 4);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    let n_clients: usize = 32;
    let barrier = Arc::new(Barrier::new(n_clients));

    let handles: Vec<_> = (0..n_clients)
        .map(|client_id| {
            let addr = addr.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                let key = format!("k-{}", client_id);
                let value = format!("v-{}", client_id);
                let response_bytes = send_one_command(&addr, Command::Write(key, value));
                let response = decode_response_frame(&response_bytes).expect("decode write");
                assert!(
                    matches!(response.status, ResponseStatus::Ok),
                    "client {client_id} write status was not Ok"
                );
            })
        })
        .collect();

    for h in handles {
        h.join().expect("client thread panicked");
    }

    // Verify all keys made it through, regardless of which worker handled them
    let response_bytes = send_one_command(&addr, Command::List);
    let response = decode_response_frame(&response_bytes).expect("decode list");
    assert!(matches!(response.status, ResponseStatus::Ok));
    assert_eq!(
        response.payload.len(),
        n_clients,
        "expected {} keys after concurrent writes, got {}",
        n_clients,
        response.payload.len()
    );

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}

#[test]
fn pipelined_commands_on_one_connection() {
    // Single connection sends multiple frames before reading any response.
    // Verifies the worker's parser yields multiple Commands per readable event
    // and that responses come back in order.
    let db_path = temp_db_path("pipeline");
    let mut child = start_server(&db_path, 2);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    // Issue 5 Pings back-to-back without reading
    let mut batch = Vec::new();
    for _ in 0..5 {
        batch.extend(encode_command(Command::Ping));
    }
    stream.write_all(&batch).expect("write batch");
    stream
        .shutdown(Shutdown::Write)
        .expect("shutdown write half");

    // Read all responses
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).expect("read responses");

    // Decode each response in order. The buffer has 5 length-prefixed frames concatenated.
    let mut offset = 0;
    let mut decoded_count = 0;
    while offset < buf.len() {
        // length prefix tells us how big this frame is
        let len = u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        let total = 4 + len;
        let response =
            decode_response_frame(&buf[offset..offset + total]).expect("decode response");
        assert!(matches!(response.status, ResponseStatus::Ok));
        assert_eq!(response.payload, vec!["PONG"]);
        offset += total;
        decoded_count += 1;
    }
    assert_eq!(decoded_count, 5, "expected 5 PONG responses");

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}
