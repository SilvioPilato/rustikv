use rustikv::bffp::{Command, decode_response_frame, encode_command};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
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
    path.push(format!("kv_maxconn_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn start_server(db_path: &str, max_connections: usize) -> std::process::Child {
    ProcessCommand::new(env!("CARGO_BIN_EXE_rustikv"))
        .arg(db_path)
        .arg("--tcp")
        .arg("0.0.0.0:0")
        .arg("--max-connections")
        .arg(max_connections.to_string())
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

/// Open a connection and complete one Ping round-trip so the worker has registered
/// the connection (active_connections is incremented in Connection::new on the worker side).
fn open_and_handshake(addr: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let frame = encode_command(Command::Ping);
    stream.write_all(&frame).expect("write ping");

    // Read exactly one response frame
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read len");
    let total = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; total];
    stream.read_exact(&mut payload).expect("read payload");

    let mut full = Vec::with_capacity(4 + total);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&payload);
    let response = decode_response_frame(&full).expect("decode ping");
    assert!(
        matches!(response.status, rustikv::bffp::ResponseStatus::Ok),
        "handshake ping was not Ok"
    );
    stream
}

#[test]
fn connections_beyond_cap_are_rejected() {
    let db_path = temp_db_path("over_cap");
    let cap: usize = 2;
    let mut child = start_server(&db_path, cap);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    // Hold `cap` connections, each fully registered with a worker
    let _holders: Vec<TcpStream> = (0..cap).map(|_| open_and_handshake(&addr)).collect();

    // The next connection should be accepted at TCP level (kernel-side accept)
    // but immediately closed by the acceptor without handing it to a worker
    let mut over_cap = TcpStream::connect(&addr).expect("connect over cap");
    over_cap
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set timeout");

    let mut buf = [0u8; 16];
    let result = over_cap.read(&mut buf);
    // Expected outcome: server-side close, so read returns Ok(0) (EOF)
    // or an error if the kernel sent RST.
    match result {
        Ok(0) => {} // EOF — server dropped us as expected
        Ok(n) => panic!(
            "over-cap connection should have been closed, got {} bytes",
            n
        ),
        Err(_) => {} // RST or read error is also acceptable rejection signal
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}

#[test]
fn cap_releases_after_holders_disconnect() {
    let db_path = temp_db_path("releases");
    let cap: usize = 1;
    let mut child = start_server(&db_path, cap);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    // Hold one connection (at the cap)
    let holder = open_and_handshake(&addr);

    // Drop it — Connection::Drop on the worker decrements active_connections
    drop(holder);

    // Give the worker a moment to process the FIN and drop its Connection
    thread::sleep(Duration::from_millis(200));

    // A fresh connection should now succeed end-to-end
    let _post_drop = open_and_handshake(&addr);

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}
