use std::env;
use std::fs;
use std::io::Read;
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
    path.push(format!("kv_idle_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn start_server_with_idle_timeout(db_path: &str, timeout_secs: u64) -> std::process::Child {
    ProcessCommand::new(env!("CARGO_BIN_EXE_rustikv"))
        .arg(db_path)
        .arg("--tcp")
        .arg("0.0.0.0:0")
        .arg("--read-timeout-secs")
        .arg(timeout_secs.to_string())
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

#[test]
fn idle_connection_is_closed_by_server() {
    let db_path = temp_db_path("closed");
    let mut child = start_server_with_idle_timeout(&db_path, 1);
    let addr = read_server_addr(&db_path);
    wait_for_server(&addr);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    // Don't send anything. The 100ms sweep should pick this up after ~1.0-1.1s
    // and close it. Give a generous window so we don't flake under CI load.
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");

    let started = Instant::now();
    let mut buf = [0u8; 16];
    let result = stream.read(&mut buf);
    let elapsed = started.elapsed();

    match result {
        Ok(0) => {
            // EOF — server closed us. Verify it actually waited at least the timeout
            // (if the server closed instantly, the test wouldn't be exercising the sweep).
            assert!(
                elapsed >= Duration::from_millis(800),
                "server closed too quickly: {:?}",
                elapsed
            );
        }
        Ok(n) => panic!("expected EOF, got {} bytes", n),
        Err(e) => panic!("read failed: {}", e),
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = fs::remove_file(format!("{}/server.addr", db_path));
}
