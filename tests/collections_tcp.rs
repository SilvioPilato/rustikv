//! End-to-end tests for LSM collections over TCP: USE / CREATE / DROP / SHOW
//! routing, data isolation, per-collection default TTL, and catalog
//! persistence across a server restart.

use rustikv::bffp::{
    Command, DecodedResponse, ResponseStatus, decode_response_frame, encode_command,
};
use std::{
    env, fs,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    process::{Child, Command as ProcCommand},
    thread,
    time::{Duration, Instant, SystemTime},
};

struct ServerProcess {
    child: Child,
    db_path: String,
    addr: String,
}

impl ServerProcess {
    fn start(db_path: &str, engine: &str) -> Self {
        let _ = fs::create_dir_all(db_path);
        let _ = fs::remove_file(format!("{db_path}/server.addr"));

        let child = ProcCommand::new(env!("CARGO_BIN_EXE_rustikv"))
            .arg(db_path)
            .arg("--engine")
            .arg(engine)
            .arg("--tcp")
            .arg("127.0.0.1:0") // ephemeral port; actual addr written to server.addr
            .spawn()
            .expect("failed to start server");

        let addr_file = format!("{db_path}/server.addr");
        let start = Instant::now();
        let addr = loop {
            if Path::new(&addr_file).exists()
                && let Ok(content) = fs::read_to_string(&addr_file)
                && !content.trim().is_empty()
            {
                break content.trim().to_string();
            }
            if start.elapsed() > Duration::from_secs(5) {
                panic!("server did not publish an address in time");
            }
            thread::sleep(Duration::from_millis(25));
        };

        // Wait until the port actually accepts connections.
        let start = Instant::now();
        while TcpStream::connect(&addr).is_err() {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("server did not accept connections in time");
            }
            thread::sleep(Duration::from_millis(25));
        }

        Self {
            child,
            db_path: db_path.to_string(),
            addr,
        }
    }

    fn start_lsm(db_path: &str) -> Self {
        Self::start(db_path, "lsm")
    }

    fn connect(&self) -> TcpStream {
        TcpStream::connect(&self.addr).expect("connect to server")
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_file(format!("{}/server.addr", self.db_path));
    }
}

fn temp_db_path(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_store_colltcp_{suffix}_{nanos}"));
    path.to_string_lossy().to_string()
}

/// Send one command on a persistent connection and decode the response.
/// The connection is reused so per-connection state (the current collection
/// set by USE) survives across calls.
fn send(stream: &mut TcpStream, cmd: Command) -> DecodedResponse {
    let frame = encode_command(cmd);
    stream.write_all(&frame).expect("write frame");

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read length prefix");
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; frame_len];
    stream.read_exact(&mut payload).expect("read payload");

    let mut full = Vec::with_capacity(4 + frame_len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&payload);
    decode_response_frame(&full).expect("decode response")
}

// --- SHOW ---

#[test]
fn show_lists_the_default_collection() {
    let db = temp_db_path("show");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    let resp = send(&mut s, Command::ShowCollections);
    assert!(matches!(resp.status, ResponseStatus::Ok));
    // Default collection is the db name ("segment"), default TTL 0.
    assert_eq!(resp.payload, vec!["segment\t0".to_string()]);
}

// --- CREATE / USE / routing ---

#[test]
fn create_use_write_read_roundtrip() {
    let db = temp_db_path("crud");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    assert!(matches!(
        send(
            &mut s,
            Command::CreateCollection("metrics".into(), Some(3600))
        )
        .status,
        ResponseStatus::Ok
    ));
    assert!(matches!(
        send(&mut s, Command::Use("metrics".into())).status,
        ResponseStatus::Ok
    ));
    assert!(matches!(
        send(&mut s, Command::Write("k1".into(), "hello".into(), None)).status,
        ResponseStatus::Ok
    ));

    let read = send(&mut s, Command::Read("k1".into()));
    assert!(matches!(read.status, ResponseStatus::Ok));
    assert_eq!(read.payload, vec!["hello".to_string()]);

    // SHOW now reports both collections, sorted, with their default TTLs.
    let show = send(&mut s, Command::ShowCollections);
    assert_eq!(
        show.payload,
        vec!["metrics\t3600".to_string(), "segment\t0".to_string()]
    );
}

#[test]
fn collections_isolate_data_across_use() {
    let db = temp_db_path("isolate");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    send(&mut s, Command::CreateCollection("metrics".into(), None));
    send(&mut s, Command::Use("metrics".into()));
    send(
        &mut s,
        Command::Write("k1".into(), "in-metrics".into(), None),
    );

    // Switch back to the default collection — k1 must not be visible there.
    send(&mut s, Command::Use("segment".into()));
    let read = send(&mut s, Command::Read("k1".into()));
    assert!(
        matches!(read.status, ResponseStatus::NotFound),
        "key written in metrics must not leak into the default collection"
    );
}

#[test]
fn use_unknown_collection_errors() {
    let db = temp_db_path("usebad");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    let resp = send(&mut s, Command::Use("ghost".into()));
    assert!(matches!(resp.status, ResponseStatus::Error));
}

// --- DROP ---

#[test]
fn drop_collection_removes_it() {
    let db = temp_db_path("drop");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    send(&mut s, Command::CreateCollection("metrics".into(), None));
    assert!(matches!(
        send(&mut s, Command::DropCollection("metrics".into())).status,
        ResponseStatus::Ok
    ));

    let show = send(&mut s, Command::ShowCollections);
    assert_eq!(show.payload, vec!["segment\t0".to_string()]);

    // USE on the dropped collection now fails.
    assert!(matches!(
        send(&mut s, Command::Use("metrics".into())).status,
        ResponseStatus::Error
    ));
}

#[test]
fn drop_default_collection_errors() {
    let db = temp_db_path("dropdef");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    let resp = send(&mut s, Command::DropCollection("segment".into()));
    assert!(matches!(resp.status, ResponseStatus::Error));
}

// --- KV engine rejects collection commands ---

#[test]
fn collection_commands_error_on_kv_engine() {
    let db = temp_db_path("kv");
    let srv = ServerProcess::start(&db, "kv");
    let mut s = srv.connect();

    assert!(matches!(
        send(&mut s, Command::CreateCollection("metrics".into(), None)).status,
        ResponseStatus::Error
    ));
    assert!(matches!(
        send(&mut s, Command::ShowCollections).status,
        ResponseStatus::Error
    ));
}

// --- per-collection default TTL (the #76 payoff) ---

#[test]
fn write_inherits_collection_default_ttl() {
    let db = temp_db_path("ttlwrite");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    // 1-second default TTL; a WRITE with no explicit TTL inherits it.
    send(
        &mut s,
        Command::CreateCollection("ephemeral".into(), Some(1)),
    );
    send(&mut s, Command::Use("ephemeral".into()));
    send(&mut s, Command::Write("k".into(), "v".into(), None));

    let immediate = send(&mut s, Command::Read("k".into()));
    assert!(matches!(immediate.status, ResponseStatus::Ok));
    assert_eq!(immediate.payload, vec!["v".to_string()]);

    thread::sleep(Duration::from_millis(1200));
    let expired = send(&mut s, Command::Read("k".into()));
    assert!(
        matches!(expired.status, ResponseStatus::NotFound),
        "value should have expired under the collection's default TTL"
    );
}

#[test]
fn incr_inherits_collection_default_ttl() {
    let db = temp_db_path("ttlincr");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    send(
        &mut s,
        Command::CreateCollection("counters".into(), Some(1)),
    );
    send(&mut s, Command::Use("counters".into()));

    let incr = send(&mut s, Command::Incr("hits".into()));
    assert!(matches!(incr.status, ResponseStatus::Ok));
    assert_eq!(incr.payload, vec!["1".to_string()]);

    thread::sleep(Duration::from_millis(1200));
    let expired = send(&mut s, Command::Read("hits".into()));
    assert!(
        matches!(expired.status, ResponseStatus::NotFound),
        "INCR-created key should expire under the collection's default TTL"
    );
}

#[test]
fn explicit_write_ttl_overrides_collection_default() {
    let db = temp_db_path("ttloverride");
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    // Collection default is 1s, but Some(0) means "no expiry" explicitly.
    send(&mut s, Command::CreateCollection("mixed".into(), Some(1)));
    send(&mut s, Command::Use("mixed".into()));
    send(&mut s, Command::Write("keep".into(), "v".into(), Some(0)));

    thread::sleep(Duration::from_millis(1200));
    let still_there = send(&mut s, Command::Read("keep".into()));
    assert!(
        matches!(still_there.status, ResponseStatus::Ok),
        "explicit TTL of 0 must override the collection default and persist"
    );
    assert_eq!(still_there.payload, vec!["v".to_string()]);
}

// --- catalog persistence across restart ---

#[test]
fn collections_persist_across_server_restart() {
    let db = temp_db_path("restart");

    {
        let srv = ServerProcess::start_lsm(&db);
        let mut s = srv.connect();
        send(
            &mut s,
            Command::CreateCollection("metrics".into(), Some(42)),
        );
        send(&mut s, Command::Use("metrics".into()));
        send(
            &mut s,
            Command::Write("k1".into(), "durable".into(), Some(0)),
        );
    } // server killed; catalog + WAL remain on disk

    // Fresh server process over the same db dir.
    let srv = ServerProcess::start_lsm(&db);
    let mut s = srv.connect();

    let show = send(&mut s, Command::ShowCollections);
    assert_eq!(
        show.payload,
        vec!["metrics\t42".to_string(), "segment\t0".to_string()],
        "collection + its default TTL must survive a restart"
    );

    send(&mut s, Command::Use("metrics".into()));
    let read = send(&mut s, Command::Read("k1".into()));
    assert!(matches!(read.status, ResponseStatus::Ok));
    assert_eq!(read.payload, vec!["durable".to_string()]);
}
