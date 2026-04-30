//! # redis-compare — TCP server benchmark: rustikv vs Redis
//!
//! Compares rustikv's BFFP TCP server against Redis (the industry-standard KV server)
//! on the same sequential WRITE + READ workloads. Both servers must be running.
//!
//! **Setup (before running this benchmark):**
//! 1. Start Redis: `docker run -d -p 6379:6379 redis`
//! 2. Start rustikv: `cargo run --release -- /tmp/bench-db --engine lsm --fsync-interval never`
//! 3. Run this: `cargo run --release --bin redis-compare -- --count 10000 --value-size 100`
//!
//! **Output:** Throughput + latency (min/mean/p99/max) for WRITE and READ phases,
//! followed by a comparison showing rustikv's performance as % of Redis.

use rustikv::bffp::{Command, ResponseStatus, decode_response_frame, encode_command};
use std::{
    io::{self, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

#[derive(Debug)]
struct PhaseResult {
    count: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

#[derive(Debug)]
struct Stats {
    throughput: f64,
    min: Duration,
    mean: Duration,
    p99: Duration,
    max: Duration,
}

fn compute_stats(r: &mut PhaseResult) -> Stats {
    let mut sorted = r.latencies.clone();
    sorted.sort_unstable();

    let count = r.latencies.len();
    let throughput = if r.elapsed.as_secs_f64() > 0.0 {
        count as f64 / r.elapsed.as_secs_f64()
    } else {
        0.0
    };

    let mean = if count > 0 {
        let total: Duration = r.latencies.iter().sum();
        total / count as u32
    } else {
        Duration::ZERO
    };

    let p99_idx = (count as f64 * 0.99) as usize;
    let p99 = sorted
        .get(p99_idx.min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or(Duration::ZERO);

    Stats {
        throughput,
        min: sorted.first().copied().unwrap_or(Duration::ZERO),
        mean,
        p99,
        max: sorted.last().copied().unwrap_or(Duration::ZERO),
    }
}

fn format_duration(d: Duration) -> String {
    format!("{:.3?}", d)
}

fn print_phase(server: &str, phase: &str, r: &mut PhaseResult) {
    let stats = compute_stats(r);
    println!("=== [{}] {} ({} ops) ===", server, phase, r.count);
    println!("  Total:      {}", format_duration(r.elapsed));
    println!("  Throughput: {:.0} ops/sec", stats.throughput);
    println!(
        "  Latency     min={}  mean={}  p99={}  max={}",
        format_duration(stats.min),
        format_duration(stats.mean),
        format_duration(stats.p99),
        format_duration(stats.max),
    );
}

// ============================================================================
// rustikv adapter (BFFP protocol)
// ============================================================================

fn rustikv_send_command(stream: &mut TcpStream, cmd: Command) -> io::Result<bool> {
    let frame = encode_command(cmd);
    stream.write_all(&frame)?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;

    let mut payload = vec![0u8; frame_len];
    stream.read_exact(&mut payload)?;

    let mut full = Vec::with_capacity(4 + frame_len);
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&payload);

    let res = decode_response_frame(&full)?;
    match res.status {
        ResponseStatus::Ok | ResponseStatus::Noop => Ok(true),
        ResponseStatus::NotFound => Ok(false),
        ResponseStatus::Error => Err(io::Error::other(res.payload.join("; "))),
    }
}

fn rustikv_write(stream: &mut TcpStream, key: &str, value: &str) -> io::Result<()> {
    rustikv_send_command(stream, Command::Write(key.to_string(), value.to_string()))?;
    Ok(())
}

fn rustikv_read(stream: &mut TcpStream, key: &str) -> io::Result<bool> {
    rustikv_send_command(stream, Command::Read(key.to_string()))
}

// ============================================================================
// Redis adapter
// ============================================================================

fn redis_write(con: &mut redis::Connection, key: &str, value: &str) -> io::Result<()> {
    redis::cmd("SET").arg(key).arg(value).exec(con).unwrap();
    Ok(())
}

fn redis_read(con: &mut redis::Connection, key: &str) -> io::Result<bool> {
    let result: Option<String> = redis::cmd("GET")
        .arg(key)
        .query(con)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(result.is_some())
}

// ============================================================================
// Benchmark runners
// ============================================================================

fn benchmark_rustikv(host: &str, count: usize, value_size: usize) -> io::Result<()> {
    let mut stream = TcpStream::connect(host)?;
    let value = "x".repeat(value_size);

    // Generate keys
    let keys: Vec<String> = (0..count).map(|i| format!("bench:key:{:08}", i)).collect();

    // WRITE phase
    let write_start = Instant::now();
    let mut write_latencies = Vec::with_capacity(count);
    for key in &keys {
        let t = Instant::now();
        rustikv_write(&mut stream, key, &value)?;
        write_latencies.push(t.elapsed());
    }
    let write_elapsed = write_start.elapsed();

    let mut write_result = PhaseResult {
        count,
        elapsed: write_elapsed,
        latencies: write_latencies,
    };
    print_phase("rustikv", "WRITE", &mut write_result);
    println!();

    // READ phase
    let read_start = Instant::now();
    let mut read_latencies = Vec::with_capacity(count);
    for key in &keys {
        let t = Instant::now();
        rustikv_read(&mut stream, key)?;
        read_latencies.push(t.elapsed());
    }
    let read_elapsed = read_start.elapsed();

    let mut read_result = PhaseResult {
        count,
        elapsed: read_elapsed,
        latencies: read_latencies,
    };
    print_phase("rustikv", "READ", &mut read_result);
    println!();

    Ok(())
}

fn benchmark_redis(host: &str, count: usize, value_size: usize) -> io::Result<()> {
    let client = redis::Client::open(format!("redis://{}", host))
        .map_err(|e| io::Error::other(e.to_string()))?;
    let mut con = client
        .get_connection()
        .map_err(|e| io::Error::other(e.to_string()))?;

    let value = "x".repeat(value_size);

    // Generate keys
    let keys: Vec<String> = (0..count).map(|i| format!("bench:key:{:08}", i)).collect();

    // WRITE phase
    let write_start = Instant::now();
    let mut write_latencies = Vec::with_capacity(count);
    for key in &keys {
        let t = Instant::now();
        redis_write(&mut con, key, &value)?;
        write_latencies.push(t.elapsed());
    }
    let write_elapsed = write_start.elapsed();

    let mut write_result = PhaseResult {
        count,
        elapsed: write_elapsed,
        latencies: write_latencies,
    };
    print_phase("redis", "WRITE", &mut write_result);
    println!();

    // READ phase
    let read_start = Instant::now();
    let mut read_latencies = Vec::with_capacity(count);
    for key in &keys {
        let t = Instant::now();
        redis_read(&mut con, key)?;
        read_latencies.push(t.elapsed());
    }
    let read_elapsed = read_start.elapsed();

    let mut read_result = PhaseResult {
        count,
        elapsed: read_elapsed,
        latencies: read_latencies,
    };
    print_phase("redis", "READ", &mut read_result);
    println!();

    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut rustikv_host = "127.0.0.1:6666".to_string();
    let mut redis_host = "127.0.0.1:6379".to_string();
    let mut count = 10_000;
    let mut value_size = 100;
    let mut engines_str = "rustikv,redis".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rustikv-host" => {
                i += 1;
                rustikv_host = args[i].clone();
            }
            "--redis-host" => {
                i += 1;
                redis_host = args[i].clone();
            }
            "--count" => {
                i += 1;
                count = args[i].parse().unwrap_or(10_000);
            }
            "--value-size" => {
                i += 1;
                value_size = args[i].parse().unwrap_or(100);
            }
            "--engines" => {
                i += 1;
                engines_str = args[i].clone();
            }
            _ => {}
        }
        i += 1;
    }

    let engines_to_run: Vec<&str> = engines_str.split(',').map(|s| s.trim()).collect();

    println!("# redis-compare: TCP server benchmark");
    println!("# count: {}", count);
    println!("# value-size: {}", value_size);
    println!();

    let _results: Vec<(&str, f64, f64)> = Vec::new();

    for engine_name in engines_to_run {
        match engine_name {
            "rustikv" => match benchmark_rustikv(&rustikv_host, count, value_size) {
                Ok(_) => {
                    // stats captured and printed in benchmark_rustikv
                }
                Err(e) => eprintln!("rustikv benchmark failed: {}", e),
            },
            "redis" => match benchmark_redis(&redis_host, count, value_size) {
                Ok(_) => {
                    // stats captured and printed in benchmark_redis
                }
                Err(e) => eprintln!("redis benchmark failed: {}", e),
            },
            _ => eprintln!("Unknown engine: {}", engine_name),
        }
    }

    Ok(())
}
