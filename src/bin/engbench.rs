//! # engbench — In-process engine benchmark
//!
//! Directly benchmarks rustikv engines (LsmEngine, KVEngine) against reference
//! databases (RocksDB, sled, redb) with no TCP overhead.
//!
//! Runs sequential WRITE then READ phases, collecting per-op timings and
//! reporting throughput + latency (min/mean/p99/max).

use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::settings::FSyncStrategy;
use rustikv::size_tiered::SizeTiered;
use std::{
    fs, io,
    time::{Duration, Instant},
};

trait BenchEngine: Sync + Send {
    fn name(&self) -> &str;
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn read(&self, key: &[u8]) -> io::Result<bool>;
}

struct LsmEngineAdapter {
    engine: LsmEngine,
}

impl BenchEngine for LsmEngineAdapter {
    fn name(&self) -> &str {
        "rustikv-lsm"
    }
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let k = String::from_utf8_lossy(key).into_owned();
        let v = String::from_utf8_lossy(value).into_owned();
        self.engine.set(&k, &v)
    }
    fn read(&self, key: &[u8]) -> io::Result<bool> {
        let k = String::from_utf8_lossy(key).into_owned();
        Ok(self.engine.get(&k)?.is_some())
    }
}

struct KvEngineAdapter {
    engine: KVEngine,
}

impl BenchEngine for KvEngineAdapter {
    fn name(&self) -> &str {
        "rustikv-kv"
    }
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let k = String::from_utf8_lossy(key).into_owned();
        let v = String::from_utf8_lossy(value).into_owned();
        self.engine.set(&k, &v)
    }
    fn read(&self, key: &[u8]) -> io::Result<bool> {
        let k = String::from_utf8_lossy(key).into_owned();
        Ok(self.engine.get(&k)?.is_some())
    }
}

struct SledAdapter {
    db: sled::Db,
}

impl BenchEngine for SledAdapter {
    fn name(&self) -> &str {
        "sled"
    }
    fn write(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.db
            .insert(key, value)
            .map_err(|e| io::Error::other(format!("{:?}", e)))?;
        Ok(())
    }
    fn read(&self, key: &[u8]) -> io::Result<bool> {
        self.db
            .get(key)
            .map(|opt| opt.is_some())
            .map_err(|e| io::Error::other(format!("{:?}", e)))
    }
}

#[derive(Debug)]
struct LatencyStats {
    throughput: u64,
    min: Duration,
    mean: Duration,
    p99: Duration,
    max: Duration,
}

impl LatencyStats {
    fn from_durations(durations: &[Duration], wall_time: Duration) -> Self {
        let mut sorted = durations.to_vec();
        sorted.sort_unstable();

        let count = durations.len() as u64;
        let throughput = if wall_time.as_secs_f64() > 0.0 {
            (count as f64 / wall_time.as_secs_f64()) as u64
        } else {
            0
        };

        let mean = if count > 0 {
            let total: Duration = durations.iter().sum();
            Duration::from_nanos(total.as_nanos() as u64 / count)
        } else {
            Duration::ZERO
        };

        let p99_idx = (count as f64 * 0.99) as usize;
        let p99 = sorted
            .get(p99_idx.min(sorted.len().saturating_sub(1)))
            .copied()
            .unwrap_or(Duration::ZERO);

        Self {
            throughput,
            min: sorted.first().copied().unwrap_or(Duration::ZERO),
            mean,
            p99,
            max: sorted.last().copied().unwrap_or(Duration::ZERO),
        }
    }
}

fn format_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{}ms", d.as_millis())
    } else if d.as_micros() > 0 {
        format!("{}µs", d.as_micros())
    } else {
        format!("{}ns", d.as_nanos())
    }
}

fn run_phase(
    engine: &dyn BenchEngine,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
    is_write: bool,
) -> io::Result<LatencyStats> {
    let mut durations = Vec::with_capacity(keys.len());

    let start = Instant::now();
    for (i, key) in keys.iter().enumerate() {
        let op_start = Instant::now();
        if is_write {
            engine.write(key, &values[i % values.len()])?;
        } else {
            engine.read(key)?;
        }
        durations.push(op_start.elapsed());
    }
    let wall_time = start.elapsed();

    Ok(LatencyStats::from_durations(&durations, wall_time))
}

fn create_lsm_engine(db_path: &str) -> io::Result<LsmEngineAdapter> {
    let _ = fs::remove_dir_all(db_path);
    fs::create_dir_all(db_path)?;

    let strategy = SizeTiered::new(4, 32, 4096, false);
    let engine = LsmEngine::new(
        db_path,
        "bench",
        10_000_000,
        Box::new(strategy),
        4096,
        false,
    )?;
    Ok(LsmEngineAdapter { engine })
}

fn create_kv_engine(db_path: &str) -> io::Result<KvEngineAdapter> {
    let _ = fs::remove_dir_all(db_path);
    fs::create_dir_all(db_path)?;

    let engine = KVEngine::new(db_path, "bench", 52_428_800, FSyncStrategy::Never)?;
    Ok(KvEngineAdapter { engine })
}

fn create_sled(db_path: &str) -> io::Result<SledAdapter> {
    let _ = fs::remove_dir_all(db_path);
    fs::create_dir_all(db_path)?;

    let db = sled::open(db_path).map_err(|e| io::Error::other(format!("{:?}", e)))?;
    Ok(SledAdapter { db })
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut count = 10_000;
    let mut value_size = 100;
    let mut engines_str = "lsm,kv,sled".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
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

    let keys: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("bench:key:{:08}", i).into_bytes())
        .collect();

    let value = vec![b'x'; value_size];
    let values = vec![value];

    println!("# engbench: in-process engine benchmark");
    println!("# count: {}", count);
    println!("# value-size: {}", value_size);
    println!();

    let temp_dir = std::env::temp_dir().join("engbench");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir)?;

    for engine_name in engines_to_run {
        let db_path = temp_dir.join(engine_name);
        let db_path_str = db_path.to_string_lossy();

        match engine_name {
            "lsm" => match create_lsm_engine(&db_path_str) {
                Ok(engine) => benchmark_engine(&engine, engine_name, &keys, &values)?,
                Err(e) => eprintln!("Failed to create LSM engine: {}", e),
            },
            "kv" => match create_kv_engine(&db_path_str) {
                Ok(engine) => benchmark_engine(&engine, engine_name, &keys, &values)?,
                Err(e) => eprintln!("Failed to create KV engine: {}", e),
            },
            "sled" => match create_sled(&db_path_str) {
                Ok(engine) => benchmark_engine(&engine, engine_name, &keys, &values)?,
                Err(e) => eprintln!("Failed to create sled: {}", e),
            },
            _ => eprintln!("Unknown engine: {}", engine_name),
        }

        let _ = fs::remove_dir_all(&db_path);
        println!();
    }

    let _ = fs::remove_dir_all(&temp_dir);
    Ok(())
}

fn benchmark_engine(
    engine: &dyn BenchEngine,
    _name: &str,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
) -> io::Result<()> {
    println!("# engine: {}", engine.name());
    println!();

    let write_stats = run_phase(engine, keys, values, true)?;
    println!("=== WRITE ({} ops) ===", keys.len());
    println!("  Throughput: {} ops/sec", write_stats.throughput);
    println!(
        "  Latency     min={}  mean={}  p99={}  max={}",
        format_duration(write_stats.min),
        format_duration(write_stats.mean),
        format_duration(write_stats.p99),
        format_duration(write_stats.max),
    );
    println!();

    let read_stats = run_phase(engine, keys, values, false)?;
    println!("=== READ ({} ops) ===", keys.len());
    println!("  Throughput: {} ops/sec", read_stats.throughput);
    println!(
        "  Latency     min={}  mean={}  p99={}  max={}",
        format_duration(read_stats.min),
        format_duration(read_stats.mean),
        format_duration(read_stats.p99),
        format_duration(read_stats.max),
    );

    Ok(())
}
