use std::{
    env::{self},
    net::SocketAddr,
    time::Duration,
};

#[derive(Copy, Clone)]
pub enum FSyncStrategy {
    Always,
    EveryN(usize),
    Periodic(Duration),
    Never,
}

pub enum StorageStrategy {
    SizeTiered,
    Leveled,
}

pub enum BlockCompression {
    None,
    Lz77,
}

#[derive(Copy, Clone)]
pub enum EngineType {
    KV,
    Lsm,
}

pub struct Settings {
    pub db_file_path: String,
    pub tcp_addr: String,
    pub db_name: String,
    pub max_segment_bytes: u64,
    pub sync_strategy: FSyncStrategy,
    pub engine: EngineType,
    pub compaction_ratio: f32,
    pub compaction_max_segment: usize,
    pub storage_strategy: StorageStrategy,
    pub leveled_num_levels: usize,
    pub leveled_l0_threshold: usize,
    pub leveled_l1_max_bytes: u64,
    pub block_size_kb: u64,
    pub block_compression: BlockCompression,
    pub read_timeout_secs: Option<Duration>,
    pub max_connections: usize,
    pub workers: Option<usize>,
}

impl Settings {
    pub fn get_from_args() -> Settings {
        let args: Vec<String> = env::args().collect();

        if args.len() < 2 || args.iter().any(|a| a == "-h" || a == "--help") {
            Self::print_help(&args[0]);
            std::process::exit(0);
        }

        let f_path = args[1].clone();
        let mut args_iter = args.iter().skip(2);
        let mut settings = Settings {
            db_file_path: f_path,
            tcp_addr: "0.0.0.0:6666".to_string(),
            db_name: "segment".to_string(),
            max_segment_bytes: 1_048_576 * 50,
            sync_strategy: FSyncStrategy::Always,
            engine: EngineType::KV,
            compaction_ratio: 0.0,
            compaction_max_segment: 0,
            storage_strategy: StorageStrategy::SizeTiered,
            leveled_num_levels: 4,
            leveled_l0_threshold: 4,
            leveled_l1_max_bytes: 10 * 1024 * 1024,
            block_size_kb: 4,
            block_compression: BlockCompression::Lz77,
            read_timeout_secs: Some(Duration::from_secs(30)),
            max_connections: 1000,
            workers: None,
        };
        while let Some(arg) = args_iter.next() {
            match arg.as_str() {
                "-t" | "--tcp" => {
                    if let Some(value) = args_iter.next() {
                        let addr: SocketAddr = value.parse().expect("Invalid tcp address provided");
                        settings.tcp_addr = addr.to_string();
                    }
                }
                "-n" | "--name" => {
                    if let Some(value) = args_iter.next() {
                        settings.db_name = value.to_string();
                    }
                }
                "-msb" | "--max-segments-bytes" => {
                    if let Some(value) = args_iter.next() {
                        let bytes: u64 =
                            value.parse().expect("Invalid max segments bytes provided");
                        settings.max_segment_bytes = bytes;
                    }
                }
                "-fsync" | "--fsync-interval" => {
                    if let Some(value) = args_iter.next() {
                        settings.sync_strategy = Settings::parse_fsync(value).unwrap();
                    }
                }
                "-e" | "--engine" => {
                    if let Some(value) = args_iter.next() {
                        settings.engine = Settings::parse_engine(value).unwrap();
                    }
                }
                "-cr" | "--compaction-ratio" => {
                    if let Some(value) = args_iter.next() {
                        settings.compaction_ratio =
                            value.parse().expect("Invalid compaction ratio provided");
                    }
                }
                "-cms" | "--compaction-max-segments" => {
                    if let Some(value) = args_iter.next() {
                        settings.compaction_max_segment = value
                            .parse()
                            .expect("Invalid compaction max segments provided");
                    }
                }
                "-ss" | "--storage-strategy" => {
                    if let Some(value) = args_iter.next() {
                        settings.storage_strategy =
                            Settings::parse_storage_strategy(value).unwrap();
                    }
                }
                "-lnl" | "--leveled-num-levels" => {
                    if let Some(value) = args_iter.next() {
                        settings.leveled_num_levels =
                            value.parse().expect("Invalid leveled num levels provided");
                    }
                }
                "-ll0" | "--leveled-l0-threshold" => {
                    if let Some(value) = args_iter.next() {
                        settings.leveled_l0_threshold = value
                            .parse()
                            .expect("Invalid leveled L0 threshold provided");
                    }
                }
                "-ll1" | "--leveled-l1-max-bytes" => {
                    if let Some(value) = args_iter.next() {
                        settings.leveled_l1_max_bytes = value
                            .parse()
                            .expect("Invalid leveled L1 max bytes provided");
                    }
                }
                "-bsk" | "--block-size-kb" => {
                    if let Some(value) = args_iter.next() {
                        let kb: u64 = value.parse().expect("Invalid block size kb provided");
                        if !(1..=1024).contains(&kb) {
                            panic!("Block size must be between 1 and 1024 KB, got: {}", kb);
                        }
                        settings.block_size_kb = kb;
                    }
                }
                "-bc" | "--block-compression" => {
                    if let Some(value) = args_iter.next() {
                        settings.block_compression =
                            Settings::parse_block_compression(value).unwrap()
                    }
                }
                "-rts" | "--read-timeout-secs" => {
                    if let Some(value) = args_iter.next() {
                        let read_timeout_secs: u64 =
                            value.parse().expect("Invalid read timeout secs provided");

                        if read_timeout_secs == 0 {
                            settings.read_timeout_secs = None;
                        } else {
                            settings.read_timeout_secs =
                                Some(Duration::from_secs(read_timeout_secs))
                        }
                    }
                }
                "-mc" | "--max-connections" => {
                    if let Some(value) = args_iter.next() {
                        settings.max_connections =
                            value.parse().expect("Invalid max connections provided");
                    }
                }
                "-w" | "--workers" => {
                    if let Some(value) = args_iter.next() {
                        let workers: usize =
                            value.parse().expect("Invalid workers number provided");
                        if workers == 0 {
                            settings.workers = None;
                        } else {
                            settings.workers = Some(workers);
                        }
                    }
                }
                _ => println!("Unknown argument: {}", arg),
            }
        }

        settings
    }

    fn print_help(prog_name: &str) {
        println!("Usage: {} <db_path> [OPTIONS]", prog_name);
        println!();
        println!("ARGUMENTS:");
        println!("  <db_path>              Path to the database directory");
        println!();
        println!("OPTIONS:");
        println!("  -t, --tcp <ADDR>       TCP bind address (default: 0.0.0.0:6666)");
        println!("  -n, --name <NAME>      Database name prefix (default: segment)");
        println!("  -msb, --max-segments-bytes <BYTES>");
        println!("                         Max bytes per segment (default: 52428800)");
        println!("  -fsync, --fsync-interval <POLICY>");
        println!(
            "                         Fsync strategy: 'always', 'never', 'every:N', 'every:Ns'"
        );
        println!("                         (default: always)");
        println!("  -e, --engine <ENGINE>  Storage engine: 'kv' or 'lsm' (default: kv)");
        println!("  -cr, --compaction-ratio <RATIO>");
        println!(
            "                         Auto-compact when dead/total bytes exceeds ratio (default: 0.0 = disabled)"
        );
        println!("  -ss, --storage-strategy <STRATEGY>");
        println!(
            "                         Storage strategy: 'size-tiered' or 'leveled' (default: size-tiered)"
        );
        println!("  -lnl, --leveled-num-levels <N>");
        println!("                         Number of levels for leveled compaction (default: 4)");
        println!("  -ll0, --leveled-l0-threshold <N>");
        println!("                         L0 file count before compaction (default: 4)");
        println!("  -ll1, --leveled-l1-max-bytes <BYTES>");
        println!("                         L1 max size in bytes (default: 10485760)");
        println!("  -bsk, --block-size-kb <KB>");
        println!(
            "                         Block size in KB for SSTable blocks (default: 4, range: 1-1024)"
        );
        println!("  -bc, --block-compression <COMPRESSION>");
        println!("                         Block compression: 'none' or 'lz77' (default: lz77)");
        println!("  -rts, --read-timeout-secs <SECS>");
        println!(
            "                         Read timeout per connection in seconds (default: 30, 0 = disabled)"
        );
        println!("  -mc, --max-connections <N>");
        println!(
            "                         Maximum concurrent connections (default: 1000, 0 = unlimited)"
        );
        println!("  -w, --workers <N>");
        println!(
            "                         Numbers of workers deployed (default: None, 0 = available parallelism)"
        );
        println!("  -h, --help             Print this help message");
    }
    fn parse_fsync(s: &str) -> Result<FSyncStrategy, String> {
        match s {
            "always" => Ok(FSyncStrategy::Always),
            "never" => Ok(FSyncStrategy::Never),
            _ => {
                let val = s
                    .strip_prefix("every:")
                    .ok_or_else(|| format!("unknown fsync policy: {s}"))?;

                if let Some(secs_str) = val.strip_suffix('s') {
                    let n: u64 = secs_str
                        .parse()
                        .map_err(|_| format!("invalid fsync interval: {val}"))?;
                    if n == 0 {
                        return Err("fsync interval must be > 0".into());
                    }
                    Ok(FSyncStrategy::Periodic(Duration::from_secs(n)))
                } else {
                    let n: usize = val
                        .parse()
                        .map_err(|_| format!("invalid fsync interval: {val}"))?;
                    if n == 0 {
                        return Err("fsync interval must be > 0".into());
                    }
                    Ok(FSyncStrategy::EveryN(n))
                }
            }
        }
    }

    fn parse_engine(s: &str) -> Result<EngineType, String> {
        match s {
            "kv" => Ok(EngineType::KV),
            "lsm" => Ok(EngineType::Lsm),
            _ => Err(format!("Unsupported engine provided: {s}")),
        }
    }

    fn parse_storage_strategy(s: &str) -> Result<StorageStrategy, String> {
        match s {
            "leveled" => Ok(StorageStrategy::Leveled),
            "size-tiered" => Ok(StorageStrategy::SizeTiered),
            _ => Err(format!("Unsupported storage strategy provided: {s}")),
        }
    }

    fn parse_block_compression(s: &str) -> Result<BlockCompression, String> {
        match s {
            "none" => Ok(BlockCompression::None),
            "lz77" => Ok(BlockCompression::Lz77),
            _ => Err(format!("Unsupported block compression provided: {s}")),
        }
    }
}
