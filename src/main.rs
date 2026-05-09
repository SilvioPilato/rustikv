use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::leveled::Leveled;
use rustikv::lsmengine::LsmEngine;
use rustikv::server::Server;
use rustikv::settings::{BlockCompression, EngineType, Settings, StorageStrategy};
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::io::{self};
use std::sync::Arc;
fn main() -> io::Result<()> {
    let settings = Settings::get_from_args();
    let database: Arc<dyn StorageEngine> = match settings.engine {
        EngineType::KV => match KVEngine::from_dir(
            &settings.db_file_path,
            &settings.db_name,
            settings.max_segment_bytes,
            settings.sync_strategy,
        )? {
            Some(db) => Arc::new(db),
            None => Arc::new(KVEngine::new(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes,
                settings.sync_strategy,
            )?),
        },
        EngineType::Lsm => {
            let block_size_bytes = (settings.block_size_kb * 1024) as usize;
            let block_compression_enabled =
                matches!(settings.block_compression, BlockCompression::Lz77);
            let strategy: Box<dyn rustikv::storage_strategy::StorageStrategy> =
                match settings.storage_strategy {
                    StorageStrategy::SizeTiered => Box::new(SizeTiered::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        4,  // min_threshold
                        32, // max_threshold
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                    StorageStrategy::Leveled => Box::new(Leveled::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        settings.leveled_num_levels,
                        settings.leveled_l0_threshold,
                        settings.leveled_l1_max_bytes,
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                };

            Arc::new(LsmEngine::from_dir(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes as usize,
                strategy,
                block_size_bytes,
                block_compression_enabled,
            )?)
        }
    };
    let stats = Arc::new(Stats::new());

    let server = Server::new(database, settings, stats);
    let handle = server.start()?;
    handle.join().expect("acceptor thread panicked");

    Ok(())
}
