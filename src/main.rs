use rustikv::collections::Collections;
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::lsmengine::LsmConfig;
use rustikv::server::{Backend, Server};
use rustikv::settings::{EngineType, Settings};
use rustikv::stats::Stats;
use std::io::{self};
use std::sync::Arc;

fn main() -> io::Result<()> {
    let settings = Settings::get_from_args();

    let backend = Arc::new(match settings.engine {
        EngineType::KV => {
            // KV has no collections — a single engine behind the Backend.
            let kv: Arc<dyn StorageEngine> = match KVEngine::from_dir(
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
            };
            Backend::Kv(kv)
        }
        EngineType::Lsm => {
            // The default collection takes the configured db name, so existing
            // single-engine data keeps loading after the collections upgrade.
            let collections = Collections::load_from_file(
                settings.db_file_path.clone(),
                settings.db_name.clone(),
                LsmConfig::from(&settings),
            )?;
            Backend::Lsm(Arc::new(collections))
        }
    });

    let stats = Arc::new(Stats::new());

    let server = Server::new(backend, settings, stats);
    let handle = server.start()?;
    handle.join().expect("acceptor thread panicked");

    Ok(())
}
