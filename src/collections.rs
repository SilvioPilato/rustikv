use crate::settings::StorageStrategyKind;
use crate::{
    leveled::Leveled,
    lsmengine::{LsmConfig, LsmEngine},
    size_tiered::SizeTiered,
    storage_strategy::StorageStrategy,
};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, read_to_string},
    io::{self},
    path::Path,
    sync::{Arc, RwLock},
};

const CATALOG_FILE: &str = "collections.catalog";
const CATALOG_TMP: &str = "collections.catalog.tmp";

pub struct Collections {
    // Lock order: always acquire `engines` before `metadata` to avoid deadlock.
    engines: RwLock<HashMap<String, Arc<LsmEngine>>>,
    metadata: RwLock<HashMap<String, CollectionMeta>>, // live registry
    db_path: String,
    default_name: String, // the un-droppable default
    template: LsmConfig,  // engine settings stamped onto newly-created collections
}

#[derive(Clone)]
pub struct CollectionMeta {
    pub name: String,
    pub default_ttl_secs: u32,
    pub max_memtable_bytes: usize,
    pub block_size_bytes: usize,
    pub block_compression_enabled: bool,
    pub storage_strategy: StorageStrategyKind,
    pub leveled_num_levels: usize,
    pub leveled_l0_threshold: usize,
    pub leveled_l1_max_bytes: u64,
}

impl Collections {
    /// Build the live registry from the on-disk catalog.
    ///
    /// `catalog_path` is the catalog *file*; `db_path` is the *directory* that
    /// holds every collection's WAL/SSTable files. A missing catalog file is
    /// treated as a fresh database (empty catalog) rather than an error.
    ///
    /// `default_config` supplies the engine settings for the un-droppable
    /// default collection when it is not yet present in the catalog (fresh
    /// start / migration from single-engine mode).
    pub fn load_from_file(
        db_path: String,
        default_name: String,
        template: LsmConfig,
    ) -> io::Result<Collections> {
        // The default collection name comes from `--name` and is stamped into
        // the catalog like any other collection, so it must satisfy the same
        // charset rule. Reject it up front: bootstrap below would otherwise
        // persist a catalog that `parse_catalog` rejects on the next start
        // (and an out-of-charset name like `a_b` could let a sibling drop
        // collide with this collection's files). Fail fast with a clear error.
        if !Self::is_valid_collection_name(&default_name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "invalid default collection name '{default_name}': \
                     must be non-empty and match [A-Za-z0-9-]+ (set via --name)"
                ),
            ));
        }

        // A missing catalog file means a brand-new database: start empty.
        let catalog_path = Path::new(&db_path).join(CATALOG_FILE);
        let mut metadata = HashMap::new();
        let mut catalog = match Collections::parse_catalog(&catalog_path) {
            Ok(metas) => metas,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e),
        };

        // Bootstrap / migrate: the default collection must always exist.
        if !catalog.iter().any(|m| m.name == default_name) {
            catalog.push(Collections::meta_from_template(&template, &default_name, 0));
            Collections::persist_catalog(&db_path, &catalog)?;
        }

        let mut engines: HashMap<String, Arc<LsmEngine>> = HashMap::new();
        for meta in catalog.into_iter() {
            let engine = Self::build_engine(&db_path, &meta)?;
            engines.insert(meta.name.clone(), engine);
            metadata.insert(meta.name.clone(), meta);
        }

        Ok(Collections {
            engines: RwLock::new(engines),
            metadata: RwLock::new(metadata),
            db_path,
            default_name,
            template,
        })
    }

    /// Stamp a `CollectionMeta` for `name` from the server's engine template,
    /// with the given default TTL. The single place that maps server config →
    /// per-collection config, used by both bootstrap and runtime `create`.
    fn meta_from_template(
        template: &LsmConfig,
        name: &str,
        default_ttl_secs: u32,
    ) -> CollectionMeta {
        CollectionMeta {
            name: name.to_string(),
            default_ttl_secs,
            max_memtable_bytes: template.max_memtable_bytes,
            block_size_bytes: template.block_size_bytes,
            block_compression_enabled: template.block_compression_enabled,
            storage_strategy: template.storage_strategy,
            leveled_num_levels: template.leveled_num_levels,
            leveled_l0_threshold: template.leveled_l0_threshold,
            leveled_l1_max_bytes: template.leveled_l1_max_bytes,
        }
    }

    /// Construct (and recover) a single collection's engine from disk.
    ///
    /// Uses the `*_from_dir` constructors so existing SSTables are loaded and
    /// the WAL is replayed — `new` would start fresh and drop unflushed data.
    fn build_engine(db_path: &str, meta: &CollectionMeta) -> io::Result<Arc<LsmEngine>> {
        let strategy: Box<dyn StorageStrategy> = match meta.storage_strategy {
            StorageStrategyKind::SizeTiered => Box::new(SizeTiered::load_from_dir(
                db_path,
                &meta.name,
                4,  // min_threshold
                32, // max_threshold
                meta.block_size_bytes,
                meta.block_compression_enabled,
            )?),
            StorageStrategyKind::Leveled => Box::new(Leveled::load_from_dir(
                db_path,
                &meta.name,
                meta.leveled_num_levels,
                meta.leveled_l0_threshold,
                meta.leveled_l1_max_bytes,
                meta.block_size_bytes,
                meta.block_compression_enabled,
            )?),
        };

        let engine = LsmEngine::from_dir(
            db_path,
            &meta.name,
            meta.max_memtable_bytes,
            strategy,
            meta.block_size_bytes,
            meta.block_compression_enabled,
        )?;
        Ok(Arc::new(engine))
    }

    fn parse_catalog(path: &Path) -> io::Result<Vec<CollectionMeta>> {
        let contents = read_to_string(path)?;
        let mut metadata: Vec<CollectionMeta> = Vec::new();
        let mut seen: HashSet<&str> = HashSet::new();

        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            } // spec: tolerate blank lines

            // format: name\tdefault_ttl_secs\tmax_memtable_bytes\tblock_size_bytes\t
            //         block_compression_enabled\tstorage_strategy\t
            //         leveled_num_levels\tleveled_l0_threshold\tleveled_l1_max_bytes
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != 9 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad catalog line: expected 9 fields",
                ));
            }

            let name = fields[0];
            if !Self::is_valid_collection_name(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid collection name",
                ));
            }

            if !seen.insert(name) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("duplicate collection name in catalog: {name}"),
                ));
            }

            let default_ttl_secs: u32 = fields[1].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad catalog default_ttl_secs")
            })?;

            let max_memtable_bytes: usize = fields[2].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad catalog max_memtable_bytes")
            })?;

            let block_size_bytes: usize = fields[3].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad catalog block_size_bytes")
            })?;

            let block_compression_enabled: bool = fields[4].parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad catalog block_compression_enabled",
                )
            })?;

            let storage_strategy = match fields[5] {
                "size-tiered" => StorageStrategyKind::SizeTiered,
                "leveled" => StorageStrategyKind::Leveled,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bad catalog storage_strategy",
                    ));
                }
            };

            let leveled_num_levels: usize = fields[6].parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "bad catalog leveled_num_levels")
            })?;

            let leveled_l0_threshold: usize = fields[7].parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad catalog leveled_l0_threshold",
                )
            })?;

            let leveled_l1_max_bytes: u64 = fields[8].parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad catalog leveled_l1_max_bytes",
                )
            })?;

            metadata.push(CollectionMeta {
                name: name.to_string(),
                default_ttl_secs,
                max_memtable_bytes,
                block_size_bytes,
                block_compression_enabled,
                storage_strategy,
                leveled_num_levels,
                leveled_l0_threshold,
                leveled_l1_max_bytes,
            });
        }
        Ok(metadata)
    }

    fn is_valid_collection_name(name: &str) -> bool {
        !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    }

    pub fn get(&self, name: &str) -> Option<Arc<LsmEngine>> {
        self.engines.read().unwrap().get(name).cloned()
    }

    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    /// Per-collection default TTL in seconds, or `None` if the collection is
    /// unknown. A value of `0` means "no default expiry".
    pub fn default_ttl(&self, name: &str) -> Option<u32> {
        self.metadata
            .read()
            .unwrap()
            .get(name)
            .map(|m| m.default_ttl_secs)
    }

    /// Create a collection by name, stamping engine config from the server
    /// template. Convenience over `create` for the `CREATE COLLECTION` path,
    /// where the client supplies only a name and optional default TTL.
    pub fn create_named(&self, name: &str, default_ttl_secs: u32) -> io::Result<()> {
        let config = Collections::meta_from_template(&self.template, name, default_ttl_secs);
        self.create(config)
    }

    /// Remove a collection: drop its engine, delete its data files, and update
    /// the catalog. Named `remove` (not `drop`) to avoid colliding with the
    /// `Drop` destructor when called through smart pointers like `Arc`.
    pub fn remove(&self, name: &str) -> io::Result<()> {
        if name == self.default_name {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot drop the default collection",
            ));
        }
        // Mutate the registry and persist the catalog under the locks, then
        // release them *before* the slow part (joining the engine's flush
        // thread and deleting files). Every data op routes through `get()`
        // (an `engines` read lock), so holding the write lock across that I/O
        // would stall reads/writes on *every* collection for the duration.
        let engine = {
            let mut engines = self.engines.write().unwrap();
            let mut metadata = self.metadata.write().unwrap();

            // Keep the removed entries so we can restore them if persist fails.
            let engine = match engines.remove(name) {
                Some(e) => e,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "no such collection",
                    ));
                }
            };
            let meta = metadata.remove(name);
            if meta.is_none() {
                eprintln!(
                    "collections: registry inconsistency — '{name}' present in engines but missing from metadata"
                );
            }

            // Record the removal durably *before* touching data files: a crash
            // between the two then leaves harmless orphan files rather than a
            // catalog that points at deleted data.
            if let Err(e) = Collections::persist_catalog(&self.db_path, metadata.values()) {
                // Roll back so memory matches the still-intact catalog on disk.
                engines.insert(name.to_string(), engine);
                if let Some(m) = meta {
                    metadata.insert(name.to_string(), m);
                }
                return Err(e);
            }
            engine
        }; // registry locks released here

        // Drop our engine reference now (before deleting files) so its flush
        // thread is joined here if this was the last Arc — avoids racing a
        // final flush against file deletion. Done outside the registry locks.
        drop(engine);
        Collections::delete_collection_files(&self.db_path, name)?;

        Ok(())
    }

    pub fn create(&self, config: CollectionMeta) -> io::Result<()> {
        if !Collections::is_valid_collection_name(&config.name) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid collection name",
            ));
        }
        let mut engines = self.engines.write().unwrap();
        let mut metadata = self.metadata.write().unwrap();
        if engines.contains_key(&config.name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "collection exists",
            ));
        }
        let name = config.name.clone();
        let engine = Collections::build_engine(&self.db_path, &config)?;
        engines.insert(name.clone(), engine);
        metadata.insert(name.clone(), config);

        // On a persist failure, roll back the in-memory inserts so memory
        // matches the durable catalog (contract: on Err, nothing changed) and
        // clean up the WAL/SSTable files `build_engine` just created, so a
        // failed CREATE leaves no orphans behind. Drop the engine first to
        // join its flush thread and release file handles (required on Windows
        // before the files can be removed).
        if let Err(e) = Collections::persist_catalog(&self.db_path, metadata.values()) {
            let engine = engines.remove(&name);
            metadata.remove(&name);
            drop(engine);
            let _ = Collections::delete_collection_files(&self.db_path, &name);
            return Err(e);
        }
        Ok(())
    }

    pub fn show(&self) -> Vec<CollectionMeta> {
        let mut metas: Vec<CollectionMeta> =
            self.metadata.read().unwrap().values().cloned().collect();

        metas.sort_by(|a, b| a.name.cmp(&b.name));
        metas
    }

    fn delete_collection_files(db_path: &str, name: &str) -> io::Result<()> {
        let wal = format!("{name}.wal");
        let sst_prefix = format!("{name}_");
        for entry in std::fs::read_dir(db_path)? {
            let entry = entry?;
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname == wal || (fname.starts_with(&sst_prefix) && fname.ends_with(".sst")) {
                std::fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn persist_catalog<'a>(
        db_path: &str,
        entries: impl IntoIterator<Item = &'a CollectionMeta>,
    ) -> io::Result<()> {
        let tmp = Path::new(db_path).join(CATALOG_TMP);
        let final_path = Path::new(db_path).join(CATALOG_FILE);

        // Sort by name so the on-disk catalog is deterministic (stable diffs,
        // reproducible tests) regardless of HashMap iteration order.
        let mut entries: Vec<&CollectionMeta> = entries.into_iter().collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));

        let mut body = String::new();
        for meta in entries {
            body.push_str(&meta.name);
            body.push('\t');
            body.push_str(&meta.default_ttl_secs.to_string());
            body.push('\t');
            body.push_str(&meta.max_memtable_bytes.to_string());
            body.push('\t');
            body.push_str(&meta.block_size_bytes.to_string());
            body.push('\t');
            body.push_str(&meta.block_compression_enabled.to_string());
            body.push('\t');
            body.push_str(meta.storage_strategy.as_str());
            body.push('\t');
            body.push_str(&meta.leveled_num_levels.to_string());
            body.push('\t');
            body.push_str(&meta.leveled_l0_threshold.to_string());
            body.push('\t');
            body.push_str(&meta.leveled_l1_max_bytes.to_string());
            body.push('\n');
        }
        fs::write(&tmp, body)?;
        fs::rename(&tmp, &final_path)?; // atomic replace
        Ok(())
    }
}
