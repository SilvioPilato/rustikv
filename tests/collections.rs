use rustikv::collections::{CollectionMeta, Collections};
use rustikv::engine::StorageEngine;
use rustikv::lsmengine::LsmConfig;
use rustikv::settings::StorageStrategyKind;
use std::{env, fs, path::Path, time::SystemTime};

const CATALOG_FILE: &str = "collections.catalog";
const DEFAULT: &str = "default";

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_store_collections_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn test_config() -> LsmConfig {
    LsmConfig {
        storage_strategy: StorageStrategyKind::SizeTiered,
        leveled_num_levels: 4,
        leveled_l0_threshold: 4,
        leveled_l1_max_bytes: 10 * 1024 * 1024,
        block_size_bytes: 4096,
        block_compression_enabled: true,
        max_memtable_bytes: 1_048_576, // 1 MB — won't auto-flush during tests
    }
}

/// A CollectionMeta mirroring `test_config`, with a chosen name + default TTL.
fn meta(name: &str, default_ttl_secs: u32) -> CollectionMeta {
    CollectionMeta {
        name: name.to_string(),
        default_ttl_secs,
        max_memtable_bytes: 1_048_576,
        block_size_bytes: 4096,
        block_compression_enabled: true,
        storage_strategy: StorageStrategyKind::SizeTiered,
        leveled_num_levels: 4,
        leveled_l0_threshold: 4,
        leveled_l1_max_bytes: 10 * 1024 * 1024,
    }
}

fn load(dir: &str) -> Collections {
    Collections::load_from_file(dir.to_string(), DEFAULT.to_string(), test_config()).unwrap()
}

// --- load / bootstrap ---

#[test]
fn fresh_load_creates_default_and_persists_catalog() {
    let dir = temp_dir("fresh");
    let c = load(&dir);

    assert!(c.get(DEFAULT).is_some(), "default collection must exist");
    assert!(
        Path::new(&dir).join(CATALOG_FILE).exists(),
        "bootstrap must persist a catalog file on first run"
    );
}

#[test]
fn load_accepts_hyphenated_default_name() {
    // A hyphen is in the legal charset, so a `--name` like this must load.
    let dir = temp_dir("hyphendefault");
    let c = Collections::load_from_file(dir, "my-segment".to_string(), test_config())
        .expect("hyphenated default name is valid");
    assert!(c.get("my-segment").is_some());
}

/// `load_from_file` returns the opaque `Collections` on success, which isn't
/// `Debug`, so `unwrap_err`/`expect_err` won't compile. Extract the error by
/// hand instead.
fn load_err(dir: String, default_name: &str) -> std::io::Error {
    match Collections::load_from_file(dir, default_name.to_string(), test_config()) {
        Ok(_) => panic!("expected load_from_file to fail, but it succeeded"),
        Err(e) => e,
    }
}

#[test]
fn load_rejects_invalid_default_name() {
    // The default collection name comes from `--name`; if it falls outside the
    // [A-Za-z0-9-] charset the server must refuse to start with a clear error
    // instead of booting once and then failing to reload its own catalog.
    let dir = temp_dir("baddefault");
    assert_eq!(
        load_err(dir.clone(), "bad_name").kind(),
        std::io::ErrorKind::InvalidInput
    );

    // And it must not have written a catalog it would later reject.
    assert!(
        !Path::new(&dir).join(CATALOG_FILE).exists(),
        "an invalid default name must not persist a catalog"
    );

    // Empty name is rejected too.
    assert_eq!(
        load_err(temp_dir("emptydefault"), "").kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
fn load_rejects_malformed_catalog() {
    // A corrupt catalog line must fail the load loudly rather than silently
    // dropping collections (which would orphan their data files).
    let dir = temp_dir("badcatalog");
    fs::write(
        Path::new(&dir).join(CATALOG_FILE),
        "metrics\t0\tnot-enough-fields\n",
    )
    .unwrap();
    assert_eq!(
        load_err(dir, DEFAULT).kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn load_rejects_catalog_with_bad_field_value() {
    // Right field count, but a non-numeric TTL: still InvalidData.
    let dir = temp_dir("badfield");
    fs::write(
        Path::new(&dir).join(CATALOG_FILE),
        // name, ttl(bad), memtable, block, compress, strategy, levels, l0, l1
        "metrics\tnotanumber\t1048576\t4096\ttrue\tsize-tiered\t4\t4\t10485760\n",
    )
    .unwrap();
    assert_eq!(
        load_err(dir, DEFAULT).kind(),
        std::io::ErrorKind::InvalidData
    );
}

// --- create ---

#[test]
fn create_makes_collection_usable() {
    let dir = temp_dir("create");
    let c = load(&dir);
    c.create(meta("metrics", 3600)).unwrap();

    let engine = c.get("metrics").expect("metrics should be in the registry");
    engine.set("k", "v").unwrap();
    assert_eq!(
        engine.get("k").unwrap(),
        Some(("k".to_string(), "v".to_string()))
    );
}

#[test]
fn create_duplicate_errors() {
    let dir = temp_dir("dup");
    let c = load(&dir);
    c.create(meta("metrics", 0)).unwrap();

    let err = c.create(meta("metrics", 0)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
}

#[test]
fn create_rejects_invalid_names() {
    let dir = temp_dir("invalid");
    let c = load(&dir);

    // underscore is not in the [A-Za-z0-9-] charset
    assert_eq!(
        c.create(meta("bad_name", 0)).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    // empty name
    assert_eq!(
        c.create(meta("", 0)).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

// --- drop ---

#[test]
fn drop_removes_collection_and_files() {
    let dir = temp_dir("drop");
    let c = load(&dir);
    c.create(meta("metrics", 0)).unwrap();

    let wal = Path::new(&dir).join("metrics.wal");
    assert!(wal.exists(), "wal should exist after create");

    c.remove("metrics").unwrap();
    assert!(c.get("metrics").is_none(), "registry entry must be gone");
    assert!(!wal.exists(), "data files must be deleted on drop");
}

#[test]
fn drop_default_errors() {
    let dir = temp_dir("dropdef");
    let c = load(&dir);
    assert_eq!(
        c.remove(DEFAULT).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
fn drop_missing_errors() {
    let dir = temp_dir("dropmiss");
    let c = load(&dir);
    assert_eq!(
        c.remove("ghost").unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

// --- show ---

#[test]
fn show_returns_all_collections_sorted() {
    let dir = temp_dir("show");
    let c = load(&dir);
    c.create(meta("zebra", 0)).unwrap();
    c.create(meta("alpha", 0)).unwrap();

    let names: Vec<String> = c.show().into_iter().map(|m| m.name).collect();
    assert_eq!(
        names,
        vec![
            "alpha".to_string(),
            "default".to_string(),
            "zebra".to_string()
        ]
    );
}

// --- catalog round-trip / reload ---

#[test]
fn catalog_survives_reload_with_config() {
    let dir = temp_dir("reload");
    {
        let c = load(&dir);
        c.create(meta("metrics", 86400)).unwrap();
        c.create(meta("events", 0)).unwrap();
    } // Collections dropped: in-memory registry gone, only the catalog file remains

    let c2 = load(&dir); // rebuilds purely from the on-disk catalog
    assert!(c2.get("metrics").is_some());
    assert!(c2.get("events").is_some());
    assert!(c2.get(DEFAULT).is_some());

    // Per-collection metadata must round-trip through the catalog.
    let metrics = c2
        .show()
        .into_iter()
        .find(|m| m.name == "metrics")
        .expect("metrics in metadata");
    assert_eq!(metrics.default_ttl_secs, 86400);
    assert_eq!(metrics.block_size_bytes, 4096);
    assert_eq!(metrics.storage_strategy.as_str(), "size-tiered");
}

#[test]
fn dropped_collection_does_not_resurrect_after_reload() {
    let dir = temp_dir("dropreload");
    {
        let c = load(&dir);
        c.create(meta("temp", 0)).unwrap();
        c.remove("temp").unwrap();
    }
    let c2 = load(&dir);
    assert!(
        c2.get("temp").is_none(),
        "a dropped collection must stay gone across reload"
    );
}

#[test]
fn data_survives_reload() {
    let dir = temp_dir("datapersist");
    {
        let c = load(&dir);
        c.create(meta("metrics", 0)).unwrap();
        c.get("metrics").unwrap().set("k", "v").unwrap();
    } // engine dropped; data lives in the WAL on disk

    let c2 = load(&dir); // build_engine uses *_from_dir → replays the WAL
    assert_eq!(
        c2.get("metrics").unwrap().get("k").unwrap(),
        Some(("k".to_string(), "v".to_string())),
        "writes must survive a reload via WAL replay"
    );
}

// --- data isolation ---

#[test]
fn collections_isolate_their_keyspaces() {
    let dir = temp_dir("isolate");
    let c = load(&dir);
    c.create(meta("a", 0)).unwrap();
    c.create(meta("b", 0)).unwrap();

    c.get("a").unwrap().set("key", "from-a").unwrap();
    c.get("b").unwrap().set("key", "from-b").unwrap();

    assert_eq!(
        c.get("a").unwrap().get("key").unwrap(),
        Some(("key".to_string(), "from-a".to_string()))
    );
    assert_eq!(
        c.get("b").unwrap().get("key").unwrap(),
        Some(("key".to_string(), "from-b".to_string()))
    );
}
