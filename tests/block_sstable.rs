use rustikv::block::{BlockHeader, BlockReader, BlockWriter};
use rustikv::engine::StorageEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::memtable::Memtable;
use rustikv::record::{Record, RecordHeader};
use rustikv::size_tiered::SizeTiered;
use rustikv::sstable::SSTable;
use std::{env, fs, time::SystemTime};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_store_block_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn make_record(key: &str, value: &str) -> Record {
    Record {
        header: RecordHeader {
            crc32: 0,
            key_size: key.len() as u64,
            value_size: value.len() as u64,
            flags: 0,
            expiry_ms: None,
        },
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[test]
fn test_block_header_serialization() {
    let header = BlockHeader {
        uncompressed_size: 1024,
        stored_size: 512,
        compression_flag: 1,
    };

    let bytes = header.to_bytes();
    assert_eq!(bytes.len(), 9);

    let restored = BlockHeader::from_bytes(&bytes);
    assert_eq!(restored.uncompressed_size, 1024);
    assert_eq!(restored.stored_size, 512);
    assert_eq!(restored.compression_flag, 1);
}

#[test]
fn test_block_header_big_endian() {
    let header = BlockHeader {
        uncompressed_size: 0x12345678,
        stored_size: 0x9ABCDEF0,
        compression_flag: 0,
    };

    let bytes = header.to_bytes();
    assert_eq!(bytes[0], 0x12);
    assert_eq!(bytes[1], 0x34);
    assert_eq!(bytes[2], 0x56);
    assert_eq!(bytes[3], 0x78);
    assert_eq!(bytes[4], 0x9A);
    assert_eq!(bytes[5], 0xBC);
    assert_eq!(bytes[6], 0xDE);
    assert_eq!(bytes[7], 0xF0);
    assert_eq!(bytes[8], 0);
}

#[test]
fn test_block_writer_flush_produces_block() {
    let mut writer = BlockWriter::new(4096, false);
    let record = make_record("key", "value");

    writer.add_record(&record).unwrap();
    let block = writer.flush().unwrap();
    assert!(
        block.is_some(),
        "flush should produce a block when buffer is non-empty"
    );
}

#[test]
fn test_block_writer_flush_empty_produces_none() {
    let mut writer = BlockWriter::new(4096, false);
    let block = writer.flush().unwrap();
    assert!(block.is_none(), "flush on empty writer should produce None");
}

#[test]
fn test_block_writer_exceeds_target_size() {
    let mut writer = BlockWriter::new(50, false); // tiny target
    let record = make_record("key", "value");

    let mut block_produced = false;
    for _ in 0..20 {
        if writer.add_record(&record).unwrap().is_some() {
            block_produced = true;
            break;
        }
    }
    assert!(
        block_produced,
        "writer should produce a block when target size is exceeded"
    );
}

#[test]
fn test_block_roundtrip_uncompressed() {
    let mut writer = BlockWriter::new(4096, false);
    let record = make_record("hello", "world");
    writer.add_record(&record).unwrap();
    let block_bytes = writer.flush().unwrap().expect("expected a block");

    // Parse header from block bytes
    let header_bytes: [u8; 9] = block_bytes[..9].try_into().unwrap();
    let header = BlockHeader::from_bytes(&header_bytes);
    assert_eq!(header.compression_flag, 0);

    // Read block payload back
    let mut payload = &block_bytes[9..];
    let decompressed = BlockReader::read_block(&mut payload, &header).unwrap();

    // Decompressed bytes should contain a readable record
    let mut cursor = decompressed.as_slice();
    let recovered = Record::read_next(&mut cursor).unwrap();
    assert_eq!(recovered.key, "hello");
    assert_eq!(recovered.value, "world");
}

#[test]
fn test_block_roundtrip_compressed() {
    let mut writer = BlockWriter::new(4096, true);
    let record = make_record("alpha", "aaaaaaaaaaaaaaaaaaaaaaaaaaaa"); // repetitive = compressible
    writer.add_record(&record).unwrap();
    let block_bytes = writer.flush().unwrap().expect("expected a block");

    let header_bytes: [u8; 9] = block_bytes[..9].try_into().unwrap();
    let header = BlockHeader::from_bytes(&header_bytes);
    assert_eq!(
        header.compression_flag, 1,
        "block should be marked as compressed"
    );

    let mut payload = &block_bytes[9..];
    let decompressed = BlockReader::read_block(&mut payload, &header).unwrap();

    let mut cursor = decompressed.as_slice();
    let recovered = Record::read_next(&mut cursor).unwrap();
    assert_eq!(recovered.key, "alpha");
    assert_eq!(recovered.value, "aaaaaaaaaaaaaaaaaaaaaaaaaaaa");
}

// ── Task 4: SSTable block I/O ───────────────────────────────────────────────

#[test]
fn test_sstable_from_memtable_blocks() {
    let mut memtable = Memtable::new();
    memtable.insert("key1".into(), "value1".into(), None);
    memtable.insert("key2".into(), "value2".into(), None);
    memtable.insert("key3".into(), "value3".into(), None);

    let dir = temp_dir("from_memtable");
    let sstable = SSTable::from_memtable(&dir, "test", &memtable, None, 4096, true).unwrap();

    assert!(sstable.bloom.might_contain("key1"));
    assert!(sstable.bloom.might_contain("key2"));
    assert!(sstable.bloom.might_contain("key3"));
}

#[test]
fn test_sstable_get_from_blocks() {
    let mut memtable = Memtable::new();
    memtable.insert("alpha".into(), "apple".into(), None);
    memtable.insert("bravo".into(), "banana".into(), None);
    memtable.insert("charlie".into(), "cherry".into(), None);

    let dir = temp_dir("get_from_blocks");
    let sstable = SSTable::from_memtable(&dir, "test", &memtable, None, 4096, true).unwrap();

    assert_eq!(
        sstable.get("alpha").unwrap(),
        Some(Some(("apple".to_string(), None)))
    );
    assert_eq!(
        sstable.get("bravo").unwrap(),
        Some(Some(("banana".to_string(), None)))
    );
    assert_eq!(
        sstable.get("charlie").unwrap(),
        Some(Some(("cherry".to_string(), None)))
    );
    assert_eq!(sstable.get("delta").unwrap(), None);
}

#[test]
fn test_sstable_iter_blocks() {
    let mut memtable = Memtable::new();
    memtable.insert("x".into(), "1".into(), None);
    memtable.insert("y".into(), "2".into(), None);
    memtable.insert("z".into(), "3".into(), None);

    let dir = temp_dir("iter_blocks");
    let sstable = SSTable::from_memtable(&dir, "test", &memtable, None, 4096, true).unwrap();

    let records: Vec<_> = sstable.iter().unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].key, "x");
    assert_eq!(records[1].key, "y");
    assert_eq!(records[2].key, "z");
}

#[test]
fn test_sstable_get_missing_key() {
    let mut memtable = Memtable::new();
    memtable.insert("aaa".into(), "val".into(), None);
    memtable.insert("bbb".into(), "val".into(), None);

    let dir = temp_dir("missing_key");
    let sstable = SSTable::from_memtable(&dir, "test", &memtable, None, 4096, true).unwrap();

    assert_eq!(sstable.get("zzz").unwrap(), None);
}

#[test]
fn test_sstable_rebuild_index() {
    let mut memtable = Memtable::new();
    for i in 0..10 {
        memtable.insert(format!("key{:02}", i), format!("val{}", i), None);
    }

    let dir = temp_dir("rebuild_index");
    let mut sstable = SSTable::from_memtable(&dir, "test", &memtable, None, 4096, true).unwrap();

    sstable.rebuild_index().unwrap();

    assert_eq!(
        sstable.get("key00").unwrap(),
        Some(Some(("val0".to_string(), None)))
    );
    assert_eq!(
        sstable.get("key09").unwrap(),
        Some(Some(("val9".to_string(), None)))
    );
}

// ── Task 4 (original) ───────────────────────────────────────────────────────

#[test]
fn test_block_header_roundtrip_zero() {
    let header = BlockHeader {
        uncompressed_size: 0,
        stored_size: 0,
        compression_flag: 0,
    };
    let bytes = header.to_bytes();
    let restored = BlockHeader::from_bytes(&bytes);
    assert_eq!(restored.uncompressed_size, 0);
    assert_eq!(restored.stored_size, 0);
    assert_eq!(restored.compression_flag, 0);
}

// ── Task 5: Integration tests ───────────────────────────────────────────────

fn lsm_engine(dir: &str) -> LsmEngine {
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(dir, "test", 1, strategy, 4096, true).unwrap()
}

#[test]
fn test_lsm_block_set_get_across_sstable() {
    let dir = temp_dir("lsm_set_get");
    let engine = lsm_engine(&dir);

    engine.set("k1", "v1").unwrap();
    engine.set("k2", "v2").unwrap();
    engine.set("k3", "v3").unwrap();

    let (_, v1) = engine.get("k1").unwrap().unwrap();
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    let (_, v3) = engine.get("k3").unwrap().unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
    assert_eq!(v3, "v3");
}

#[test]
fn test_lsm_block_missing_key_returns_none() {
    let dir = temp_dir("lsm_missing");
    let engine = lsm_engine(&dir);

    engine.set("exists", "yes").unwrap();
    assert_eq!(engine.get("missing").unwrap(), None);
}

#[test]
fn test_lsm_block_overwrite_reads_latest() {
    let dir = temp_dir("lsm_overwrite");
    let engine = lsm_engine(&dir);

    engine.set("k", "old").unwrap();
    engine.set("k", "new").unwrap();

    let (_, v) = engine.get("k").unwrap().unwrap();
    assert_eq!(v, "new");
}

#[test]
fn test_lsm_block_delete_hides_value() {
    let dir = temp_dir("lsm_delete");
    let engine = lsm_engine(&dir);

    engine.set("k", "val").unwrap();
    engine.delete("k").unwrap();
    assert_eq!(engine.get("k").unwrap(), None);
}

#[test]
fn test_lsm_block_compact_preserves_values() {
    let dir = temp_dir("lsm_compact_preserve");
    let engine = lsm_engine(&dir);

    for i in 0..20u32 {
        engine
            .set(&format!("key{:03}", i), &format!("val{}", i))
            .unwrap();
    }
    engine.compact().unwrap();

    for i in 0..20u32 {
        let (_, v) = engine.get(&format!("key{:03}", i)).unwrap().unwrap();
        assert_eq!(v, format!("val{}", i));
    }
}

#[test]
fn test_lsm_block_compact_drops_deleted_keys() {
    let dir = temp_dir("lsm_compact_delete");
    let engine = lsm_engine(&dir);

    engine.set("k1", "v1").unwrap();
    engine.set("k2", "v2").unwrap();
    engine.delete("k1").unwrap();
    engine.compact().unwrap();

    assert_eq!(engine.get("k1").unwrap(), None);
    let (_, v2) = engine.get("k2").unwrap().unwrap();
    assert_eq!(v2, "v2");
}
