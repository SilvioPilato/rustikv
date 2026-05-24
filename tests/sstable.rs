use rustikv::memtable::Memtable;
use rustikv::sstable::{SSTable, get_sstables};
use std::{env, fs, io::ErrorKind, time::SystemTime};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_store_sst_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn make_memtable(entries: &[(&str, &str)]) -> Memtable {
    let mut mt = Memtable::new();
    for (k, v) in entries {
        mt.insert(k.to_string(), v.to_string(), None);
    }
    mt
}

#[test]
fn flush_and_get() {
    let dir = temp_dir("flush_get");
    let mt = make_memtable(&[("k1", "v1"), ("k2", "v2")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    assert_eq!(sst.get("k1").unwrap(), Some(Some(("v1".to_string(), None))));
    assert_eq!(sst.get("k2").unwrap(), Some(Some(("v2".to_string(), None))));
}

#[test]
fn get_missing_key() {
    let dir = temp_dir("missing");
    let mt = make_memtable(&[("a", "1"), ("c", "3")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    assert_eq!(sst.get("b").unwrap(), None);
    assert_eq!(sst.get("z").unwrap(), None);
}

#[test]
fn get_returns_tombstone() {
    let dir = temp_dir("tombstone");
    let mut mt = Memtable::new();
    mt.insert("alive".to_string(), "yes".to_string(), None);
    mt.remove("dead".to_string());
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    assert_eq!(
        sst.get("alive").unwrap(),
        Some(Some(("yes".to_string(), None)))
    );
    assert_eq!(sst.get("dead").unwrap(), Some(None)); // tombstone
}

#[test]
fn iter_returns_sorted_records() {
    let dir = temp_dir("iter_sorted");
    let mt = make_memtable(&[("cherry", "3"), ("apple", "1"), ("banana", "2")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    let keys: Vec<String> = sst.iter().unwrap().map(|r| r.unwrap().key).collect();
    assert_eq!(keys, vec!["apple", "banana", "cherry"]);
}

#[test]
fn get_early_exit_on_greater_key() {
    let dir = temp_dir("early_exit");
    let mt = make_memtable(&[("a", "1"), ("c", "3"), ("e", "5")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    // "b" is between "a" and "c", should return None without scanning whole file
    assert_eq!(sst.get("b").unwrap(), None);
}

#[test]
fn parse_valid_filename() {
    let sst = SSTable::parse("mydb_1234567890.sst");
    assert!(sst.is_some());
    let sst = sst.unwrap();
    assert_eq!(sst.timestamp, 1234567890);
}

#[test]
fn parse_invalid_filenames() {
    assert!(SSTable::parse("mydb_1234567890.db").is_none());
    assert!(SSTable::parse("notsst").is_none());
    assert!(SSTable::parse("bad_notanumber.sst").is_none());
}

#[test]
fn get_sstables_lists_and_sorts() {
    let dir = temp_dir("list_sort");
    let mt1 = make_memtable(&[("k1", "v1")]);
    let _sst1 = SSTable::from_memtable(&dir, "test", &mt1, None, 4096, true).unwrap();

    // Small delay to ensure different timestamp
    std::thread::sleep(std::time::Duration::from_millis(5));

    let mt2 = make_memtable(&[("k2", "v2")]);
    let _sst2 = SSTable::from_memtable(&dir, "test", &mt2, None, 4096, true).unwrap();

    let tables = get_sstables(&dir, "test").unwrap();
    assert_eq!(tables.len(), 2);
    // Should be sorted oldest-first
    assert!(tables[0].timestamp <= tables[1].timestamp);
}

#[test]
fn get_sstables_filters_by_name() {
    let dir = temp_dir("filter_name");
    let mt = make_memtable(&[("k", "v")]);
    SSTable::from_memtable(&dir, "alpha", &mt, None, 4096, true).unwrap();
    SSTable::from_memtable(&dir, "beta", &mt, None, 4096, true).unwrap();

    let alpha_tables = get_sstables(&dir, "alpha").unwrap();
    assert_eq!(alpha_tables.len(), 1);

    let beta_tables = get_sstables(&dir, "beta").unwrap();
    assert_eq!(beta_tables.len(), 1);
}

// Corrupt the byte at `offset` in the file at `path` by flipping all its bits.
fn corrupt_byte(path: &std::path::Path, offset: u64) {
    let mut data = fs::read(path).unwrap();
    data[offset as usize] ^= 0xFF;
    fs::write(path, data).unwrap();
}

#[test]
fn iter_propagates_crc_error() {
    let dir = temp_dir("iter_crc_error");
    let mt = make_memtable(&[("apple", "1"), ("banana", "2"), ("cherry", "3")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, false).unwrap();

    // Corrupt the first byte of the key payload. With uncompressed blocks the layout is:
    // 9-byte block header, then records at the old offsets — so key starts at 9+21 = 30.
    corrupt_byte(&sst.path, 30);

    let results: Vec<_> = sst.iter().unwrap().collect();
    assert!(
        results.iter().any(|r| r.is_err()),
        "expected at least one Err from a corrupt record"
    );
    let err = results
        .iter()
        .find(|r| r.is_err())
        .unwrap()
        .as_ref()
        .unwrap_err();
    assert_eq!(
        err.kind(),
        ErrorKind::InvalidData,
        "expected CRC mismatch error"
    );
}

#[test]
fn get_propagates_crc_error() {
    let dir = temp_dir("get_crc_error");
    let mt = make_memtable(&[("apple", "1"), ("banana", "2")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, false).unwrap();

    // Corrupt the first byte of the key payload. With uncompressed blocks the layout is:
    // 9-byte block header, then records at the old offsets — so key starts at 9+21 = 30.
    corrupt_byte(&sst.path, 30);

    // Searching for "banana" (second record) requires scanning past the corrupt first record.
    let result = sst.get("banana");
    assert!(
        result.is_err(),
        "expected Err when corrupt record is in scan path"
    );
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidData);
}

#[test]
fn iter_clean_eof_yields_none() {
    let dir = temp_dir("iter_eof");
    let mt = make_memtable(&[("k", "v")]);
    let sst = SSTable::from_memtable(&dir, "test", &mt, None, 4096, true).unwrap();

    // Iterator should yield exactly one record then stop cleanly (None), not an error.
    let mut iter = sst.iter().unwrap();
    assert!(iter.next().unwrap().is_ok());
    assert!(
        iter.next().is_none(),
        "expected clean EOF after last record"
    );
}
