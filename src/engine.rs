use std::any::Any;
use std::io;

pub trait StorageEngine: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<(String, String)>, std::io::Error>;
    fn delete(&self, key: &str) -> Result<Option<()>, std::io::Error>;
    fn compact(&self) -> Result<(), std::io::Error>;
    fn dead_bytes(&self) -> u64;
    fn total_bytes(&self) -> u64;
    fn segment_count(&self) -> usize;
    fn list_keys(&self) -> io::Result<Vec<String>>;
    fn exists(&self, key: &str) -> bool;
    fn mget(&self, keys: Vec<String>) -> Result<Vec<(String, Option<String>)>, std::io::Error>;
    fn as_any(&self) -> &dyn Any;
    fn compact_step(&self) -> io::Result<bool> {
        Ok(false)
    }
    fn ttl(&self, key: &str, expiry_ms: Option<u64>) -> io::Result<TtlOutcome>;
    fn set_with_ttl(&self, key: &str, value: &str, expiry_ms: Option<u64>) -> io::Result<()>;
    fn mset_with_ttl(&self, items: Vec<(String, String, Option<u64>)>) -> io::Result<()>;
    fn set(&self, key: &str, value: &str) -> io::Result<()> {
        self.set_with_ttl(key, value, None)
    }
    fn mset(&self, items: Vec<(String, String)>) -> io::Result<()> {
        self.mset_with_ttl(items.into_iter().map(|(k, v)| (k, v, None)).collect())
    }
    fn incr(&self, key: &str) -> io::Result<i64>;
}

pub trait RangeScan {
    fn range(&self, start: &str, end: &str) -> io::Result<Vec<(String, String)>>;
}

pub enum TtlOutcome {
    Set,
    Persisted,
    NotFound,
}
