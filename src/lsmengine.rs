use std::ops::Bound::{Included, Unbounded};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet, HashSet},
    io,
    path::PathBuf,
};

use crate::engine::TtlOutcome;
use crate::storage_strategy::StorageStrategy;
use crate::utils::{is_expired, now_ms, prefix_successor};
use crate::{
    engine::{RangeScan, StorageEngine},
    memtable::Memtable,
    sstable::SSTable,
    wal::Wal,
};
use std::sync::atomic::Ordering::Relaxed;

/// Lock-free read resolution: active → immutable → SSTable.
/// Borrows state directly — cannot lock, so re-entrancy is structurally impossible.
pub(crate) fn lookup(
    active: &Memtable,
    immutable: Option<&Memtable>,
    strategy: &dyn StorageStrategy,
    key: &str,
    now_ms: u64,
) -> io::Result<Option<(String, Option<u64>)>> {
    if let Some(entry) = active.entry(key) {
        return match (entry.value.as_deref(), entry.expiry_ms) {
            (Some(_), Some(ms)) if is_expired(ms, now_ms) => Ok(None),
            (Some(v), exp) => Ok(Some((v.to_string(), exp))),
            (None, _) => Ok(None),
        };
    }

    if let Some(memtable) = immutable
        && let Some(entry) = memtable.entry(key)
    {
        return match (entry.value.as_deref(), entry.expiry_ms) {
            (Some(_), Some(ms)) if is_expired(ms, now_ms) => Ok(None),
            (Some(v), exp) => Ok(Some((v.to_string(), exp))),
            (None, _) => Ok(None),
        };
    }

    for segment in strategy.iter_for_key(key) {
        match segment.get(key)? {
            Some(Some((v, exp))) => return Ok(Some((v, exp))),
            Some(None) => return Ok(None),
            None => continue,
        }
    }

    Ok(None)
}

/// Lock-free write: WAL append + memtable mutation.
/// Returns the new memtable size_bytes so the caller can decide to flush AFTER releasing locks.
pub(crate) fn apply_write(
    wal: &mut Wal,
    active: &mut Memtable,
    key: &str,
    value: Option<&str>,
    expiry_ms: Option<u64>,
) -> io::Result<usize> {
    match value {
        Some(v) => {
            wal.append(key.to_string(), v.to_string(), false, expiry_ms)?;
            active.insert(key.to_string(), v.to_string(), expiry_ms);
        }
        None => {
            wal.append(key.to_string(), String::new(), true, None)?;
            active.remove(key.to_string());
        }
    }
    Ok(active.size_bytes())
}

struct LsmShared {
    active: RwLock<Memtable>,
    immutable: RwLock<Option<Memtable>>,
    flush_handle: Mutex<Option<thread::JoinHandle<()>>>,
    db_path: String,
    db_name: String,
    max_memtable_bytes: AtomicUsize,
    wal: Mutex<Wal>,
    storage_strategy: RwLock<Box<dyn StorageStrategy>>,
    block_size_bytes: usize,
    block_compression_enabled: bool,
}

pub struct LsmEngine {
    shared: Arc<LsmShared>,
}

impl LsmEngine {
    pub fn new(
        db_path: &str,
        db_name: &str,
        max_memtable_bytes: usize,
        storage_strategy: Box<dyn StorageStrategy>,
        block_size_bytes: usize,
        block_compression_enabled: bool,
    ) -> io::Result<LsmEngine> {
        let wal = Wal::open(&PathBuf::from(db_path), db_name.to_string())?;
        Ok(LsmEngine {
            shared: Arc::new(LsmShared {
                active: RwLock::new(Memtable::new()),
                immutable: RwLock::new(None),
                flush_handle: Mutex::new(None),
                db_path: db_path.to_string(),
                db_name: db_name.to_string(),
                max_memtable_bytes: AtomicUsize::from(max_memtable_bytes),
                wal: Mutex::new(wal),
                storage_strategy: RwLock::new(storage_strategy),
                block_size_bytes,
                block_compression_enabled,
            }),
        })
    }

    pub fn from_dir(
        dir: &str,
        db_name: &str,
        max_memtable_bytes: usize,
        storage_strategy: Box<dyn StorageStrategy>,
        block_size_bytes: usize,
        block_compression_enabled: bool,
    ) -> io::Result<Self> {
        let wal = Wal::open(&PathBuf::from(dir), db_name.to_string())?;
        let memtable = wal.replay()?;
        Ok(LsmEngine {
            shared: Arc::new(LsmShared {
                active: RwLock::new(memtable),
                immutable: RwLock::new(None),
                flush_handle: Mutex::new(None),
                db_path: dir.to_string(),
                db_name: db_name.to_string(),
                max_memtable_bytes: AtomicUsize::from(max_memtable_bytes),
                wal: Mutex::new(wal),
                storage_strategy: RwLock::new(storage_strategy),
                block_size_bytes,
                block_compression_enabled,
            }),
        })
    }

    fn flush_memtable_async(&self) -> io::Result<()> {
        // Hold flush_handle for the entire operation to serialize concurrent flush calls
        let mut handle = self.shared.flush_handle.lock().unwrap();

        // Backpressure: wait for any in-flight flush to finish
        if let Some(h) = handle.take() {
            h.join().unwrap();
        }

        // Previous flush is done — immutable is None, safe to swap
        {
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            let mut immutable = self.shared.immutable.write().unwrap();
            let old = std::mem::take(&mut *active);
            *immutable = Some(old);
            wal.reset().map_err(|e| {
                eprintln!("flush_memtable_async: WAL reset failed: {}", e);
                e
            })?;
        }

        let shared = Arc::clone(&self.shared);
        let jh = thread::spawn(move || {
            {
                let immutable = shared.immutable.read().unwrap();
                if let Some(ref memtable) = *immutable {
                    let sstable = SSTable::from_memtable(
                        &shared.db_path,
                        &shared.db_name,
                        memtable,
                        None,
                        shared.block_size_bytes,
                        shared.block_compression_enabled,
                    )
                    .unwrap_or_else(|e| {
                        eprintln!("flush_memtable_async: SSTable write failed: {}", e);
                        panic!("flush_memtable_async: SSTable write failed");
                    });
                    drop(immutable);
                    let mut storage_strategy = shared.storage_strategy.write().unwrap();
                    storage_strategy.add_sstable(sstable).unwrap_or_else(|e| {
                        eprintln!("flush_memtable_async: add_sstable failed: {}", e);
                        panic!("flush_memtable_async: add_sstable failed");
                    });
                }
            }

            {
                let mut immutable = shared.immutable.write().unwrap();
                *immutable = None;
            }
        });

        *handle = Some(jh);
        Ok(())
    }
}

impl Drop for LsmEngine {
    fn drop(&mut self) {
        let mut handle = self.shared.flush_handle.lock().unwrap();
        if let Some(h) = handle.take() {
            let _ = h.join();
        }
    }
}

impl StorageEngine for LsmEngine {
    fn get(&self, key: &str) -> Result<Option<(String, String)>, std::io::Error> {
        let now = now_ms();
        let active = self.shared.active.read().unwrap();
        let immutable = self.shared.immutable.read().unwrap();
        let strategy = self.shared.storage_strategy.read().unwrap();
        match lookup(&active, immutable.as_ref(), strategy.as_ref(), key, now)? {
            Some((v, _)) => Ok(Some((key.to_string(), v))),
            None => Ok(None),
        }
    }

    fn set(&self, key: &str, value: &str) -> Result<(), std::io::Error> {
        let memtable_size = {
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            apply_write(&mut wal, &mut active, key, Some(value), None)?
        };

        if memtable_size >= self.shared.max_memtable_bytes.load(Relaxed) {
            self.flush_memtable_async()?;
        }

        Ok(())
    }

    fn delete(&self, key: &str) -> Result<Option<()>, std::io::Error> {
        let exists = self.get(key)?.is_some();

        let size_bytes = {
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            apply_write(&mut wal, &mut active, key, None, None)?
        };

        if size_bytes >= self.shared.max_memtable_bytes.load(Relaxed) {
            self.flush_memtable_async()?;
        }

        if exists { Ok(Some(())) } else { Ok(None) }
    }

    fn compact(&self) -> Result<(), std::io::Error> {
        // Wait for any in-flight background flush to finish
        {
            let mut handle = self.shared.flush_handle.lock().unwrap();
            if let Some(h) = handle.take() {
                h.join().unwrap();
            }
        }

        let mut wal = self.shared.wal.lock().unwrap();

        // Acquire active+immutable before storage_strategy to match get()'s
        // lock order (active → immutable → storage_strategy).
        let mut active = self.shared.active.write().unwrap();
        let mut immutable = self.shared.immutable.write().unwrap();
        let mut storage_strategy = self.shared.storage_strategy.write().unwrap();

        // Inline flush: swap active into immutable, flush to SSTable.
        let old = std::mem::take(&mut *active);
        *immutable = Some(old);

        if let Some(ref memtable) = *immutable {
            let sstable = SSTable::from_memtable(
                &self.shared.db_path,
                &self.shared.db_name,
                memtable,
                None,
                self.shared.block_size_bytes,
                self.shared.block_compression_enabled,
            )?;
            storage_strategy.add_sstable(sstable)?;
        }

        wal.reset()?;
        *immutable = None;

        // Release memtable locks before the (potentially slow) compaction I/O.
        drop(immutable);
        drop(active);
        drop(wal);

        storage_strategy.compact_all(&self.shared.db_path, &self.shared.db_name)?;

        Ok(())
    }

    fn compact_step(&self) -> io::Result<bool> {
        let mut storage_strategy = self.shared.storage_strategy.write().unwrap();
        storage_strategy.compact_if_needed(&self.shared.db_path, &self.shared.db_name)
    }

    fn dead_bytes(&self) -> u64 {
        0
    }

    fn total_bytes(&self) -> u64 {
        0
    }

    fn segment_count(&self) -> usize {
        let storage_strategy = self.shared.storage_strategy.read().unwrap();
        storage_strategy.segment_count()
    }

    fn list_keys(&self) -> io::Result<Vec<String>> {
        let mut keys: HashSet<String> = HashSet::new();
        let now_ms = now_ms();
        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            for segment in storage_strategy.iter_all() {
                for result in segment.iter()? {
                    let record = result?;
                    if record.header.is_tombstone() {
                        keys.remove(&record.key);
                        continue;
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        keys.remove(&record.key);
                        continue;
                    }

                    keys.insert(record.key);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (key, entry) in memtable.entries() {
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                            keys.remove(key);
                        }
                        (Some(_), _) => {
                            keys.insert(key.clone());
                        }
                        (None, _) => {
                            keys.remove(key);
                        }
                    }
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (key, entry) in memtable.entries() {
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                        keys.remove(key);
                    }
                    (Some(_), _) => {
                        keys.insert(key.clone());
                    }
                    (None, _) => {
                        keys.remove(key);
                    }
                }
            }
        }

        Ok(keys.into_iter().collect())
    }

    fn exists(&self, key: &str) -> bool {
        self.get(key).map(|v| v.is_some()).unwrap_or(false)
    }

    fn mget(&self, keys: Vec<String>) -> Result<Vec<(String, Option<String>)>, std::io::Error> {
        let mut res: Vec<(String, Option<String>)> = Vec::new();
        for key in keys {
            match self.get(&key)? {
                Some((k, v)) => {
                    res.push((k, Some(v)));
                }
                None => {
                    res.push((key, None));
                }
            }
        }

        Ok(res)
    }

    fn mset(&self, items: Vec<(String, String)>) -> Result<(), std::io::Error> {
        for (k, v) in items {
            self.set(&k, &v)?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ttl(&self, key: &str, expiry_ms: Option<u64>) -> io::Result<crate::engine::TtlOutcome> {
        let now = now_ms();
        let (found, size) = {
            // Lock order: wal -> active -> immutable -> storage_strategy
            // (matches set's wal->active and get's active->immutable->storage_strategy)
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            let immutable = self.shared.immutable.read().unwrap();
            let strategy = self.shared.storage_strategy.read().unwrap();

            match lookup(&active, immutable.as_ref(), strategy.as_ref(), key, now)? {
                Some((v, _)) => {
                    let sz = apply_write(&mut wal, &mut active, key, Some(&v), expiry_ms)?;
                    (true, Some(sz))
                }
                None => (false, None),
            }
        };
        if !found {
            return Ok(TtlOutcome::NotFound);
        }
        if let Some(sz) = size
            && sz >= self.shared.max_memtable_bytes.load(Relaxed)
        {
            self.flush_memtable_async()?;
        }
        Ok(if expiry_ms.is_some() {
            TtlOutcome::Set
        } else {
            TtlOutcome::Persisted
        })
    }

    fn set_with_ttl(&self, key: &str, value: &str, expiry_ms: Option<u64>) -> io::Result<()> {
        let memtable_size = {
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            apply_write(&mut wal, &mut active, key, Some(value), expiry_ms)?
        };

        if memtable_size >= self.shared.max_memtable_bytes.load(Relaxed) {
            self.flush_memtable_async()?;
        }

        Ok(())
    }

    fn mset_with_ttl(&self, items: Vec<(String, String, Option<u64>)>) -> io::Result<()> {
        for (k, v, expiry_ms) in items {
            self.set_with_ttl(&k, &v, expiry_ms)?;
        }

        Ok(())
    }

    fn incr(&self, key: &str) -> io::Result<i64> {
        let now = now_ms();
        let (memtable_size, value) = {
            let mut wal = self.shared.wal.lock().unwrap();
            let mut active = self.shared.active.write().unwrap();
            let immutable = self.shared.immutable.read().unwrap();
            let strategy = self.shared.storage_strategy.read().unwrap();
            let (mut value, expiry_ms): (i64, Option<u64>) =
                match lookup(&active, immutable.as_ref(), strategy.as_ref(), key, now)? {
                    Some((v, ttl)) => (
                        v.parse::<i64>()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        ttl,
                    ),
                    None => (0i64, None),
                };
            value = value.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "increment would overflow")
            })?;

            (
                apply_write(
                    &mut wal,
                    &mut active,
                    key,
                    Some(&value.to_string()),
                    expiry_ms,
                )?,
                value,
            )
        };

        if memtable_size >= self.shared.max_memtable_bytes.load(Relaxed) {
            self.flush_memtable_async()?;
        }

        Ok(value)
    }
}

fn resolve_numeric(op: &str, pairs: Vec<(String, String)>) -> io::Result<Vec<f64>> {
    let mut nums = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        match v.parse::<f64>() {
            Ok(n) => nums.push(n),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{op}: non-numeric value for key {k}"),
                ));
            }
        }
    }
    Ok(nums)
}

impl RangeScan for LsmEngine {
    fn range(&self, start: &str, end: &str) -> io::Result<Vec<(String, String)>> {
        if start > end {
            return Ok(vec![]);
        }

        let now_ms = now_ms();
        let mut b_map: BTreeMap<String, String> = BTreeMap::new();
        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            for segment in storage_strategy.iter_files_for_range(start, end) {
                for result in segment.iter()? {
                    let record = result?;
                    if record.key.as_str() < start || record.key.as_str() > end {
                        continue;
                    }

                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        b_map.remove(&record.key);
                        continue;
                    }

                    if record.header.is_tombstone() {
                        b_map.remove(&record.key);
                        continue;
                    }

                    b_map.insert(record.key, record.value);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(start), Included(end)))
                {
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                        (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                        (None, _) => b_map.remove(k),
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(start), Included(end)))
            {
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                    (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                    (None, _) => b_map.remove(k),
                };
            }
        }

        Ok(b_map.into_iter().collect())
    }

    fn prefix(&self, prefix: &str) -> io::Result<Vec<(String, String)>> {
        let now_ms = now_ms();
        let mut b_map: BTreeMap<String, String> = BTreeMap::new();

        // Pruning hint only (Inv 2). None => no finite successor, scan all (Inv 3/9).
        let successor = prefix_successor(prefix);

        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            let segments: Vec<&SSTable> = match successor.as_deref() {
                Some(end) => storage_strategy.iter_files_for_range(prefix, end).collect(),
                None => storage_strategy.iter_all().collect(),
            };
            for segment in segments {
                for result in segment.iter()? {
                    let record = result?;
                    if !record.key.starts_with(prefix) {
                        continue; // Inv 1
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        b_map.remove(&record.key);
                        continue;
                    }
                    if record.header.is_tombstone() {
                        b_map.remove(&record.key);
                        continue;
                    }
                    b_map.insert(record.key, record.value);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(prefix), Unbounded))
                {
                    if !k.starts_with(prefix) {
                        break; // sorted keys: matches are contiguous from `prefix`
                    }
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                        (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                        (None, _) => b_map.remove(k),
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(prefix), Unbounded))
            {
                if !k.starts_with(prefix) {
                    break;
                }
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                    (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                    (None, _) => b_map.remove(k),
                };
            }
        }

        Ok(b_map.into_iter().collect())
    }

    fn count_prefix(&self, prefix: &str) -> io::Result<usize> {
        let now_ms = now_ms();
        let mut set: BTreeSet<String> = BTreeSet::new();
        let successor = prefix_successor(prefix);

        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            let segments: Vec<&SSTable> = match successor.as_deref() {
                Some(end) => storage_strategy.iter_files_for_range(prefix, end).collect(),
                None => storage_strategy.iter_all().collect(),
            };
            for segment in segments {
                for result in segment.iter()? {
                    let record = result?;
                    if !record.key.starts_with(prefix) {
                        continue; // Inv 1
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        set.remove(&record.key);
                        continue;
                    }
                    if record.header.is_tombstone() {
                        set.remove(&record.key);
                        continue;
                    }
                    set.insert(record.key);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(prefix), Unbounded))
                {
                    if !k.starts_with(prefix) {
                        break;
                    }
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => set.remove(k),
                        (Some(_), _) => set.insert(k.clone()),
                        (None, _) => set.remove(k),
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(prefix), Unbounded))
            {
                if !k.starts_with(prefix) {
                    break;
                }
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => set.remove(k),
                    (Some(_), _) => set.insert(k.clone()),
                    (None, _) => set.remove(k),
                };
            }
        }

        Ok(set.len())
    }

    fn count_range(&self, start: &str, end: &str) -> io::Result<usize> {
        if start > end {
            return Ok(0);
        }

        let now_ms = now_ms();
        let mut set: BTreeSet<String> = BTreeSet::new();

        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            for segment in storage_strategy.iter_files_for_range(start, end) {
                for result in segment.iter()? {
                    let record = result?;
                    if record.key.as_str() < start || record.key.as_str() > end {
                        continue;
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        set.remove(&record.key);
                        continue;
                    }
                    if record.header.is_tombstone() {
                        set.remove(&record.key);
                        continue;
                    }
                    set.insert(record.key);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(start), Included(end)))
                {
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => set.remove(k),
                        (Some(_), _) => set.insert(k.clone()),
                        (None, _) => set.remove(k),
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(start), Included(end)))
            {
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => set.remove(k),
                    (Some(_), _) => set.insert(k.clone()),
                    (None, _) => set.remove(k),
                };
            }
        }

        Ok(set.len())
    }

    fn sum_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
        let pairs = self.prefix(prefix)?;
        let nums = resolve_numeric("SUM", pairs)?;
        if nums.is_empty() {
            return Ok(None);
        }
        Ok(Some(nums.iter().sum()))
    }

    fn sum_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
        let pairs = self.range(start, end)?;
        let nums = resolve_numeric("SUM", pairs)?;
        if nums.is_empty() {
            return Ok(None);
        }
        Ok(Some(nums.iter().sum()))
    }

    fn avg_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
        let pairs = self.prefix(prefix)?;
        let nums = resolve_numeric("AVG", pairs)?;
        if nums.is_empty() {
            return Ok(None);
        }
        let s: f64 = nums.iter().sum();
        Ok(Some(s / nums.len() as f64))
    }

    fn avg_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
        let pairs = self.range(start, end)?;
        let nums = resolve_numeric("AVG", pairs)?;
        if nums.is_empty() {
            return Ok(None);
        }
        let s: f64 = nums.iter().sum();
        Ok(Some(s / nums.len() as f64))
    }

    fn min_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
        let pairs = self.prefix(prefix)?;
        Ok(fold_min(&resolve_numeric("MIN", pairs)?))
    }

    fn min_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
        let pairs = self.range(start, end)?;
        Ok(fold_min(&resolve_numeric("MIN", pairs)?))
    }

    fn max_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
        let pairs = self.prefix(prefix)?;
        Ok(fold_max(&resolve_numeric("MAX", pairs)?))
    }

    fn max_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
        let pairs = self.range(start, end)?;
        Ok(fold_max(&resolve_numeric("MAX", pairs)?))
    }
}

/// Minimum of `nums`, or `None` if empty. Seeds from the first value (not an
/// `INFINITY` sentinel) and propagates NaN, so the result agrees with SUM/AVG
/// when a non-finite value is present and never leaks the seed on an all-NaN set.
fn fold_min(nums: &[f64]) -> Option<f64> {
    let mut it = nums.iter().copied();
    let first = it.next()?;
    Some(it.fold(first, |acc, x| {
        if acc.is_nan() || x.is_nan() {
            f64::NAN
        } else {
            acc.min(x)
        }
    }))
}

/// Maximum of `nums`, or `None` if empty. See [`fold_min`] for the seeding and
/// NaN-propagation rationale.
fn fold_max(nums: &[f64]) -> Option<f64> {
    let mut it = nums.iter().copied();
    let first = it.next()?;
    Some(it.fold(first, |acc, x| {
        if acc.is_nan() || x.is_nan() {
            f64::NAN
        } else {
            acc.max(x)
        }
    }))
}
