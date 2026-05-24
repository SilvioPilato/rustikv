use std::any::Any;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Error, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::{Mutex, RwLock};

use crate::engine::{StorageEngine, TtlOutcome};
use crate::hash_index::HashIndex;
use crate::hint::{Hint, HintEntry};
use crate::record::{
    FLAG_HAS_EXPIRY, FLAG_TOMBSTONE, MAX_KEY_SIZE, MAX_VALUE_SIZE, Record, RecordHeader,
};
use crate::segment::{Segment, get_segments};
use crate::settings::FSyncStrategy;
use crate::utils::{is_expired, now_ms};
use crate::wal::Wal;
use crate::worker::BackgroundWorker;

pub struct KVEngine {
    index: RwLock<HashIndex>,
    wal: Mutex<Wal>,
    active_file: Mutex<ActiveFileState>,
    active_segment: Mutex<Segment>,
    db_path: String,
    db_name: String,
    max_segment_bytes: AtomicU64,
    writes_since_fsync: AtomicU64,
    fsync_strategy: FSyncStrategy,
    dead_bytes: AtomicU64,
    total_bytes: AtomicU64,
    segment_count: AtomicUsize,
}

pub(crate) struct ActiveFileState {
    file: File,
    fsync_handle: Option<BackgroundWorker>,
}

/// Lock-free read resolution for the Bitcask-style engine: index → segment file.
///
/// Borrows state directly (never `&self`), so it cannot acquire a lock — std
/// `Mutex`/`RwLock` re-entrancy is structurally impossible. Returns the live
/// value, or `None` if the key is absent or expired (expiry filtering matches
/// the LSM `lookup`: expired ⇒ logically absent).
pub(crate) fn kv_lookup(
    index: &HashIndex,
    db_path: &str,
    db_name: &str,
    active_segment: &Segment,
    key: &str,
    now_ms: u64,
) -> io::Result<Option<(String, Option<u64>)>> {
    let entry = match index.get(key) {
        Some(e) => e,
        None => return Ok(None),
    };
    if entry.expiry_ms.is_some_and(|e| is_expired(e, now_ms)) {
        return Ok(None);
    }

    let mut file = if entry.segment_timestamp == active_segment.timestamp {
        File::open(active_segment.path(db_path))?
    } else {
        let seg = Segment {
            segment_name: db_name.to_string(),
            timestamp: entry.segment_timestamp,
        };
        File::open(seg.path(db_path))?
    };
    file.seek(SeekFrom::Start(entry.offset))?;
    let record = Record::read_next(&mut file)?;
    Ok(Some((record.value, entry.expiry_ms)))
}

/// Lock-free segment roll. Operates on already-held guard state and the
/// caller's WAL guard — it never calls `self.wal.lock()`, so it can neither
/// re-enter the WAL mutex (the latent `ttl`-rolls self-deadlock) nor invert
/// the canonical order against `compact` (the old `set`-rolls ABBA).
fn roll_active(
    wal: &mut Wal,
    active_file: &mut ActiveFileState,
    active_segment: &mut Segment,
    db_path: &str,
    db_name: &str,
    fsync_strategy: FSyncStrategy,
    segment_count: &AtomicUsize,
) -> io::Result<()> {
    // Ensure the old segment is durable before resetting the WAL.
    active_file.file.sync_all()?;
    active_file.fsync_handle.take();

    let segment = Segment::new(db_name).map_err(io::Error::other)?;
    let file: File = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(segment.path(db_path))?;

    let segment_path = segment.path(db_path);
    *active_file = ActiveFileState {
        file,
        fsync_handle: KVEngine::spawn_fsync_worker(fsync_strategy, segment_path),
    };
    *active_segment = segment;
    wal.reset()?;
    segment_count.fetch_add(1, Relaxed);
    Ok(())
}

/// Lock-free value write: WAL append → segment append (rolling first if the
/// active segment would overflow) → index update. Borrows guard state directly,
/// so the whole read-modify-write of `ttl` can run under one held lock span
/// without any helper re-locking `self`.
///
/// Returns `(new_record_size, replaced_old_record_size)` so the caller can
/// update the dead/total byte counters *after* releasing the locks.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kv_append(
    wal: &mut Wal,
    active_file: &mut ActiveFileState,
    active_segment: &mut Segment,
    index: &mut HashIndex,
    db_path: &str,
    db_name: &str,
    max_segment_bytes: u64,
    fsync_strategy: FSyncStrategy,
    segment_count: &AtomicUsize,
    key: &str,
    value: &str,
    expiry_ms: Option<u64>,
) -> io::Result<(u64, Option<u64>)> {
    wal.append(key.to_string(), value.to_string(), false, expiry_ms)?;

    let record = Record {
        header: RecordHeader {
            crc32: 0u32,
            key_size: key.len() as u64,
            value_size: value.len() as u64,
            flags: if expiry_ms.is_some() {
                FLAG_HAS_EXPIRY
            } else {
                0
            },
            expiry_ms,
        },
        key: key.to_string(),
        value: value.to_string(),
    };

    let seg_size = active_file.file.seek(SeekFrom::End(0))?;
    if max_segment_bytes < seg_size + record.size_on_disk() {
        roll_active(
            wal,
            active_file,
            active_segment,
            db_path,
            db_name,
            fsync_strategy,
            segment_count,
        )?;
    }

    let offset = record.append(&mut active_file.file)?;
    let timestamp = active_segment.timestamp;
    let new_size = record.size_on_disk();

    let replaced = index
        .set(key.to_string(), offset, timestamp, new_size, expiry_ms)
        .map(|old| old.record_size);

    Ok((new_size, replaced))
}

impl KVEngine {
    /// Creates a new, empty database in the given directory.
    ///
    /// Creates `db_path` if it does not exist, opens a fresh segment file,
    /// and returns a `DB` ready for reads and writes.
    pub fn new(
        db_path: &str,
        db_name: &str,
        max_segment_bytes: u64,
        fsync_strategy: FSyncStrategy,
    ) -> io::Result<KVEngine> {
        std::fs::create_dir_all(db_path)?;
        let segment = Segment::new(db_name).map_err(io::Error::other)?;
        let file: File = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(segment.path(db_path))?;
        let fsync_handle = Self::spawn_fsync_worker(fsync_strategy, segment.path(db_path));
        Ok(KVEngine {
            index: RwLock::new(HashIndex::new()),
            active_file: Mutex::new(ActiveFileState { file, fsync_handle }),
            db_path: db_path.to_string(),
            db_name: db_name.to_string(),
            active_segment: Mutex::new(segment),
            max_segment_bytes: AtomicU64::from(max_segment_bytes),
            writes_since_fsync: AtomicU64::from(0),
            fsync_strategy,
            dead_bytes: AtomicU64::from(0),
            total_bytes: AtomicU64::from(0),
            segment_count: AtomicUsize::from(1),
            wal: Mutex::new(Wal::open(&PathBuf::from(db_path), db_name.to_string())?),
        })
    }

    /// Reopens an existing database from disk.
    ///
    /// Scans `db_dir` for segment files matching `db_name`, rebuilds the
    /// in-memory index (from hint files when available, otherwise by scanning
    /// records), and returns a `DB` positioned at the latest segment.
    ///
    /// Returns `Ok(None)` if the directory does not exist or contains no
    /// matching segments.
    pub fn from_dir(
        db_dir: &str,
        db_name: &str,
        max_segment_bytes: u64,
        fsync_strategy: FSyncStrategy,
    ) -> Result<Option<KVEngine>, Error> {
        let segments = match get_segments(db_dir, db_name) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        if segments.is_empty() {
            return Ok(None);
        }

        let mut hash_index = HashIndex::new();
        let mut current_file = None;
        let mut active_segment = None;
        let mut size = 0;

        for segment in segments.iter() {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(segment.path(db_dir))?;
            match Hint::read_file(segment.hint_path(db_dir)) {
                Ok(hints) => {
                    hints.iter().for_each(|entry| {
                        if !entry.tombstone {
                            hash_index.set(
                                entry.key.clone(),
                                entry.offset,
                                segment.timestamp,
                                0,
                                entry.expiry_ms,
                            );
                        }
                    });
                }
                Err(_) => {
                    hash_index.merge_from_file(&mut file, segment.timestamp)?;
                }
            }
            current_file = Some(file);
            active_segment = Some(segment);
            size += std::fs::metadata(segment.path(db_dir))?.len();
        }
        let mut wal = Wal::open(&PathBuf::from(db_dir), db_name.to_string())?;
        let memtable = wal.replay()?;
        let file = current_file.as_mut().unwrap();
        let segment = active_segment.unwrap();
        for (key, entry) in memtable.entries() {
            match entry.value.as_deref() {
                Some(val) => {
                    let record = Record {
                        header: RecordHeader {
                            crc32: 0u32,
                            key_size: key.len() as u64,
                            value_size: val.len() as u64,
                            flags: if entry.expiry_ms.is_some() {
                                FLAG_HAS_EXPIRY
                            } else {
                                0u8
                            },
                            expiry_ms: entry.expiry_ms,
                        },
                        key: key.to_string(),
                        value: val.to_string(),
                    };
                    let offset = record.append(file)?;
                    hash_index.set(
                        key.to_string(),
                        offset,
                        segment.timestamp,
                        record.size_on_disk(),
                        entry.expiry_ms,
                    );
                    size += record.size_on_disk();
                }
                None => {
                    let record = Record {
                        header: RecordHeader {
                            crc32: 0u32,
                            key_size: key.len() as u64,
                            value_size: 0,
                            flags: FLAG_TOMBSTONE,
                            expiry_ms: None,
                        },
                        key: key.to_string(),
                        value: String::new(),
                    };
                    record.append(file)?;
                    hash_index.delete(key);
                    size += record.size_on_disk();
                }
            }
        }

        wal.reset()?;
        let segment = active_segment.unwrap().to_owned();
        let fsync_handle = Self::spawn_fsync_worker(fsync_strategy, segment.path(db_dir));
        Ok(Some(KVEngine {
            index: RwLock::new(hash_index),
            active_file: Mutex::new(ActiveFileState {
                file: current_file.unwrap(),
                fsync_handle,
            }),
            db_path: db_dir.to_string(),
            db_name: db_name.to_string(),
            active_segment: Mutex::new(Segment {
                segment_name: segment.segment_name,
                timestamp: segment.timestamp,
            }),
            max_segment_bytes: AtomicU64::from(max_segment_bytes),
            writes_since_fsync: AtomicU64::from(0),
            fsync_strategy,
            dead_bytes: AtomicU64::from(0),
            total_bytes: AtomicU64::from(size),
            segment_count: AtomicUsize::from(segments.len()),
            wal: Mutex::new(wal),
        }))
    }

    /// Builds a compacted KVEngine from the current state, returning
    /// the new engine without modifying self.
    fn build_compacted(&self) -> Result<(KVEngine, Vec<Segment>), Error> {
        let old_segments = get_segments(&self.db_path, &self.db_name)?;
        let new_db = KVEngine::new(
            &self.db_path,
            &self.db_name,
            self.max_segment_bytes.load(Relaxed),
            self.fsync_strategy,
        )?;
        let now = now_ms();
        let keys_with_expiry: Vec<(String, Option<u64>)> = {
            let index_lock = self.index.read().unwrap();
            index_lock
                .ls_keys()
                .filter_map(|k| {
                    let entry = index_lock.get(k).unwrap();
                    // Drop expired keys during compaction
                    if entry.expiry_ms.is_some_and(|e| is_expired(e, now)) {
                        None
                    } else {
                        Some((k.clone(), entry.expiry_ms))
                    }
                })
                .collect()
        };

        for (k, expiry_ms) in &keys_with_expiry {
            let value = match self.get(k)? {
                Some((_, value)) => value,
                None => continue,
            };
            new_db.set_with_ttl(k, &value, *expiry_ms)?;
        }

        {
            let file_lock = new_db.active_file.lock().unwrap();
            file_lock.file.sync_all()?;
        }

        // Write hint files for the new segments (filter out old ones).
        {
            let new_index_lock = new_db.index.read().unwrap();
            let new_segments: Vec<_> = get_segments(&self.db_path, &self.db_name)?
                .into_iter()
                .filter(|s| !old_segments.iter().any(|o| o.timestamp == s.timestamp))
                .collect();
            for segment in &new_segments {
                let hint_entries: Vec<HintEntry> = new_index_lock
                    .ls_keys()
                    .filter_map(|k| {
                        let entry = new_index_lock.get(k).unwrap();
                        if entry.segment_timestamp == segment.timestamp {
                            Some(HintEntry {
                                key_size: k.len() as u64,
                                offset: entry.offset,
                                tombstone: false,
                                expiry_ms: entry.expiry_ms,
                                key: k.clone(),
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                Hint::write_file(segment.hint_path(&self.db_path), &hint_entries)?;
            }
        }

        // Don't delete old segments here — compact() deletes them
        // after swapping the index, so concurrent readers are safe.
        Ok((new_db, old_segments))
    }

    /// Spawns a background worker that periodically fsyncs the given segment file.
    ///
    /// Returns `None` if the fsync strategy is not `Periodic`.
    fn spawn_fsync_worker(
        fsync_strategy: FSyncStrategy,
        segment_path: PathBuf,
    ) -> Option<BackgroundWorker> {
        if let FSyncStrategy::Periodic(duration) = fsync_strategy {
            let job = move || {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&segment_path)
                    .unwrap();
                file.sync_all().unwrap();
            };
            Some(BackgroundWorker::spawn(duration, job))
        } else {
            None
        }
    }

    /// Flushes writes to disk according to the configured `FSyncStrategy`.
    fn fsync(&self) -> io::Result<()> {
        self.writes_since_fsync.fetch_add(1, Relaxed);
        match self.fsync_strategy {
            FSyncStrategy::Always => {
                self.writes_since_fsync.store(0, Relaxed);
                self.active_file.lock().unwrap().file.sync_all()?
            }
            FSyncStrategy::Never => {}
            FSyncStrategy::EveryN(n) => {
                if n <= self.writes_since_fsync.load(Relaxed) as usize {
                    self.writes_since_fsync.store(0, Relaxed);
                    self.active_file.lock().unwrap().file.sync_all()?
                }
            }
            FSyncStrategy::Periodic(_) => {}
        }

        Ok(())
    }
}

impl StorageEngine for KVEngine {
    /// Compacts the database by rewriting only live key-value pairs into
    /// fresh segments, then deleting the old segment and hint files.
    fn compact(&self) -> Result<(), Error> {
        let (new_db, old_segments) = self.build_compacted()?;

        let mut wal = self.wal.lock().unwrap();
        let mut file_state = self.active_file.lock().unwrap();
        let mut segment_guard = self.active_segment.lock().unwrap();
        let mut index_guard = self.index.write().unwrap();

        *file_state = new_db.active_file.into_inner().unwrap();
        *segment_guard = new_db.active_segment.into_inner().unwrap();
        *index_guard = new_db.index.into_inner().unwrap();

        self.writes_since_fsync.store(0, Relaxed);
        self.segment_count.store(1, Relaxed);

        wal.reset()?;

        // Drop all locks before file deletion — the index now points
        // to new segments, so no reader will reference old files.
        drop(index_guard);
        drop(segment_guard);
        drop(file_state);
        drop(wal);

        for segment in &old_segments {
            fs::remove_file(segment.path(&self.db_path))?;
            let _ = fs::remove_file(segment.hint_path(&self.db_path));
        }

        Ok(())
    }

    /// Retrieves the key-value pair associated with the given key.
    ///
    /// Looks up the key in the in-memory index, seeks to the record's byte
    /// offset in the appropriate segment file, and reads the full record
    /// (verifying its CRC32 checksum).
    ///
    /// Returns `Ok(None)` if the key is not in the index.
    fn get(&self, key: &str) -> Result<Option<(String, String)>, Error> {
        let now = now_ms();
        // Snapshot the active segment, then drop its lock; hold the index
        // read lock across kv_lookup's file I/O so compact() cannot swap the
        // index and delete old segment files mid-read.
        let active_segment = self.active_segment.lock().unwrap().clone();
        let index = self.index.read().unwrap();
        match kv_lookup(
            &index,
            &self.db_path,
            &self.db_name,
            &active_segment,
            key,
            now,
        )? {
            Some((value, _)) => Ok(Some((key.to_string(), value))),
            None => Ok(None),
        }
    }

    /// Inserts or updates a key-value pair in the database.
    ///
    /// Appends a record to the active segment file and updates the in-memory
    /// index. If the active segment would exceed `max_segment_bytes`, a new
    /// segment is rolled first.
    ///
    /// Previous entries for the same key become dead bytes, reclaimable by
    /// compaction.
    fn set(&self, key: &str, value: &str) -> Result<(), Error> {
        if key.len() > MAX_KEY_SIZE || value.len() > MAX_VALUE_SIZE {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "key or value exceeds maximum allowed size",
            ));
        }

        // Canonical lock order: wal → active_file → active_segment → index.
        // Held across kv_append (incl. any roll, which reuses the wal guard).
        let (new_size, replaced) = {
            let mut wal = self.wal.lock().unwrap();
            let mut active_file = self.active_file.lock().unwrap();
            let mut active_segment = self.active_segment.lock().unwrap();
            let mut index = self.index.write().unwrap();
            kv_append(
                &mut wal,
                &mut active_file,
                &mut active_segment,
                &mut index,
                &self.db_path,
                &self.db_name,
                self.max_segment_bytes.load(Relaxed),
                self.fsync_strategy,
                &self.segment_count,
                key,
                value,
                None,
            )?
        };

        if let Some(old_size) = replaced {
            self.dead_bytes.fetch_add(old_size, Relaxed);
        }
        self.total_bytes.fetch_add(new_size, Relaxed);
        self.fsync()?;

        Ok(())
    }

    /// Deletes a key from the database.
    ///
    /// Removes the key from the in-memory index and appends a tombstone
    /// record to the active segment. Returns `Ok(None)` if the key was not
    /// present.
    ///
    /// Concurrency: `delete` acquires `wal`, `index`, and `active_file` in
    /// three independent acquire-use-drop blocks and never holds two of them
    /// simultaneously, so the canonical lock order (which governs only nested
    /// holds) does not apply here — there is no ordering or atomicity hazard
    /// against `compact`/`ttl`. It is therefore intentionally left outside the
    /// kv_lookup/kv_append orchestration.
    fn delete(&self, key: &str) -> Result<Option<()>, Error> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "key or value exceeds maximum allowed size",
            ));
        }

        {
            let mut wal = self.wal.lock().unwrap();
            wal.append(key.to_string(), String::new(), true, None)?;
        }

        let old = {
            let mut index = self.index.write().unwrap();
            index.delete(key)
        };
        match old {
            Some(entry) => {
                let record = Record {
                    header: RecordHeader {
                        crc32: 0u32,
                        key_size: key.len() as u64,
                        value_size: 0,
                        flags: FLAG_TOMBSTONE,
                        expiry_ms: None,
                    },
                    key: key.to_string(),
                    value: String::new(),
                };
                {
                    let mut active_file = self.active_file.lock().unwrap();
                    record.append(&mut active_file.file)?;
                }
                self.fsync()?;
                self.dead_bytes
                    .fetch_add(entry.record_size + record.size_on_disk(), Relaxed);
                self.total_bytes.fetch_add(record.size_on_disk(), Relaxed);

                Ok(Some(()))
            }
            None => Ok(None),
        }
    }

    fn dead_bytes(&self) -> u64 {
        self.dead_bytes.load(Relaxed)
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Relaxed)
    }

    fn segment_count(&self) -> usize {
        self.segment_count.load(Relaxed)
    }

    fn list_keys(&self) -> io::Result<Vec<String>> {
        let now = now_ms();
        let index = self.index.read().unwrap();
        Ok(index
            .ls_keys()
            .filter(|k| {
                !index
                    .get(k)
                    .is_some_and(|e| e.expiry_ms.is_some_and(|exp| is_expired(exp, now)))
            })
            .cloned()
            .collect())
    }

    fn exists(&self, key: &str) -> bool {
        let index = self.index.read().unwrap();
        match index.get(key) {
            Some(e) => !e.expiry_ms.is_some_and(|exp| is_expired(exp, now_ms())),
            None => false,
        }
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
        // Atomic read-modify-write. Canonical lock order
        // (wal → active_file → active_segment → index) held across BOTH
        // kv_lookup and kv_append, so no concurrent writer can slip between
        // the read and the re-append (the lost-update / resurrection race).
        let now = now_ms();
        let written = {
            let mut wal = self.wal.lock().unwrap();
            let mut active_file = self.active_file.lock().unwrap();
            let mut active_segment = self.active_segment.lock().unwrap();
            let mut index = self.index.write().unwrap();

            match kv_lookup(
                &index,
                &self.db_path,
                &self.db_name,
                &active_segment,
                key,
                now,
            )? {
                None => None,
                Some((value, _)) => Some(kv_append(
                    &mut wal,
                    &mut active_file,
                    &mut active_segment,
                    &mut index,
                    &self.db_path,
                    &self.db_name,
                    self.max_segment_bytes.load(Relaxed),
                    self.fsync_strategy,
                    &self.segment_count,
                    key,
                    &value,
                    expiry_ms,
                )?),
            }
        };

        match written {
            None => Ok(TtlOutcome::NotFound),
            Some((new_size, replaced)) => {
                if let Some(old_size) = replaced {
                    self.dead_bytes.fetch_add(old_size, Relaxed);
                }
                self.total_bytes.fetch_add(new_size, Relaxed);
                self.fsync()?;
                if expiry_ms.is_some() {
                    Ok(TtlOutcome::Set)
                } else {
                    Ok(TtlOutcome::Persisted)
                }
            }
        }
    }

    fn set_with_ttl(&self, key: &str, value: &str, expiry_ms: Option<u64>) -> io::Result<()> {
        if key.len() > MAX_KEY_SIZE || value.len() > MAX_VALUE_SIZE {
            return Err(Error::new(
                std::io::ErrorKind::InvalidInput,
                "key or value exceeds maximum allowed size",
            ));
        }

        // Canonical lock order: wal → active_file → active_segment → index.
        let (new_size, replaced) = {
            let mut wal = self.wal.lock().unwrap();
            let mut active_file = self.active_file.lock().unwrap();
            let mut active_segment = self.active_segment.lock().unwrap();
            let mut index = self.index.write().unwrap();
            kv_append(
                &mut wal,
                &mut active_file,
                &mut active_segment,
                &mut index,
                &self.db_path,
                &self.db_name,
                self.max_segment_bytes.load(Relaxed),
                self.fsync_strategy,
                &self.segment_count,
                key,
                value,
                expiry_ms,
            )?
        };

        if let Some(old_size) = replaced {
            self.dead_bytes.fetch_add(old_size, Relaxed);
        }
        self.total_bytes.fetch_add(new_size, Relaxed);
        self.fsync()?;

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
        let (value, new_size, replaced) = {
            let mut wal = self.wal.lock().unwrap();
            let mut active_file = self.active_file.lock().unwrap();
            let mut active_segment = self.active_segment.lock().unwrap();
            let mut index = self.index.write().unwrap();
            let (current, expiry_ms) = match kv_lookup(
                &index,
                &self.db_path,
                &self.db_name,
                &active_segment,
                key,
                now,
            )? {
                Some((v, ttl)) => (
                    v.parse::<i64>()
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                    ttl,
                ),
                None => (0, None),
            };
            let next = current.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "increment would overflow")
            })?;
            let (new_size, replaced) = kv_append(
                &mut wal,
                &mut active_file,
                &mut active_segment,
                &mut index,
                &self.db_path,
                &self.db_name,
                self.max_segment_bytes.load(Relaxed),
                self.fsync_strategy,
                &self.segment_count,
                key,
                &next.to_string(),
                expiry_ms,
            )?;
            (next, new_size, replaced)
        };

        if let Some(old_size) = replaced {
            self.dead_bytes.fetch_add(old_size, Relaxed);
        }
        self.total_bytes.fetch_add(new_size, Relaxed);
        self.fsync()?;

        Ok(value)
    }
}
