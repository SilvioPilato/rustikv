use std::{
    fs::{File, OpenOptions, read_dir},
    io::{self, BufReader, ErrorKind, Read, Seek, Write},
    path::PathBuf,
    time::SystemTime,
};

use crate::{
    block::{BlockHeader, BlockReader, BlockWriter},
    bloom::BloomFilter,
    memtable::Memtable,
    record::{Record, RecordHeader},
};

const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASH_COUNT: u32 = 7;

pub struct SSTable {
    pub path: PathBuf,
    pub timestamp: u64, // for ordering segments newest-to-oldest
    name: String,
    pub level: Option<usize>,
    sparse_index: Vec<(String, u64)>,
    pub bloom: BloomFilter,
    min: Option<(String, u64)>,
    max: Option<(String, u64)>,
}

pub struct SSTableIter {
    file: BufReader<File>,
    current_block: Vec<u8>,
    block_pos: usize,
    done: bool,
}

impl Iterator for SSTableIter {
    type Item = io::Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.block_pos < self.current_block.len() {
                let mut slice = &self.current_block[self.block_pos..];
                let before = slice.len();
                match Record::read_next(&mut slice) {
                    Ok(record) => {
                        self.block_pos += before - slice.len();
                        return Some(Ok(record));
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        self.block_pos = self.current_block.len();
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
            }

            let mut header_bytes = [0u8; 9];
            match self.file.read_exact(&mut header_bytes) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
            let header = BlockHeader::from_bytes(&header_bytes);
            match BlockReader::read_block(&mut self.file, &header) {
                Ok(data) => {
                    self.current_block = data;
                    self.block_pos = 0;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl SSTable {
    /// Flush a memtable to disk as a sorted segment file
    pub fn from_memtable(
        dir: &str,
        name: &str,
        memtable: &Memtable,
        level: Option<usize>,
        target_block_size: usize,
        compression_enabled: bool,
    ) -> io::Result<SSTable> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| io::Error::other("SystemTime error"))?
            .as_nanos() as u64;
        let filename = match level {
            Some(l) => format!("{}_L{}_{}.sst", name, l, timestamp),
            None => format!("{}_{}.sst", name, timestamp),
        };
        let path = PathBuf::from(dir).join(filename);

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut sparse_index: Vec<(String, u64)> = Vec::new();
        let key_count = memtable.entries().len();
        let bloom_bytes = (key_count * BLOOM_BITS_PER_KEY).div_ceil(8);
        let mut bloom = BloomFilter::new(bloom_bytes.max(1), BLOOM_HASH_COUNT);
        let mut last_key: Option<String> = None;
        let mut block_writer = BlockWriter::new(target_block_size, compression_enabled);
        let mut file_offset = 0u64;
        let mut first_key_in_block = None;

        for (key, opt) in memtable.entries().iter() {
            let (value, tombstone) = match opt {
                Some(v) => (v.to_string(), false),
                None => (String::new(), true),
            };
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            let record = Record {
                header: RecordHeader {
                    crc32: 0u32,
                    key_size: key.len() as u64,
                    value_size: value.len() as u64,
                    tombstone,
                },
                key: key.to_string(),
                value,
            };

            bloom.insert(key);
            last_key = Some(key.clone());
            if let Some(block_bytes) = block_writer.add_record(&record)? {
                sparse_index.push((first_key_in_block.take().unwrap(), file_offset));
                file.write_all(&block_bytes)?;
                file_offset += block_bytes.len() as u64;
                first_key_in_block = Some(key.clone());
            }
        }

        if let Some(block_bytes) = block_writer.flush()? {
            if let Some(ref key) = first_key_in_block {
                sparse_index.push((key.to_owned(), file_offset));
            }
            file.write_all(&block_bytes)?;
        }
        let min = sparse_index.first().cloned();
        let max = last_key.map(|k| {
            let pos = sparse_index.partition_point(|(sk, _)| sk.as_str() <= k.as_str());
            let offset = if pos > 0 { sparse_index[pos - 1].1 } else { 0 };
            (k, offset)
        });
        Ok(SSTable {
            path,
            timestamp,
            name: name.to_string(),
            level,
            sparse_index,
            bloom,
            min,
            max,
        })
    }

    /// Scan the file for a key, return the value (or None/tombstone)
    pub fn get(&self, key: &str) -> io::Result<Option<Option<String>>> {
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }
        let offset = self.get_offset(key);

        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(io::SeekFrom::Start(offset))?;

        let iter = SSTableIter {
            block_pos: 0,
            file: reader,
            current_block: Vec::new(),
            done: false,
        };

        for result in iter {
            let record = result?;
            match record.key.as_str().cmp(key) {
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Equal => {
                    let value = if record.header.tombstone {
                        None
                    } else {
                        Some(record.value)
                    };
                    return Ok(Some(value));
                }
                std::cmp::Ordering::Less => continue,
            }
        }
        Ok(None)
    }

    /// Iterate all records in order (for compaction merging)
    pub fn iter(&self) -> io::Result<SSTableIter> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        Ok(SSTableIter {
            file: BufReader::new(file),
            block_pos: 0,
            current_block: Vec::new(),
            done: false,
        })
    }

    /// Parses an SSTable filename into a stub `SSTable`.
    /// **Caller must call `rebuild_index` before use** — `sparse_index`, `bloom`,
    /// `min`, and `max` are all uninitialised placeholders until then.
    pub fn parse(filename: &str) -> Option<Self> {
        let stem = filename.strip_suffix(".sst")?;
        let (name_and_maybe_level, ts) = stem.rsplit_once('_')?;
        let timestamp: u64 = ts.parse().ok()?;
        // Try to extract level: {name}_L{level}_{timestamp}.sst
        let (name, level) = if let Some((n, l)) = name_and_maybe_level.rsplit_once("_L") {
            match l.parse::<usize>() {
                Ok(lvl) => (n.to_string(), Some(lvl)),
                Err(_) => (name_and_maybe_level.to_string(), None),
            }
        } else {
            (name_and_maybe_level.to_string(), None)
        };
        let path = PathBuf::new();
        let sparse_index: Vec<(String, u64)> = Vec::new();
        let bloom = BloomFilter::new(1, BLOOM_HASH_COUNT);
        Some(Self {
            path,
            timestamp,
            name,
            level,
            sparse_index,
            bloom,
            min: None,
            max: None,
        })
    }

    /// Rebuild the sparse index and Bloom filter by scanning all records.
    pub fn rebuild_index(&mut self) -> io::Result<()> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut sparse_index: Vec<(String, u64)> = Vec::new();
        let mut keys: Vec<String> = Vec::new();
        loop {
            let block_offset = reader.stream_position()?;

            let mut header_buf = [0u8; 9];
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let header = BlockHeader::from_bytes(&header_buf);
            let block_data = BlockReader::read_block(&mut reader, &header)?;
            let mut slice = block_data.as_slice();
            let mut first_key: Option<String> = None;

            loop {
                match Record::read_next(&mut slice) {
                    Ok(record) => {
                        if first_key.is_none() {
                            first_key = Some(record.key.clone());
                        }
                        keys.push(record.key);
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
            }

            if let Some(k) = first_key {
                sparse_index.push((k, block_offset));
            }
        }
        let bloom_bytes = (keys.len() * BLOOM_BITS_PER_KEY).div_ceil(8);
        let mut bloom = BloomFilter::new(bloom_bytes.max(1), BLOOM_HASH_COUNT);
        let mut last_key: Option<&String> = None;
        for key in &keys {
            bloom.insert(key);
            last_key = Some(key);
        }

        self.min = sparse_index.first().cloned();
        self.max = last_key.map(|k| {
            let pos = sparse_index.partition_point(|(sk, _)| sk.as_str() <= k.as_str());
            let offset = if pos > 0 { sparse_index[pos - 1].1 } else { 0 };
            (k.clone(), offset)
        });
        self.sparse_index = sparse_index;
        self.bloom = bloom;
        Ok(())
    }

    fn get_offset(&self, key: &str) -> u64 {
        let pos = self
            .sparse_index
            .partition_point(|(k, _)| k.as_str() <= key);
        if pos > 0 {
            self.sparse_index[pos - 1].1 // seek to this offset
        } else {
            0 // start of file
        }
    }

    pub fn get_min(&self) -> &Option<(String, u64)> {
        &self.min
    }

    pub fn get_max(&self) -> &Option<(String, u64)> {
        &self.max
    }

    pub fn size_on_disk(&self) -> io::Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }
}

pub fn get_sstables(dir: &str, db_name: &str) -> io::Result<Vec<SSTable>> {
    let mut tables: Vec<SSTable> = read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().to_string();
            let mut table = SSTable::parse(&name)?;
            table.path = PathBuf::from(dir).join(&name);
            if table.name == db_name {
                Some(table)
            } else {
                None
            }
        })
        .collect();
    tables.sort_by_key(|s| s.timestamp);
    for table in &mut tables {
        table.rebuild_index()?;
    }
    Ok(tables)
}
