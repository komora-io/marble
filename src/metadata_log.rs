// A stripped-down tiny-lsm. Could be made much simpler, but
// this works.
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, prelude::*, BufReader, BufWriter, Result};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use fault_injection::fallible;

const LOG_PATH: &str = "metadata_log";
const TABLE_DIR: &str = "metadata_tables";
const U64_SZ: usize = std::mem::size_of::<u64>();
const K: usize = 8;
const V: usize = 8;

#[derive(Default)]
struct Queue<T> {
    mu: std::sync::Mutex<std::collections::VecDeque<T>>,
    cv: std::sync::Condvar,
}

impl<T> Queue<T> {
    fn recv(&self) -> T {
        let mut q = self.mu.lock().unwrap();

        while q.is_empty() {
            q = self.cv.wait(q).unwrap();
        }

        q.pop_back().unwrap()
    }

    fn send(&self, item: T) {
        let mut q = self.mu.lock().unwrap();
        q.push_front(item);
        drop(q);
        self.cv.notify_all();
    }
}

#[derive(Default)]
struct Waiter {
    done: std::sync::atomic::AtomicBool,
    mu: std::sync::Mutex<()>,
    cv: std::sync::Condvar,
}

impl Waiter {
    fn wait(self: Arc<Waiter>) {
        if self.done.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let mut mu = self.mu.lock().unwrap();
        while !self.done.load(std::sync::atomic::Ordering::Acquire) {
            mu = self.cv.wait(mu).unwrap();
        }
    }

    fn finished(self: Arc<Waiter>) {
        let _mu = self.mu.lock().unwrap();
        self.done.store(true, std::sync::atomic::Ordering::Release);
        self.cv.notify_all();
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy)]
pub struct MetadataLogConfig {
    /// When the log file exceeds this size, a new
    /// compressed and compacted table will be flushed
    /// to disk and the log file will be truncated.
    pub max_log_length: usize,
    /// When the background compactor thread looks for
    /// contiguous ranges of tables to merge, it will
    /// require all tables to be at least
    /// 1/`merge_ratio` * the size of the first table
    /// in the contiguous window under consideration.
    pub merge_ratio: u8,
    /// When the background compactor thread looks for
    /// ranges of tables to merge, it will require
    /// ranges to be at least this long.
    pub merge_window: u8,
    /// All inserts go directly to a `BufWriter` wrapping
    /// the log file. This option determines how large
    /// that in-memory buffer is.
    pub log_bufwriter_size: u32,
}

impl Default for MetadataLogConfig {
    fn default() -> MetadataLogConfig {
        MetadataLogConfig {
            max_log_length: 32 * 1024 * 1024,
            merge_ratio: 3,
            merge_window: 5,
            log_bufwriter_size: 1024 * 1024,
        }
    }
}

struct WorkerStats {
    read_bytes: AtomicU64,
    written_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub resident_bytes: u64,
    pub on_disk_bytes: u64,
    pub logged_bytes: u64,
    pub written_bytes: u64,
    pub read_bytes: u64,
    pub space_amp: f64,
    pub write_amp: f64,
}

fn hash(k: &u64, v: &Option<u64>) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[v.is_some() as u8]);
    hasher.update(&k.to_be_bytes());

    if let Some(v) = v {
        hasher.update(&v.to_be_bytes());
    } else {
        hasher.update(&[0; V]);
    }

    // we XOR the hash to make sure it's something other
    // than 0 when empty, because 0 is an easy value to
    // create accidentally or via corruption.
    hasher.finalize() ^ 0xFF
}

#[inline]
fn hash_batch_len(len: usize) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&(len as u64).to_le_bytes());

    hasher.finalize() ^ 0xFF
}

enum WorkerMessage {
    NewT { id: u64, sst_sz: u64 },
    Stop(Arc<Waiter>),
    Heartbeat(Arc<Waiter>),
}

struct Worker {
    table_directory: BTreeMap<u64, u64>,
    inbox: Arc<Queue<WorkerMessage>>,
    path: PathBuf,
    config: MetadataLogConfig,
    stats: Arc<WorkerStats>,
}

impl Worker {
    fn run(mut self) {
        while self.tick() {}
        log::info!("tiny-metadata_store compaction worker quitting");
    }

    fn tick(&mut self) -> bool {
        let message = self.inbox.recv();
        if !self.handle_message(message) {
            return false;
        }

        // only compact one run at a time before checking
        // for new messages.
        if let Err(e) = self.table_maintenance() {
            log::error!("error while compacting tables in the background: {:?}", e);
        }

        true
    }

    fn handle_message(&mut self, message: WorkerMessage) -> bool {
        match message {
            WorkerMessage::NewT { id, sst_sz } => {
                self.table_directory.insert(id, sst_sz);
                true
            }
            WorkerMessage::Stop(waiter) => {
                waiter.finished();
                false
            }
            WorkerMessage::Heartbeat(waiter) => {
                waiter.finished();
                true
            }
        }
    }

    fn table_maintenance(&mut self) -> Result<()> {
        let on_disk_size: u64 = self.table_directory.values().sum();

        log::debug!("disk size: {}", on_disk_size);
        let max_tables = self.config.merge_ratio as usize * self.config.merge_window as usize;
        if self.table_directory.len() > max_tables {
            log::debug!(
                "performing full compaction, decompressed on-disk database size has grown beyond \
                 {}x the in-memory size",
                max_tables
            );
            let run_to_compact: Vec<u64> = self.table_directory.keys().copied().collect();

            return self.compact_table_run(&run_to_compact);
        }

        if self.table_directory.len() < self.config.merge_window.max(2) as usize {
            return Ok(());
        }

        for window in self
            .table_directory
            .iter()
            .collect::<Vec<_>>()
            .windows(self.config.merge_window.max(2) as usize)
        {
            if window
                .iter()
                .skip(1)
                .all(|w| *w.1 * self.config.merge_ratio as u64 > *window[0].1)
            {
                let run_to_compact: Vec<u64> = window.into_iter().map(|(id, _sum)| **id).collect();

                self.compact_table_run(&run_to_compact)?;
                return Ok(());
            }
        }

        Ok(())
    }

    // This function must be able to crash at any point
    // without leaving the system in an unrecoverable
    // state, or without losing data. This function must
    // be nullipotent from the external API surface's
    // perspective.
    fn compact_table_run(&mut self, table_ids: &[u64]) -> Result<()> {
        log::debug!(
            "trying to compact table_ids {:?}",
            table_ids
                .iter()
                .map(|id| id_format(*id))
                .collect::<Vec<_>>()
        );

        let mut map = HashMap::new();

        let mut read_pairs = 0;

        for table_id in table_ids {
            for (k, v) in read_table(&self.path, *table_id)? {
                map.insert(k, v);
                read_pairs += 1;
            }
        }

        self.stats
            .read_bytes
            .fetch_add(read_pairs * (4 + 1 + K + V) as u64, Ordering::Relaxed);

        let sst_id = table_ids
            .iter()
            .max()
            .expect("compact_table_run called with empty set of sst ids");

        write_table(&self.path, *sst_id, &map, true)?;

        self.stats
            .written_bytes
            .fetch_add(map.len() as u64 * (4 + 1 + K + V) as u64, Ordering::Relaxed);

        let sst_sz = map.len() as u64 * (4 + K + V) as u64;
        self.table_directory.insert(*sst_id, sst_sz);

        log::debug!("compacted range into table {}", id_format(*sst_id));

        for table_id in table_ids {
            if table_id == sst_id {
                continue;
            }
            fallible!(fs::remove_file(
                self.path.join(TABLE_DIR).join(id_format(*table_id))
            ));
            self.table_directory
                .remove(table_id)
                .expect("compacted sst not present in table_directory");
        }
        let parent = fallible!(fs::File::open(self.path.join(TABLE_DIR)));
        fallible!(parent.sync_all());

        Ok(())
    }
}

fn id_format(id: u64) -> String {
    format!("{:016x}", id)
}

fn list_tables(path: &Path, remove_tmp: bool) -> Result<BTreeMap<u64, u64>> {
    let mut table_map = BTreeMap::new();

    for dir_entry_res in fallible!(fs::read_dir(path.join(TABLE_DIR))) {
        let dir_entry = fallible!(dir_entry_res);
        let file_name = if let Ok(f) = dir_entry.file_name().into_string() {
            f
        } else {
            continue;
        };

        if let Ok(id) = u64::from_str_radix(&file_name, 16) {
            let metadata = fallible!(dir_entry.metadata());

            table_map.insert(id, metadata.len());
        } else {
            if remove_tmp && file_name.ends_with("-tmp") {
                log::warn!("removing incomplete table rewrite {}", file_name);
                fallible!(fs::remove_file(path.join(TABLE_DIR).join(file_name)));
            }
        }
    }

    Ok(table_map)
}

fn write_table(
    path: &Path,
    id: u64,
    items: &HashMap<u64, Option<u64>>,
    tmp_mv: bool,
) -> Result<()> {
    let sst_dir_path = path.join(TABLE_DIR);
    let sst_path = if tmp_mv {
        sst_dir_path.join(format!("{:x}-tmp", id))
    } else {
        sst_dir_path.join(id_format(id))
    };

    let file = fallible!(fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&sst_path));

    let mut bw = BufWriter::new(file);

    fallible!(bw.write_all(&(items.len() as u64).to_le_bytes()));

    for (k, v) in items {
        let crc: u32 = hash(k, v);
        fallible!(bw.write_all(&crc.to_le_bytes()));
        fallible!(bw.write_all(&[v.is_some() as u8]));
        fallible!(bw.write_all(&k.to_be_bytes()));

        if let Some(v) = v {
            fallible!(bw.write_all(&v.to_be_bytes()));
        } else {
            fallible!(bw.write_all(&[0; V]));
        }
    }

    fallible!(bw.flush());

    fallible!(bw.get_mut().sync_all());
    let parent = fallible!(fs::File::open(path.join(TABLE_DIR)));
    fallible!(parent.sync_all());

    if tmp_mv {
        let new_path = sst_dir_path.join(id_format(id));
        fallible!(fs::rename(sst_path, new_path));
    }

    Ok(())
}

fn read_table(path: &Path, id: u64) -> Result<Vec<(u64, Option<u64>)>> {
    let file = fallible!(fs::OpenOptions::new()
        .read(true)
        .open(path.join(TABLE_DIR).join(id_format(id))));

    let mut reader = BufReader::with_capacity(16 * 1024 * 1024, file);

    // crc + tombstone discriminant + key + value
    let mut buf = vec![0; 4 + 1 + K + V];

    let len_buf = &mut [0; 8];

    fallible!(reader.read_exact(len_buf));

    let expected_len: u64 = u64::from_le_bytes(*len_buf);
    let mut table = Vec::with_capacity(expected_len as usize);

    while let Ok(()) = reader.read_exact(&mut buf) {
        let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let d: bool = match buf[4] {
            0 => false,
            1 => true,
            _ => {
                log::warn!("detected torn-write while reading table {:016x}", id);
                break;
            }
        };
        let k_buf: [u8; K] = buf[5..K + 5].try_into().unwrap();
        let k = u64::from_be_bytes(k_buf);
        let v: Option<u64> = if d {
            let v_buf = buf[K + 5..5 + K + V].try_into().unwrap();
            let v = u64::from_be_bytes(v_buf);
            Some(v)
        } else {
            None
        };
        let crc_actual: u32 = hash(&k, &v);

        if crc_expected != crc_actual {
            log::warn!("detected torn-write while reading table {:016x}", id);
            break;
        }

        table.push((k, v));
    }

    if table.len() as u64 != expected_len {
        log::warn!(
            "table {:016x} tear detected - process probably crashed before full table could be \
             written out",
            id
        );
    }

    Ok(table)
}

pub struct MetadataLog {
    // `BufWriter` flushes on drop
    memtable: HashMap<u64, Option<u64>>,
    worker_outbox: Arc<Queue<WorkerMessage>>,
    next_table_id: u64,
    dirty_bytes: usize,
    log: BufWriter<fs::File>,
    path: PathBuf,
    config: MetadataLogConfig,
    stats: Stats,
    _worker_stats: Arc<WorkerStats>,
}

impl Drop for MetadataLog {
    fn drop(&mut self) {
        let waiter: Arc<Waiter> = Arc::default();

        self.worker_outbox.send(WorkerMessage::Stop(waiter.clone()));

        waiter.wait();
    }
}

impl MetadataLog {
    pub fn recover<P: AsRef<Path>>(p: P) -> Result<(HashMap<u64, u64>, MetadataLog)> {
        MetadataLog::recover_with_config(p, MetadataLogConfig::default())
    }

    pub fn recover_with_config<P: AsRef<Path>>(
        p: P,
        config: MetadataLogConfig,
    ) -> Result<(HashMap<u64, u64>, MetadataLog)> {
        let path = p.as_ref();
        if !path.exists() {
            fallible!(fs::create_dir_all(path));
            fallible!(fs::create_dir(path.join(TABLE_DIR)));
            fallible!(fallible!(fs::File::open(path.join(TABLE_DIR))).sync_all());
            fallible!(fallible!(fs::File::open(path)).sync_all());
            let mut parent_opt = path.parent();

            // need to recursively fsync parents since
            // we used create_dir_all
            while let Some(parent) = parent_opt {
                if parent.file_name().is_none() {
                    break;
                }
                if fs::File::open(parent).and_then(|f| f.sync_all()).is_err() {
                    // we made a reasonable attempt, but
                    // permissions
                    // can sometimes get in the way, and at
                    // this point it's
                    // becoming pedantic.
                    break;
                }
                parent_opt = parent.parent();
            }
        }

        let table_directory = list_tables(path, true)?;

        let mut table = HashMap::default();
        for table_id in table_directory.keys() {
            for (k, v) in read_table(path, *table_id)? {
                if let Some(v) = v {
                    table.insert(k, v);
                } else {
                    table.remove(&k);
                }
            }
        }

        let max_table_id = table_directory.keys().next_back().copied();

        let log = fallible!(fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.join(LOG_PATH)));

        let mut reader = BufReader::new(log);

        let tuple_sz = U64_SZ.max(K + V);
        let header_sz = 5;
        let header_tuple_sz = header_sz + tuple_sz;
        let mut buf = vec![0; header_tuple_sz];

        let mut memtable = HashMap::new();
        let mut recovered = 0;

        // write_batch is the pending memtable updates, the
        // number of remaining items in the write
        // batch, and the number of bytes that have
        // been recovered in the write batch.
        let mut write_batch: Option<(_, usize, u64)> = None;
        while let Ok(()) = reader.read_exact(&mut buf) {
            let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let d: bool = match buf[4] {
                0 => false,
                1 => true,
                2 if write_batch.is_none() => {
                    // begin batch
                    let batch_sz_buf: [u8; 8] = buf[5..5 + 8].try_into().unwrap();
                    let batch_sz: u64 = u64::from_le_bytes(batch_sz_buf);
                    log::debug!("processing batch of len {}", batch_sz);

                    let crc_actual = hash_batch_len(usize::try_from(batch_sz).unwrap());
                    if crc_expected != crc_actual {
                        log::warn!("crc mismatch for batch size marker");
                        break;
                    }

                    if !buf[5 + U64_SZ..].iter().all(|e| *e == 0) {
                        log::warn!(
                            "expected all pad bytes after logged batch manifests to be zero, but \
                             some corruption was detected"
                        );
                        break;
                    }

                    if batch_sz > usize::MAX as u64 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "recovering a batch size over usize::MAX is not supported",
                        ));
                    }

                    let wb_remaining = batch_sz as usize;
                    let wb_recovered = buf.len() as u64;

                    if wb_remaining > 0 {
                        write_batch = Some((
                            Vec::with_capacity(batch_sz as usize),
                            wb_remaining,
                            wb_recovered,
                        ));
                    } else {
                        recovered += buf.len() as u64;
                    }

                    continue;
                }
                _ => {
                    log::warn!("invalid log message discriminant detected: {}", buf[4]);
                    break;
                }
            };
            let k_buf: [u8; K] = buf[5..5 + K].try_into().unwrap();
            let k = u64::from_be_bytes(k_buf);
            let v: Option<u64> = if d {
                let v_buf = buf[K + 5..5 + K + V].try_into().unwrap();
                let v = u64::from_be_bytes(v_buf);
                Some(v)
            } else {
                None
            };

            let crc_actual: u32 = hash(&k, &v);

            if crc_expected != crc_actual {
                log::warn!(
                    "crc mismatch for kv pair {:?}-{:?}: expected {} actual {}, torn log detected",
                    k,
                    v,
                    crc_expected,
                    crc_actual
                );
                break;
            }

            let pad_start = if v.is_some() { 5 + K + V } else { 5 + K };

            if !buf[pad_start..].iter().all(|e| *e == 0) {
                log::warn!(
                    "expected all pad bytes for logged kv entries to be zero, but some corruption \
                     was detected"
                );
                break;
            }

            let (mut wb, mut wb_remaining, mut wb_recovered) = write_batch.take().unwrap();

            wb.push((k, v));
            wb_remaining = wb_remaining.checked_sub(1).unwrap();
            wb_recovered = wb_recovered.checked_add(buf.len() as u64).unwrap();

            // apply the write batch all at once
            // or never at all
            if wb_remaining == 0 {
                for (k, v) in wb {
                    memtable.insert(k, v);

                    if let Some(v) = v {
                        table.insert(k, v);
                    } else {
                        table.remove(&k);
                    }
                }
                recovered += wb_recovered;
            } else {
                write_batch = Some((wb, wb_remaining, wb_recovered));
            }
        }

        // need to back up a few bytes to chop off the torn
        // log
        log::debug!("recovered {} pieces of metadata", table.len(),);
        log::debug!("rewinding log down to length {}", recovered);
        let log_file = reader.get_mut();
        fallible!(log_file.seek(io::SeekFrom::Start(recovered)));
        fallible!(log_file.set_len(recovered));
        fallible!(log_file.sync_all());
        fallible!(fallible!(fs::File::open(path.join(TABLE_DIR))).sync_all());

        let q = Arc::new(Queue {
            mu: Default::default(),
            cv: Default::default(),
        });

        let _worker_stats = Arc::new(WorkerStats {
            read_bytes: 0.into(),
            written_bytes: 0.into(),
        });

        let worker: Worker = Worker {
            path: path.clone().into(),
            table_directory,
            inbox: q.clone(),
            config,
            stats: _worker_stats.clone(),
        };

        std::thread::spawn(move || worker.run());

        let hb_waiter: Arc<Waiter> = Arc::default();
        q.send(WorkerMessage::Heartbeat(hb_waiter.clone()));

        hb_waiter.wait();

        let metadata_store = MetadataLog {
            log: BufWriter::with_capacity(config.log_bufwriter_size as usize, reader.into_inner()),
            path: path.into(),
            next_table_id: max_table_id.unwrap_or(0) + 1,
            dirty_bytes: recovered as usize,
            worker_outbox: q,
            config,
            stats: Stats {
                logged_bytes: recovered,
                on_disk_bytes: 0,
                read_bytes: 0,
                written_bytes: 0,
                resident_bytes: table.len() as u64 * (K + V) as u64,
                space_amp: 0.,
                write_amp: 0.,
            },
            _worker_stats,
            memtable,
        };

        Ok((table, metadata_store))
    }

    /// Returns previous non-zero values for each item in
    /// batch
    pub fn log_batch(&mut self, write_batch: &[(u64, Option<u64>)]) -> Result<()> {
        let batch_len: [u8; 8] = (write_batch.len() as u64).to_le_bytes();
        let crc = hash_batch_len(write_batch.len());

        fallible!(self.log.write_all(&crc.to_le_bytes()));
        fallible!(self.log.write_all(&[2_u8]));
        fallible!(self.log.write_all(&batch_len));

        // the zero pad is necessary because every log
        // entry must have the same length, whether
        // it's a batch size or actual kv tuple.
        let tuple_sz = U64_SZ.max(K + V);
        let pad_sz = tuple_sz - U64_SZ;
        let pad = [0; U64_SZ];
        fallible!(self.log.write_all(&pad[..pad_sz]));

        for (k, v_opt) in write_batch {
            self.log_mutation(*k, *v_opt)?;
            self.memtable.insert(*k, *v_opt);
        }

        if self.dirty_bytes > self.config.max_log_length {
            self.flush()?;
        }

        Ok(())
    }

    fn log_mutation(&mut self, k: u64, v: Option<u64>) -> Result<()> {
        let crc: u32 = hash(&k, &v);
        fallible!(self.log.write_all(&crc.to_le_bytes()));
        fallible!(self.log.write_all(&[v.is_some() as u8]));
        fallible!(self.log.write_all(&k.to_be_bytes()));

        if let Some(v) = v {
            fallible!(self.log.write_all(&v.to_be_bytes()));
        } else {
            fallible!(self.log.write_all(&[0; V]));
        };

        // the zero pad is necessary because every log
        // entry must have the same length, whether
        // it's a batch size or actual kv tuple.
        let min_tuple_sz = U64_SZ.max(K + V);
        let pad_sz = min_tuple_sz - (K + V);
        let pad = [0; U64_SZ];
        fallible!(self.log.write_all(&pad[..pad_sz]));

        let logged_bytes = 4 + 1 + min_tuple_sz;

        self.memtable.insert(k, v);

        self.dirty_bytes += logged_bytes;
        self.stats.logged_bytes += logged_bytes as u64;
        self.stats.written_bytes += logged_bytes as u64;

        Ok(())
    }

    /// Blocks until all log data has been
    /// written out to disk and fsynced. If
    /// the log file has grown above a certain
    /// threshold, it will be compacted into
    /// a new table and the log file will
    /// be truncated after the table has
    /// been written, fsynced, and the table
    /// directory has been fsyced.
    pub fn flush(&mut self) -> Result<()> {
        fallible!(self.log.flush());
        fallible!(self.log.get_mut().sync_all());

        if self.dirty_bytes > self.config.max_log_length {
            log::debug!("compacting log to table");
            let memtable = std::mem::take(&mut self.memtable);
            let sst_id = self.next_table_id;
            if let Err(e) = write_table(&self.path, sst_id, &memtable, false) {
                // put memtable back together before
                // returning
                self.memtable = memtable;
                log::error!("failed to flush metadata_store log to table: {:?}", e);
                return Err(e.into());
            }

            let sst_sz = 8 + (memtable.len() as u64 * (4 + K + V) as u64);

            self.worker_outbox
                .send(WorkerMessage::NewT { id: sst_id, sst_sz });

            self.next_table_id += 1;

            let log_file: &mut fs::File = self.log.get_mut();
            fallible!(log_file.seek(io::SeekFrom::Start(0)));
            fallible!(log_file.set_len(0));
            fallible!(log_file.sync_all());
            let parent = fallible!(fs::File::open(self.path.join(TABLE_DIR)));
            fallible!(parent.sync_all());

            self.dirty_bytes = 0;
        }

        Ok(())
    }
}
