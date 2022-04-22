use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, prelude::*, BufReader, BufWriter, Result};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc, Arc,
};

use pagetable::PageTable;

const SSTABLE_DIR: &str = "sstables";
const U64_SZ: usize = std::mem::size_of::<u64>();
const K: usize = 8;
const V: usize = 8;

fn pt_set(pt: &PageTable, k: u64, v: u64) -> u64 {
    pt.get(k).swap(v, Ordering::Release)
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// If on-disk uncompressed sstable data exceeds in-memory usage
    /// by this proportion, a full-compaction of all sstables will
    /// occur. This is only likely to happen in situations where
    /// multiple versions of most of the database's keys exist
    /// in multiple sstables, but should never happen for workloads
    /// where mostly new keys are being written.
    pub max_space_amp: u8,
    /// When the log file exceeds this size, a new compressed
    /// and compacted sstable will be flushed to disk and the
    /// log file will be truncated.
    pub max_log_length: usize,
    /// When the background compactor thread looks for contiguous
    /// ranges of sstables to merge, it will require all sstables
    /// to be at least 1/`merge_ratio` * the size of the first sstable
    /// in the contiguous window under consideration.
    pub merge_ratio: u8,
    /// When the background compactor thread looks for ranges of
    /// sstables to merge, it will require ranges to be at least
    /// this long.
    pub merge_window: u8,
    /// All inserts go directly to a `BufWriter` wrapping the log
    /// file. This option determines how large that in-memory buffer
    /// is.
    pub log_bufwriter_size: u32,
    /// The level of compression to use for the sstables with zstd.
    pub zstd_sstable_compression_level: u8,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            max_space_amp: 2,
            max_log_length: 32 * 1024 * 1024,
            merge_ratio: 3,
            merge_window: 10,
            log_bufwriter_size: 32 * 1024,
            zstd_sstable_compression_level: 3,
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

    // we XOR the hash to make sure it's something other than 0 when empty,
    // because 0 is an easy value to create accidentally or via corruption.
    hasher.finalize() ^ 0xFF
}

#[inline]
fn hash_batch_len(len: usize) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&(len as u64).to_le_bytes());

    hasher.finalize() ^ 0xFF
}

enum WorkerMessage {
    NewSST { id: u64, sst_sz: u64, db_sz: u64 },
    Stop(mpsc::Sender<()>),
    Heartbeat(mpsc::Sender<()>),
}

struct Worker {
    sstable_directory: BTreeMap<u64, u64>,
    inbox: mpsc::Receiver<WorkerMessage>,
    db_sz: u64,
    path: PathBuf,
    config: Config,
    stats: Arc<WorkerStats>,
}

impl Worker {
    fn run(mut self) {
        while self.tick() {}
        log::info!("tiny-lsm compaction worker quitting");
    }

    fn tick(&mut self) -> bool {
        match self.inbox.recv() {
            Ok(message) => {
                if !self.handle_message(message) {
                    return false;
                }
            }
            Err(mpsc::RecvError) => {
                return false;
            }
        }

        // only compact one run at a time before checking
        // for new messages.
        if let Err(e) = self.sstable_maintenance() {
            log::error!(
                "error while compacting sstables \
                in the background: {:?}",
                e
            );
        }

        true
    }

    fn handle_message(&mut self, message: WorkerMessage) -> bool {
        match message {
            WorkerMessage::NewSST { id, sst_sz, db_sz } => {
                self.db_sz = db_sz;
                self.sstable_directory.insert(id, sst_sz);
                true
            }
            WorkerMessage::Stop(dropper) => {
                drop(dropper);
                false
            }
            WorkerMessage::Heartbeat(dropper) => {
                drop(dropper);
                true
            }
        }
    }

    fn sstable_maintenance(&mut self) -> Result<()> {
        let on_disk_size: u64 = self.sstable_directory.values().sum();

        log::debug!("disk size: {} mem size: {}", on_disk_size, self.db_sz);
        if self.sstable_directory.len() > 1
            && on_disk_size / (self.db_sz + 1) > self.config.max_space_amp as u64
        {
            log::debug!(
                "performing full compaction, decompressed on-disk \
                database size has grown beyond {}x the in-memory size",
                self.config.max_space_amp
            );
            let run_to_compact: Vec<u64> = self.sstable_directory.keys().copied().collect();

            self.compact_sstable_run(&run_to_compact)?;
            return Ok(());
        }

        if self.sstable_directory.len() < self.config.merge_window.max(2) as usize {
            return Ok(());
        }

        for window in self
            .sstable_directory
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

                self.compact_sstable_run(&run_to_compact)?;
                return Ok(());
            }
        }

        Ok(())
    }

    // This function must be able to crash at any point without
    // leaving the system in an unrecoverable state, or without
    // losing data. This function must be nullipotent from the
    // external API surface's perspective.
    fn compact_sstable_run(&mut self, sstable_ids: &[u64]) -> Result<()> {
        log::debug!(
            "trying to compact sstable_ids {:?}",
            sstable_ids
                .iter()
                .map(|id| id_format(*id))
                .collect::<Vec<_>>()
        );

        let mut map = HashMap::new();

        let mut read_pairs = 0;

        for sstable_id in sstable_ids {
            for (k, v) in read_sstable(&self.path, *sstable_id)? {
                map.insert(k, v);
                read_pairs += 1;
            }
        }

        self.stats
            .read_bytes
            .fetch_add(read_pairs * (4 + 1 + K + V) as u64, Ordering::Relaxed);

        let sst_id = sstable_ids
            .iter()
            .max()
            .expect("compact_sstable_run called with empty set of sst ids");

        write_sstable(&self.path, *sst_id, &map, true, &self.config)?;

        self.stats
            .written_bytes
            .fetch_add(map.len() as u64 * (4 + 1 + K + V) as u64, Ordering::Relaxed);

        let sst_sz = map.len() as u64 * (4 + K + V) as u64;
        self.sstable_directory.insert(*sst_id, sst_sz);

        log::debug!("compacted range into sstable {}", id_format(*sst_id));

        for sstable_id in sstable_ids {
            if sstable_id == sst_id {
                continue;
            }
            fs::remove_file(self.path.join(SSTABLE_DIR).join(id_format(*sstable_id)))?;
            self.sstable_directory
                .remove(sstable_id)
                .expect("compacted sst not present in sstable_directory");
        }
        fs::File::open(self.path.join(SSTABLE_DIR))?.sync_all()?;

        Ok(())
    }
}

fn id_format(id: u64) -> String {
    format!("{:016x}", id)
}

fn list_sstables(path: &Path, remove_tmp: bool) -> Result<BTreeMap<u64, u64>> {
    let mut sstable_map = BTreeMap::new();

    for dir_entry_res in fs::read_dir(path.join(SSTABLE_DIR))? {
        let dir_entry = dir_entry_res?;
        let file_name = if let Ok(f) = dir_entry.file_name().into_string() {
            f
        } else {
            continue;
        };

        if let Ok(id) = u64::from_str_radix(&file_name, 16) {
            let metadata = dir_entry.metadata()?;

            sstable_map.insert(id, metadata.len());
        } else {
            if remove_tmp && file_name.ends_with("-tmp") {
                log::warn!("removing incomplete sstable rewrite {}", file_name);
                fs::remove_file(path.join(SSTABLE_DIR).join(file_name))?;
            }
        }
    }

    Ok(sstable_map)
}

fn write_sstable(
    path: &Path,
    id: u64,
    items: &HashMap<u64, Option<u64>>,
    tmp_mv: bool,
    config: &Config,
) -> Result<()> {
    let sst_dir_path = path.join(SSTABLE_DIR);
    let sst_path = if tmp_mv {
        sst_dir_path.join(format!("{:x}-tmp", id))
    } else {
        sst_dir_path.join(id_format(id))
    };

    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&sst_path)?;

    let max_zstd_level = zstd::compression_level_range();
    let zstd_level = config
        .zstd_sstable_compression_level
        .min(*max_zstd_level.end() as u8);

    let mut bw =
        BufWriter::new(zstd::Encoder::new(file, zstd_level as _).expect("zstd encoder failure"));

    bw.write_all(&(items.len() as u64).to_le_bytes())?;

    for (k, v) in items {
        let crc: u32 = hash(k, v);
        bw.write_all(&crc.to_le_bytes())?;
        bw.write_all(&[v.is_some() as u8])?;
        bw.write_all(&k.to_be_bytes())?;

        if let Some(v) = v {
            bw.write_all(&v.to_be_bytes())?;
        } else {
            bw.write_all(&[0; V])?;
        }
    }

    bw.flush()?;

    bw.get_mut().get_mut().sync_all()?;
    fs::File::open(path.join(SSTABLE_DIR))?.sync_all()?;

    if tmp_mv {
        let new_path = sst_dir_path.join(id_format(id));
        fs::rename(sst_path, new_path)?;
    }

    Ok(())
}

fn read_sstable(path: &Path, id: u64) -> Result<Vec<(u64, Option<u64>)>> {
    let file = fs::OpenOptions::new()
        .read(true)
        .open(path.join(SSTABLE_DIR).join(id_format(id)))?;

    let mut reader = zstd::Decoder::new(BufReader::with_capacity(16 * 1024 * 1024, file)).unwrap();

    // crc + tombstone discriminant + key + value
    let mut buf = vec![0; 4 + 1 + K + V];

    let len_buf = &mut [0; 8];

    reader.read_exact(len_buf)?;

    let expected_len: u64 = u64::from_le_bytes(*len_buf);
    let mut sstable = Vec::with_capacity(expected_len as usize);

    while let Ok(()) = reader.read_exact(&mut buf) {
        let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let d: bool = match buf[4] {
            0 => false,
            1 => true,
            _ => {
                log::warn!("detected torn-write while reading sstable {:016x}", id);
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
            log::warn!("detected torn-write while reading sstable {:016x}", id);
            break;
        }

        sstable.push((k, v));
    }

    if sstable.len() as u64 != expected_len {
        log::warn!(
            "sstable {:016x} tear detected - process probably crashed \
            before full sstable could be written out",
            id
        );
    }

    Ok(sstable)
}

pub struct Lsm {
    // `BufWriter` flushes on drop
    memtable: HashMap<u64, Option<u64>>,
    db: PageTable,
    worker_outbox: mpsc::Sender<WorkerMessage>,
    next_sstable_id: u64,
    dirty_bytes: usize,
    log: BufWriter<fs::File>,
    path: PathBuf,
    config: Config,
    stats: Stats,
    _worker_stats: Arc<WorkerStats>,
}

impl Drop for Lsm {
    fn drop(&mut self) {
        let (tx, rx) = mpsc::channel();

        if self.worker_outbox.send(WorkerMessage::Stop(tx)).is_err() {
            log::error!("failed to shut down compaction worker on Lsm drop");
            return;
        }

        for _ in rx {}
    }
}

impl std::ops::Deref for Lsm {
    type Target = PageTable;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl Lsm {
    pub fn recover<P: AsRef<Path>>(p: P) -> Result<Lsm> {
        Lsm::recover_with_config(p, Config::default())
    }

    pub fn recover_with_config<P: AsRef<Path>>(p: P, config: Config) -> Result<Lsm> {
        let path = p.as_ref();
        if !path.exists() {
            fs::create_dir_all(path)?;
            fs::create_dir(path.join(SSTABLE_DIR))?;
            fs::File::open(path.join(SSTABLE_DIR))?.sync_all()?;
            fs::File::open(path)?.sync_all()?;
            let mut parent_opt = path.parent();

            // need to recursively fsync parents since
            // we used create_dir_all
            while let Some(parent) = parent_opt {
                if parent.file_name().is_none() {
                    break;
                }
                if fs::File::open(parent).and_then(|f| f.sync_all()).is_err() {
                    // we made a reasonable attempt, but permissions
                    // can sometimes get in the way, and at this point it's
                    // becoming pedantic.
                    break;
                }
                parent_opt = parent.parent();
            }
        }

        let sstable_directory = list_sstables(path, true)?;

        let mut db = PageTable::default();
        for sstable_id in sstable_directory.keys() {
            for (k, v) in read_sstable(path, *sstable_id)? {
                if let Some(v) = v {
                    pt_set(&db, k, v);
                } else {
                    pt_set(&db, k, 0);
                }
            }
        }

        let max_sstable_id = sstable_directory.keys().next_back().copied();

        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.join("log"))?;

        let mut reader = BufReader::new(log);

        let tuple_sz = U64_SZ.max(K + V);
        let header_sz = 5;
        let header_tuple_sz = header_sz + tuple_sz;
        let mut buf = vec![0; header_tuple_sz];

        let mut memtable = HashMap::new();
        let mut recovered = 0;

        // write_batch is the pending memtable updates, the number
        // of remaining items in the write batch, and the number of
        // bytes that have been recovered in the write batch.
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
                            "expected all pad bytes after logged \
                            batch manifests to be zero, but some \
                            corruption was detected"
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
                    "expected all pad bytes for logged kv entries \
                    to be zero, but some corruption was detected"
                );
                break;
            }

            if let Some((mut wb, mut wb_remaining, mut wb_recovered)) = write_batch.take() {
                wb.push((k, v));
                wb_remaining = wb_remaining.checked_sub(1).unwrap();
                wb_recovered = wb_recovered.checked_add(buf.len() as u64).unwrap();

                // apply the write batch all at once
                // or never at all
                if wb_remaining == 0 {
                    for (k, v) in wb {
                        memtable.insert(k, v);

                        if let Some(v) = v {
                            pt_set(&db, k, v);
                        } else {
                            pt_set(&db, k, 0);
                        }
                    }
                    recovered += wb_recovered;
                } else {
                    write_batch = Some((wb, wb_remaining, wb_recovered));
                }
            } else {
                memtable.insert(k, v);

                if let Some(v) = v {
                    pt_set(&db, k, v);
                } else {
                    pt_set(&db, k, 0);
                }

                recovered += buf.len() as u64;
            }
        }

        // need to back up a few bytes to chop off the torn log
        let max_child_count = db.approximate_max_child_count();
        log::debug!(
            "recovered between {} and {} kv pairs",
            max_child_count - (1 << 16),
            max_child_count
        );
        log::debug!("rewinding log down to length {}", recovered);
        let log_file = reader.get_mut();
        log_file.seek(io::SeekFrom::Start(recovered))?;
        log_file.set_len(recovered)?;
        log_file.sync_all()?;
        fs::File::open(path.join(SSTABLE_DIR))?.sync_all()?;

        let (tx, rx) = mpsc::channel();

        let _worker_stats = Arc::new(WorkerStats {
            read_bytes: 0.into(),
            written_bytes: 0.into(),
        });

        let worker: Worker = Worker {
            path: path.clone().into(),
            sstable_directory,
            inbox: rx,
            db_sz: max_child_count * (K + V) as u64,
            config,
            stats: _worker_stats.clone(),
        };

        std::thread::spawn(move || worker.run());

        let (hb_tx, hb_rx) = mpsc::channel();
        tx.send(WorkerMessage::Heartbeat(hb_tx)).unwrap();

        for _ in hb_rx {}

        let lsm = Lsm {
            log: BufWriter::with_capacity(config.log_bufwriter_size as usize, reader.into_inner()),
            path: path.into(),
            next_sstable_id: max_sstable_id.unwrap_or(0) + 1,
            dirty_bytes: recovered as usize,
            worker_outbox: tx,
            config,
            stats: Stats {
                logged_bytes: recovered,
                on_disk_bytes: 0,
                read_bytes: 0,
                written_bytes: 0,
                resident_bytes: max_child_count * (K + V) as u64,
                space_amp: 0.,
                write_amp: 0.,
            },
            _worker_stats,
            db,
            memtable,
        };

        Ok(lsm)
    }

    /// Returns previous non-zero values for each item in batch
    pub fn write_batch(&mut self, write_batch: &[(u64, Option<u64>)]) -> Result<Vec<u64>> {
        let batch_len: [u8; 8] = (write_batch.len() as u64).to_le_bytes();
        let crc = hash_batch_len(write_batch.len());

        self.log.write_all(&crc.to_le_bytes())?;
        self.log.write_all(&[2_u8])?;
        self.log.write_all(&batch_len)?;

        // the zero pad is necessary because every log
        // entry must have the same length, whether
        // it's a batch size or actual kv tuple.
        let tuple_sz = U64_SZ.max(K + V);
        let pad_sz = tuple_sz - U64_SZ;
        let pad = [0; U64_SZ];
        self.log.write_all(&pad[..pad_sz])?;

        let mut replaced = Vec::with_capacity(write_batch.len());

        for (k, v_opt) in write_batch {
            let old = if let Some(v) = v_opt {
                pt_set(&self.db, *k, *v)
            } else {
                pt_set(&self.db, *k, 0)
            };

            if old != 0 {
                replaced.push(old);
            }

            self.log_mutation(*k, *v_opt)?;
            self.memtable.insert(*k, *v_opt);
        }

        if self.dirty_bytes > self.config.max_log_length {
            self.flush()?;
        }

        Ok(replaced)
    }

    fn log_mutation(&mut self, k: u64, v: Option<u64>) -> Result<()> {
        let crc: u32 = hash(&k, &v);
        self.log.write_all(&crc.to_le_bytes())?;
        self.log.write_all(&[v.is_some() as u8])?;
        self.log.write_all(&k.to_be_bytes())?;

        if let Some(v) = v {
            self.log.write_all(&v.to_be_bytes())?;
        } else {
            self.log.write_all(&[0; V])?;
        };

        // the zero pad is necessary because every log
        // entry must have the same length, whether
        // it's a batch size or actual kv tuple.
        let min_tuple_sz = U64_SZ.max(K + V);
        let pad_sz = min_tuple_sz - (K + V);
        let pad = [0; U64_SZ];
        self.log.write_all(&pad[..pad_sz])?;

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
    /// a new sstable and the log file will
    /// be truncated after the sstable has
    /// been written, fsynced, and the sstable
    /// directory has been fsyced.
    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        self.log.get_mut().sync_all()?;

        if self.dirty_bytes > self.config.max_log_length {
            log::debug!("compacting log to sstable");
            let memtable = std::mem::take(&mut self.memtable);
            let sst_id = self.next_sstable_id;
            if let Err(e) = write_sstable(&self.path, sst_id, &memtable, false, &self.config) {
                // put memtable back together before returning
                self.memtable = memtable;
                log::error!("failed to flush lsm log to sstable: {:?}", e);
                return Err(e.into());
            }

            let sst_sz = 8 + (memtable.len() as u64 * (4 + K + V) as u64);
            let db_sz = self.db.approximate_max_child_count() * (K + V) as u64;

            if let Err(e) = self.worker_outbox.send(WorkerMessage::NewSST {
                id: sst_id,
                sst_sz,
                db_sz,
            }) {
                log::error!("failed to send message to worker: {:?}", e);
                log::logger().flush();
                panic!("failed to send message to worker: {:?}", e);
            }

            self.next_sstable_id += 1;

            let log_file: &mut fs::File = self.log.get_mut();
            log_file.seek(io::SeekFrom::Start(0))?;
            log_file.set_len(0)?;
            log_file.sync_all()?;
            fs::File::open(self.path.join(SSTABLE_DIR))?.sync_all()?;

            self.dirty_bytes = 0;
        }

        Ok(())
    }

    pub fn stats(&mut self) -> Result<Stats> {
        self.stats.written_bytes += self._worker_stats.written_bytes.swap(0, Ordering::Relaxed);
        self.stats.read_bytes += self._worker_stats.read_bytes.swap(0, Ordering::Relaxed);
        self.stats.resident_bytes = self.db.approximate_max_child_count() * (K + V) as u64;

        let mut on_disk_bytes: u64 = std::fs::metadata(self.path.join("log"))?.len();

        on_disk_bytes += list_sstables(&self.path, false)?
            .into_iter()
            .map(|(_, len)| len)
            .sum::<u64>();

        self.stats.on_disk_bytes = on_disk_bytes;

        self.stats.write_amp =
            self.stats.written_bytes as f64 / self.stats.on_disk_bytes.max(1) as f64;
        self.stats.space_amp =
            self.stats.on_disk_bytes as f64 / self.stats.resident_bytes.max(1) as f64;
        Ok(self.stats)
    }
}
