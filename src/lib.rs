mod metadata_log;

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex, RwLock,
};

use fault_injection::fallible;
use pagetable::PageTable;

use metadata_log::MetadataLog;

pub use metadata_log::MetadataLogConfig;

const HEAP_DIR_SUFFIX: &str = "heap";
const PT_DIR_SUFFIX: &str = "object_index";
const LOCK_SUFFIX: &str = "lock";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const PT_LSN_KEY: u64 = 0;
const HEADER_LEN: usize = 20;
const MAX_GENERATION: u8 = 3;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(transparent)]
pub struct ObjectId(pub NonZeroU64);

impl std::ops::Deref for ObjectId {
    type Target = NonZeroU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ObjectId {
    pub const fn new(u: u64) -> Option<ObjectId> {
        if let Some(n) = NonZeroU64::new(u) {
            Some(ObjectId(n))
        } else {
            None
        }
    }
}

/// Statistics for file contents, to base decisions around
/// calls to `maintenance`.
pub struct FileStats {
    /// The number of live objects stored in the backing
    /// storage files.
    pub live_objects: u64,
    /// The total number of (potentially duplicated)
    /// objects stored in the backing storage files.
    pub stored_objects: u64,
    /// The number of dead objects that have been replaced
    /// or removed in other storage files, contributing
    /// to fragmentation.
    pub dead_objects: u64,
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
struct DiskLocation(NonZeroU64);

impl DiskLocation {
    fn new(u: u64) -> Option<DiskLocation> {
        Some(DiskLocation(NonZeroU64::new(u)?))
    }
}

#[derive(Debug)]
struct FileAndMetadata {
    file: File,
    location: DiskLocation,
    path: PathBuf,
    capacity: u64,
    len: AtomicU64,
    generation: u8,
    rewrite_claim: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Storage files will be kept here.
    pub path: PathBuf,
    /// Garbage collection will try to keep storage
    /// files around this size or smaller.
    pub target_file_size: usize,
    /// Remaining live percentage of a file before
    /// it's considered rewritabe.
    pub file_compaction_percent: u8,
    /// The ceiling on the largest allocation this system
    /// will ever attempt to perform in order to read an
    /// object off of disk.
    pub max_object_size: usize,
    /// A partitioning function for objects based on
    /// object ID and object size. You may override this to
    /// cause objects to be written into separate files so
    /// that garbage collection may take advantage of
    /// locality effects for your workload that are
    /// correlated to object identifiers or the size of
    /// data.
    ///
    /// Ideally, you will colocate objects that have
    /// similar expected lifespans. Doing so minimizes
    /// the costs of copying live data over time during
    /// storage file GC.
    pub partition_function: fn(object_id: ObjectId, object_size: usize) -> u8,
    /// The minimum number of files within a generation to
    /// collect if below the live compaction percent.
    pub min_compaction_files: usize,
    pub metadata_log_config: MetadataLogConfig,
}

/// Shard based on rough size ranges corresponding to SSD
/// page and block sizes
pub fn default_partition_function(_pid: ObjectId, size: usize) -> u8 {
    const SUBPAGE_MAX: usize = PAGE_MIN - 1;
    const PAGE_MIN: usize = 2048;
    const PAGE_MAX: usize = 16 * 1024;
    const BLOCK_MIN: usize = PAGE_MAX + 1;
    const BLOCK_MAX: usize = 4 * 1024 * 1024;

    match size {
        // items smaller than known SSD page sizes go to shard 0
        0..=SUBPAGE_MAX => 0,
        // items that fall roughly within the range of SSD page sizes go to shard 1
        PAGE_MIN..=PAGE_MAX => 1,
        // items that fall roughly within the size of an SSD block go to shard 2
        BLOCK_MIN..=BLOCK_MAX => 2,
        // large items that are larger than typical SSD block sizes go to shard 3
        _ => 3,
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "".into(),
            target_file_size: 1 << 28, // 256mb
            file_compaction_percent: 66,
            partition_function: default_partition_function,
            max_object_size: 16 * 1024 * 1024 * 1024, /* 16gb */
            min_compaction_files: 2,
            metadata_log_config: MetadataLogConfig::default(),
        }
    }
}

impl Config {
    fn validate(&self) -> io::Result<()> {
        if self.target_file_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Config's target_file_size must be non-zero",
            ));
        }

        if self.file_compaction_percent > 99 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Config's file_compaction_percent must be less than 100",
            ));
        }

        Ok(())
    }

    pub fn open(&self) -> io::Result<Marble> {
        Marble::open_with_config(self.clone())
    }
}

struct WritePath {
    next_file_lsn: u64,
    metadata_log: MetadataLog,
}

/// Garbage-collecting object store. A nice solution to back
/// a pagecache, for people building their own databases.
///
/// ROWEX-style concurrency: readers rarely block on other
/// readers or writers, but serializes writes to be
/// friendlier for SSD GC. This means that writes should
/// generally be performed by some background process whose
/// job it is to clean logs etc...
pub struct Marble {
    // maps from ObjectId to DiskLocation
    page_table: PageTable,
    write_path: Mutex<WritePath>,
    fams: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    config: Config,
    _file_lock: File,
}

impl Marble {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Marble> {
        let config = Config {
            path: path.as_ref().into(),
            ..Config::default()
        };

        Marble::open_with_config(config)
    }

    pub fn open_with_config(config: Config) -> io::Result<Marble> {
        use fs2::FileExt;

        config.validate()?;

        log::debug!("opening Marble at {:?}", config.path);

        // initialize directories if not present
        let heap_dir = config.path.join(HEAP_DIR_SUFFIX);

        if let Err(e) = fs::read_dir(&heap_dir) {
            if e.kind() == io::ErrorKind::NotFound {
                let _ = fs::create_dir_all(&heap_dir);
            }
        }

        let _ = File::create(config.path.join(WARN));

        let mut file_lock_opts = OpenOptions::new();
        file_lock_opts.create(true).read(true).write(true);

        let _file_lock = fallible!(file_lock_opts.open(config.path.join(LOCK_SUFFIX)));
        fallible!(_file_lock.try_lock_exclusive());

        // recover object location index
        let (metadata, metadata_log) = MetadataLog::recover(config.path.join(PT_DIR_SUFFIX))?;

        // NB LSN should initially be 1, not 0, because 0
        // represents an object being free.
        let recovered_pt_lsn = metadata.get(&PT_LSN_KEY).copied().unwrap_or(1);

        // parse file names
        // calculate file tenancy

        let mut fams = BTreeMap::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        for entry_res in fallible!(fs::read_dir(heap_dir)) {
            let entry = fallible!(entry_res);
            let path = entry.path();
            let name = path
                .file_name()
                .expect("file without name encountered in internal directory")
                .to_str()
                .expect("non-utf8 file name encountered in internal directory");

            log::trace!("examining filename {} in heap directory", name);

            // remove files w/ temp name
            if name.ends_with("tmp") {
                log::warn!(
                    "removing heap file that was not fully written before the last crash: {:?}",
                    entry.path()
                );

                fallible!(fs::remove_file(entry.path()));
                continue;
            }

            let splits: Vec<&str> = name.split("-").collect();
            if splits.len() != 4 {
                log::error!(
                    "encountered strange file in internal directory: {:?}",
                    entry.path()
                );
                continue;
            }

            let _shard = u8::from_str_radix(&splits[0], 16)
                .expect("encountered garbage filename in internal directory");
            let lsn = u64::from_str_radix(&splits[1], 16)
                .expect("encountered garbage filename in internal directory");
            let generation = u8::from_str_radix(splits[2], 16)
                .expect("encountered garbage filename in internal directory");
            let capacity = u64::from_str_radix(&splits[3], 16)
                .expect("encountered garbage filename in internal directory");

            // remove files that are ahead of the recovered
            // object location index
            if lsn > recovered_pt_lsn {
                log::warn!(
                    "removing heap file that has an lsn of {}, which is higher than the recovered \
                     pagetable lsn of {}",
                    lsn,
                    recovered_pt_lsn,
                );
                fallible!(fs::remove_file(entry.path()));
                continue;
            }

            let mut options = OpenOptions::new();
            options.read(true);

            let file = fallible!(options.open(entry.path()));
            let location = DiskLocation::new(lsn).unwrap();

            let file_size = fallible!(entry.metadata()).len();
            max_file_size = max_file_size.max(file_size);
            max_file_lsn = max_file_lsn.max(lsn);

            let fam = FileAndMetadata {
                len: 0.into(),
                capacity,
                path: entry.path().into(),
                file,
                location,
                generation,
                rewrite_claim: false.into(),
            };

            log::debug!("inserting new fam at location {:?}", location);
            assert!(fams.insert(location, fam).is_none());
        }

        let next_file_lsn = max_file_lsn + max_file_size + 1;

        // initialize file tenancy from pt
        let page_table = PageTable::default();
        for (pid, location) in metadata {
            assert_ne!(location, 0);
            if location != 0 && pid != PT_LSN_KEY {
                let (_, fam) = fams
                    .range(..=DiskLocation::new(location).unwrap())
                    .next_back()
                    .unwrap();
                fam.len.fetch_add(1, Ordering::Acquire);
                page_table.get(pid).store(location, Ordering::Release);
            }
        }

        if let Some((_, lsn_fam)) = fams
            .range(..=DiskLocation::new(recovered_pt_lsn).unwrap())
            .next_back()
        {
            lsn_fam.len.fetch_add(1, Ordering::Acquire);
        }

        Ok(Marble {
            page_table,
            fams: RwLock::new(fams),
            write_path: Mutex::new(WritePath {
                next_file_lsn,
                metadata_log,
            }),
            config,
            _file_lock,
        })
    }

    /// Read a object out of storage. If this object is
    /// unknown or has been removed, returns `Ok(None)`.
    ///
    /// May be called concurrently with background calls to
    /// `maintenance` and `write_batch`.
    pub fn read(&self, pid: ObjectId) -> io::Result<Option<Vec<u8>>> {
        let fams = self.fams.read().unwrap();

        let lsn = self.page_table.get(pid.0.get()).load(Ordering::Acquire);
        if lsn == 0 {
            return Ok(None);
        }

        let location = DiskLocation::new(lsn).unwrap();

        let (base_location, fam) = fams
            .range(..=location)
            .next_back()
            .expect("no possible storage file for object - likely file corruption");

        let file_offset = lsn - base_location.0.get();

        let mut header_buf = [0_u8; HEADER_LEN];
        fallible!(fam.file.read_exact_at(&mut header_buf, file_offset));

        let crc_expected_buf: [u8; 4] = header_buf[0..4].try_into().unwrap();
        let pid_buf: [u8; 8] = header_buf[4..12].try_into().unwrap();
        let len_buf: [u8; 8] = header_buf[12..].try_into().unwrap();
        let crc_expected = u32::from_le_bytes(crc_expected_buf);

        let len: usize = if let Ok(len) = u64::from_le_bytes(len_buf).try_into() {
            len
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "corrupted length detected",
            ));
        };

        let mut object_buf = Vec::with_capacity(len);
        unsafe {
            object_buf.set_len(len);
        }

        let object_offset = file_offset + HEADER_LEN as u64;
        fallible!(fam.file.read_exact_at(&mut object_buf, object_offset));

        drop(fams);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&len_buf);
        hasher.update(&pid_buf);
        hasher.update(&object_buf);
        let crc_actual: u32 = hasher.finalize();

        if crc_expected != crc_actual {
            log::warn!(
                "crc mismatch when reading object at offset {} in file {:?}",
                object_offset,
                file_offset
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, "crc mismatch"));
        }

        let read_pid = u64::from_le_bytes(pid_buf);

        assert_eq!(pid.0.get(), read_pid);

        Ok(Some(object_buf))
    }

    /// Statistics about current files, intended to inform
    /// decisions about when to call `maintenance` based on
    /// desired write and space amplification
    /// characteristics.
    pub fn file_statistics(&self) -> FileStats {
        let mut live_objects = 0;
        let mut stored_objects = 0;

        for (_, fam) in &*self.fams.read().unwrap() {
            live_objects += fam.len.load(Ordering::Acquire);
            stored_objects += fam.capacity;
        }

        FileStats {
            live_objects,
            stored_objects,
            dead_objects: stored_objects - live_objects,
        }
    }

    /// A monotonic measure of logical progress that this
    /// system has made. You can refer to this in logs and
    /// other stores that feed into `marble`, so that after
    /// recovering `marble`, you can avoid double-recovering
    /// any mutations that were already persisted here. Note
    /// that if you are concurrently calling `write_batch`
    /// or `maintenance`, the stable logical sequence number
    /// will increase, so you should only use this to fence
    /// idempotent operations if used in a concurrent
    /// setting.
    pub fn stable_logical_sequence_number(&self) -> u64 {
        let write_path = self.write_path.lock().unwrap();
        write_path.next_file_lsn.saturating_sub(1)
    }

    /// Write a batch of objects to disk. This function is
    /// crash-atomic but NOT runtime atomic. If you are
    /// concurrently serving reads, and require atomic batch
    /// semantics, you should serve reads out of an
    /// in-memory cache until this function returns. Creates
    /// at least one file per call. Performs several fsync
    /// calls per call. Ideally, you will heavily batch
    /// objects being written using a logger of some sort
    /// before calling this function occasionally in the
    /// background, then deleting corresponding logs after
    /// this function returns.
    pub fn write_batch<B, I>(&self, write_batch: I) -> io::Result<()>
    where
        B: AsRef<[u8]>,
        I: IntoIterator<Item = (ObjectId, Option<B>)>,
    {
        let gen = 0;
        let lsn_fence_opt = None;
        self.shard_batch(write_batch, gen, lsn_fence_opt)
    }

    fn shard_batch<B, I>(
        &self,
        write_batch: I,
        gen: u8,
        lsn_fence_opt: Option<u64>,
    ) -> io::Result<()>
    where
        B: AsRef<[u8]>,
        I: IntoIterator<Item = (ObjectId, Option<B>)>,
    {
        // maps from shard -> (shard size, map of object
        // id's to object data)
        let mut shards: HashMap<u8, (usize, HashMap<ObjectId, Option<B>>)> = HashMap::new();

        let mut fragmented_shards = vec![];

        for (pid, data_opt) in write_batch {
            let (object_size, shard_id) = if let Some(ref data) = data_opt {
                let len = data.as_ref().len();
                (len + HEADER_LEN, (self.config.partition_function)(pid, len))
            } else {
                (0, 0)
            };

            let shard = shards.entry(shard_id).or_default();

            if shard.0 > self.config.target_file_size {
                fragmented_shards.push((shard_id, std::mem::take(&mut shard.1)));
                shard.0 = 0;
            }

            shard.0 += object_size;
            shard.1.insert(pid, data_opt);
        }

        let iter = shards
            .into_iter()
            .map(|(shard, (_sz, objects))| (shard, objects))
            .chain(fragmented_shards.into_iter());

        for (shard, objects) in iter {
            self.write_batch_inner(objects, gen, shard, lsn_fence_opt)?;
        }

        Ok(())
    }

    fn write_batch_inner<B>(
        &self,
        objects: HashMap<ObjectId, Option<B>>,
        gen: u8,
        shard: u8,
        lsn_fence_opt: Option<u64>,
    ) -> io::Result<()>
    where
        B: AsRef<[u8]>,
    {
        // allocates unique temporary file names
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

        let tmp_fname = format!("{}-tmp", TMP_COUNTER.fetch_add(1, Ordering::Relaxed));
        let tmp_path = self.config.path.join(HEAP_DIR_SUFFIX).join(tmp_fname);

        let mut file_options = OpenOptions::new();
        file_options.read(true).write(true).create(true);

        let file = fallible!(file_options.open(&tmp_path));
        let mut buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        let mut new_locations: Vec<(ObjectId, Option<u64>)> = vec![];

        let mut written_bytes = 0;
        let mut capacity = 0;
        for (pid, raw_object_opt) in &objects {
            let raw_object = if let Some(raw_object) = raw_object_opt {
                raw_object.as_ref()
            } else {
                new_locations.push((*pid, None));
                continue;
            };

            if raw_object.len() > self.config.max_object_size {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "{:?} in write batch has a size of {}, which is larger than the \
                         configured `max_object_size` of {}. If this is intentional, please \
                         increase the configured `max_object_size`.",
                        pid,
                        raw_object.len(),
                        self.config.max_object_size,
                    ),
                ));
            }

            capacity += 1;

            let relative_address = written_bytes as u64;
            new_locations.push((*pid, Some(relative_address)));

            let len_buf: [u8; 8] = (raw_object.len() as u64).to_le_bytes();
            let pid_buf: [u8; 8] = pid.0.get().to_le_bytes();

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&pid_buf);
            hasher.update(&raw_object);
            let crc: u32 = hasher.finalize();

            fallible!(buf_writer.write_all(&crc.to_le_bytes()));
            fallible!(buf_writer.write_all(&pid_buf));
            fallible!(buf_writer.write_all(&len_buf));
            fallible!(buf_writer.write_all(&raw_object));

            written_bytes += HEADER_LEN + raw_object.len();
        }

        fallible!(buf_writer.flush());

        let file: File = buf_writer
            .into_inner()
            .expect("BufWriter::into_inner should not fail after calling flush directly before");

        // mv and fsync new file and directory

        fallible!(file.sync_all());

        let mut write_path = self.write_path.lock().unwrap();

        let lsn = write_path.next_file_lsn;
        write_path.next_file_lsn += written_bytes as u64 + 1;

        let fname = format!("{:02x}-{:016x}-{:01x}-{:016x}", shard, lsn, gen, capacity);
        let new_path = self.config.path.join(HEAP_DIR_SUFFIX).join(fname);

        if capacity > 0 {
            fallible!(fs::rename(tmp_path, &new_path));
        } else {
            fallible!(fs::remove_file(tmp_path));
        }

        // fsync directory to ensure new file is present
        fallible!(File::open(self.config.path.join(HEAP_DIR_SUFFIX)).and_then(|f| f.sync_all()));

        let location = DiskLocation::new(lsn).unwrap();
        if capacity > 0 {
            let fam = FileAndMetadata {
                file,
                capacity,
                len: capacity.into(),
                generation: gen,
                location,
                path: new_path,
                rewrite_claim: false.into(),
            };

            log::debug!("inserting new fam at location {:?}", lsn);

            let mut fams = self.fams.write().unwrap();
            assert!(fams.insert(location, fam).is_none());
            drop(fams);
        }

        // write a batch of updates to the page table

        assert_ne!(lsn, 0);

        let mut contention_hit = 0;
        let mut write_batch: Vec<(u64, Option<u64>)> = Vec::with_capacity(new_locations.len() + 1);

        for (pid, location_opt) in new_locations {
            let key = pid.0.get();
            let value = if let Some(location) = location_opt {
                Some(location + lsn)
            } else {
                None
            };

            if let Some(lsn_fence) = lsn_fence_opt {
                if self.page_table.get(key).load(Ordering::Acquire) >= lsn_fence {
                    // a concurrent batch has replaced this
                    // attempted object GC rewrite in a
                    // later file, invalidating the copy.
                    contention_hit += 1;
                    continue;
                }
            }

            write_batch.push((key, value));
        }

        // always mark the lsn w/ the pt batch
        let key = PT_LSN_KEY;
        let value = Some(lsn);
        write_batch.push((key, value));

        write_path.metadata_log.log_batch(&write_batch)?;
        write_path.metadata_log.flush()?;

        let mut replaced_locations = Vec::with_capacity(write_batch.len());

        for (pid, new_location_opt) in write_batch {
            if pid == PT_LSN_KEY {
                continue;
            }
            let old = self
                .page_table
                .get(pid)
                .swap(new_location_opt.unwrap_or(0), Ordering::Release);

            if let Some(lsn_fence) = lsn_fence_opt {
                assert!(
                    lsn_fence > old || pid == 0,
                    "lsn_fence of {} should always be higher than the replaced lsn of {} for pid \
                     {}",
                    lsn_fence,
                    old,
                    pid
                );
            }

            log::trace!(
                "updating metadata for pid {} from {:?} to {:?}",
                pid,
                old,
                new_location_opt
            );

            if old != 0 {
                replaced_locations.push(old);
            }
        }

        // NB this mutex should be held for the pagetable
        // location installation above
        drop(write_path);

        let fams = self.fams.read().unwrap();

        if capacity > 0 {
            let old_len = fams[&location]
                .len
                .fetch_sub(contention_hit, Ordering::Relaxed);

            assert!(
                old_len != 0 || contention_hit == 0,
                "unexpected file metadata length wrap to u64::MAX"
            );
        }

        for old_location in replaced_locations {
            let (_, fam) = fams
                .range(..=DiskLocation::new(old_location).unwrap())
                .next_back()
                .unwrap();

            let old = fam.len.fetch_sub(1, Ordering::Relaxed);
            assert_ne!(old, 0);
        }

        Ok(())
    }

    fn prune_empty_fams(&self) -> io::Result<()> {
        // get writer file lock and remove the empty fams
        let mut paths_to_remove = vec![];
        let mut fams = self.fams.write().unwrap();

        for (location, fam) in &*fams {
            if fam.len.load(Ordering::Acquire) == 0
                && !fam.rewrite_claim.swap(true, Ordering::SeqCst)
            {
                log::trace!(
                    "fam at location {:?} is empty, marking it for removal",
                    location
                );
                paths_to_remove.push((*location, fam.path.clone()));
            }
        }

        for (location, _) in &paths_to_remove {
            log::trace!("removing fam at location {:?}", location);
            fams.remove(location).unwrap();
        }

        drop(fams);

        for (_, path) in paths_to_remove {
            // If this fails, it causes a file leak, but it
            // is fixed by simply restarting.
            fallible!(std::fs::remove_file(path));
        }

        Ok(())
    }

    /// Defragments backing storage files, blocking
    /// concurrent calls to `write_batch` but not
    /// blocking concurrent calls to `read`. Returns the
    /// number of rewritten objects.
    pub fn maintenance(&self) -> io::Result<usize> {
        log::debug!("performing maintenance");

        let mut defer_unclaim = DeferUnclaim {
            marble: self,
            claims: vec![],
        };

        let mut files_to_defrag: HashMap<u8, Vec<_>> = Default::default();

        let write_path = self.write_path.lock().unwrap();
        let lsn_fence = write_path.next_file_lsn.saturating_sub(1);
        drop(write_path);

        let fams = self.fams.read().unwrap();
        for (location, meta) in &*fams {
            assert_eq!(*location, meta.location);
            let len = meta.len.load(Ordering::Acquire);
            let cap = meta.capacity;

            assert_ne!(cap, 0);

            if len != 0 && (len * 100) / cap < u64::from(self.config.file_compaction_percent) {
                if meta.rewrite_claim.swap(true, Ordering::SeqCst) {
                    // try to exclusively claim this file
                    // for rewrite to
                    // prevent concurrent attempts at
                    // rewriting its contents
                    continue;
                }

                defer_unclaim.claims.push(*location);

                log::trace!(
                    "fam at location {:?} is ready to be compacted",
                    meta.location
                );

                let generation = meta.generation.saturating_add(1).min(MAX_GENERATION);

                let entry = files_to_defrag.entry(generation).or_default();
                entry.push((location.0.get(), meta.path.clone()));
            }
        }
        drop(fams);

        let mut rewritten_objects = 0;

        // rewrite the live objects
        for (generation, files) in &files_to_defrag {
            log::trace!(
                "compacting files {:?} with generation {}",
                files,
                generation
            );
            if files.len() < self.config.min_compaction_files {
                // skip batch with too few files (claims
                // auto-released by Drop of DeferUnclaim
                continue;
            }

            let mut batch = HashMap::new();

            for (base_lsn, path) in files {
                let file = fallible!(File::open(path));
                let mut bufreader = BufReader::new(file);

                let mut offset = 0;

                loop {
                    let lsn = base_lsn + offset as u64;
                    let mut header = [0_u8; HEADER_LEN];
                    let header_res = bufreader.read_exact(&mut header);

                    match header_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(other) => return Err(other),
                    }

                    let crc_expected = u32::from_le_bytes(header[0..4].try_into().unwrap());
                    let pid_buf = header[4..12].try_into().unwrap();
                    let pid = u64::from_le_bytes(pid_buf);
                    let len_buf = header[12..20].try_into().unwrap();
                    let len = usize::try_from(u64::from_le_bytes(len_buf)).unwrap();

                    if len > self.config.max_object_size {
                        log::warn!("corrupt object size detected: {} bytes", len);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "corrupt object size",
                        ));
                    }

                    let mut object_buf = Vec::with_capacity(len);

                    unsafe {
                        object_buf.set_len(len);
                    }

                    let object_res = bufreader.read_exact(&mut object_buf);

                    match object_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(other) => return Err(other),
                    }

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(&len_buf);
                    hasher.update(&pid_buf);
                    hasher.update(&object_buf);
                    let crc_actual = hasher.finalize();

                    if crc_expected != crc_actual {
                        log::warn!(
                            "crc mismatch when reading object at offset {} in file {:?}",
                            offset,
                            path
                        );
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "crc mismatch"));
                    }

                    let current_location = self.page_table.get(pid).load(Ordering::Acquire);

                    if lsn == current_location {
                        // can attempt to rewrite
                        batch.insert(ObjectId::new(pid).unwrap(), Some(object_buf));
                    } else {
                        // object has been rewritten, this
                        // one isn't valid
                    }

                    offset += HEADER_LEN + len;
                }
            }

            rewritten_objects += batch.len();

            self.shard_batch(batch, *generation, Some(lsn_fence))?;
        }

        drop(defer_unclaim);

        self.prune_empty_fams()?;

        Ok(rewritten_objects)
    }
}

// `DeferUnclaim` exists because it was surprisingly
// leak-prone to try to manage fams that were claimed by a
// maintenance thread but never used. This ensures fams
// always get unclaimed after this function returns.
struct DeferUnclaim<'a> {
    marble: &'a Marble,
    claims: Vec<DiskLocation>,
}

impl<'a> Drop for DeferUnclaim<'a> {
    fn drop(&mut self) {
        let fams = self.marble.fams.read().unwrap();
        for claim in &self.claims {
            if let Some(fam) = fams.get(claim) {
                assert!(fam.rewrite_claim.swap(false, Ordering::SeqCst));
            }
        }
    }
}

fn _auto_trait_assertions() {
    use core::panic::{RefUnwindSafe, UnwindSafe};

    fn f<T: Send + Sync + UnwindSafe + RefUnwindSafe>() {}

    f::<Marble>();
}

#[cfg(test)]
mod test {
    use super::*;

    const TEST_DIR: &str = "testing_data_directories";

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(u64::MAX);

    fn with_tmp_instance<F: FnOnce(Marble)>(f: F) {
        let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, Ordering::SeqCst));
        let path = std::path::Path::new(TEST_DIR).join(subdir);

        let config = Config {
            path,
            ..Default::default()
        };

        let _ = std::fs::remove_dir_all(&config.path);

        let marble = config.open().unwrap();

        f(marble);

        std::fs::remove_dir_all(config.path).unwrap();
    }

    fn restart(marble: Marble) -> Marble {
        let config = marble.config.clone();
        drop(marble);
        config.open().unwrap()
    }

    #[test]
    fn test_00() {
        with_tmp_instance(|mut marble| {
            let pid = ObjectId::new(1).unwrap();
            marble
                .write_batch([(pid, Some(vec![]))].into_iter())
                .unwrap();
            assert!(marble.read(pid).unwrap().is_some());
            marble = restart(marble);
            assert!(marble.read(pid).unwrap().is_some());
        });
    }

    #[test]
    fn test_01() {
        with_tmp_instance(|mut marble| {
            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch([(pid_1, Some(vec![]))].into_iter())
                .unwrap();
            let pid_2 = ObjectId::new(2).unwrap();
            marble
                .write_batch([(pid_2, Some(vec![]))].into_iter())
                .unwrap();
            assert!(marble.read(pid_1).unwrap().is_some());
            assert!(marble.read(pid_2).unwrap().is_some());
            marble = restart(marble);
            assert!(marble.read(pid_1).unwrap().is_some());
            assert!(marble.read(pid_2).unwrap().is_some());
        });
    }

    #[test]
    fn test_02() {
        let _ = env_logger::try_init();

        with_tmp_instance(|marble| {
            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch([(pid_1, Some(vec![]))].into_iter())
                .unwrap();
            let pid_2 = ObjectId::new(2).unwrap();
            marble
                .write_batch([(pid_2, Some(vec![]))].into_iter())
                .unwrap();
            assert!(marble.read(pid_1).unwrap().is_some());
            assert!(marble.read(pid_2).unwrap().is_some());
            marble.maintenance().unwrap();
            assert!(marble.read(pid_1).unwrap().is_some());
            assert!(marble.read(pid_2).unwrap().is_some());
        });
    }

    #[test]
    fn test_03() {
        let _ = env_logger::try_init();

        with_tmp_instance(|marble| {
            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch::<Vec<u8>, _>([(pid_1, None)].into_iter())
                .unwrap();
        });
    }

    #[test]
    fn test_04() {
        let _ = env_logger::try_init();

        with_tmp_instance(|marble| {
            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch::<Vec<u8>, _>([(pid_1, None)].into_iter())
                .unwrap();

            marble.maintenance().unwrap();

            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch::<Vec<u8>, _>([(pid_1, None)].into_iter())
                .unwrap();
        });
    }

    #[test]
    fn test_05() {
        let _ = env_logger::try_init();

        with_tmp_instance(|marble| {
            let pid_1 = ObjectId::new(1).unwrap();
            marble
                .write_batch::<Vec<u8>, _>([(pid_1, None)].into_iter())
                .unwrap();

            restart(marble);
        });
    }
}
