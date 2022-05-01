pub static FAULT_INJECT_COUNTER: AtomicU64 = AtomicU64::new(u64::MAX);

fn rng_sleep() {
    let rdtsc = unsafe {
        core::arch::x86_64::_rdtsc() as u16
    };

    for _ in 0..rdtsc.trailing_zeros() {
        std::thread::yield_now();
    }
}

macro_rules! io_try {
    ($e:expr) => {{
        if crate::FAULT_INJECT_COUNTER.fetch_sub(1, Ordering::Relaxed) == 1 {
            return Err(io::Error::new(
                std::io::ErrorKind::Other,
                "injected fault",
            ));
        }

        crate::rng_sleep();

        // converts io::Error to include the location of error creation
        match $e {
            Ok(ok) => ok,
            Err(e) => return Err(io::Error::new(
                e.kind(),
                format!("{}:{} -> {}", file!(), line!(), e.to_string()),
            )),
        }}
    }
}

mod metadata_log;

use std::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, AtomicBool, Ordering},
    Mutex, RwLock,
};

use pagetable::PageTable;

use metadata_log::MetadataLog;

const HEAP_DIR_SUFFIX: &str = "heap";
const PT_DIR_SUFFIX: &str = "page_index";
const LOCK_SUFFIX: &str = "lock";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const PT_LSN_KEY: u64 = 0;
const HEADER_LEN: usize = 20;
const MAX_GENERATION: u8 = 5;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PageId(pub NonZeroU64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct DiskLocation(NonZeroU64);

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
    pub path: PathBuf,
    pub target_file_size: u64,
    /// Remaining live percentage of a file before
    /// it's considered rewritabe.
    pub file_compaction_percent: u8,
    /// The ceiling on the largest allocation this system will ever
    /// attempt to perform in order to read a page off of disk.
    pub max_page_size: usize,
    /// A partitioning function for pages based on
    /// page ID, page size, and page rewrite generation.
    /// Causes pages to be written into separate files
    /// so that garbage collection may be handled at a
    /// finer granularity. Ideally, you will colocate
    /// pages that have similar expected lifespans, to
    /// minimize the costs of copying live data over time.
    pub partition_function: fn(PageId, usize, u8) -> u8,
    /// The minimum number of files within a generation to
    /// collect if below the live compaction percent.
    pub min_compaction_files: usize
}

pub fn default_partition_function(_pid: PageId, _size: usize, _generation: u8) -> u8 {
    0
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "".into(),
            target_file_size: 1 << 28, // 256mb
            file_compaction_percent: 60,
            partition_function: default_partition_function,
            max_page_size: 16 * 1024 * 1024 * 1024, // 16gb
            min_compaction_files: 2,
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


/// Garbage-collecting object store. A nice solution to back
/// a pagecache, for people building their own databases.
///
/// ROWEX-style concurrency: readers never block on other readers
/// or writers, but serializes writes to be friendlier for SSD GC.
/// This means that writes should generally be performed by some
/// background process whose job it is to clean logs etc...
pub struct Marble {
    // maps from PageId to DiskLocation
    page_table: PageTable,
    next_file_lsn_and_metadata_log: Mutex<(u64, MetadataLog)>,
    fams: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    config: Config,
    _file_lock: File,
    rowex: Mutex<()>,
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

        let _file_lock = io_try!(file_lock_opts.open(config.path.join(LOCK_SUFFIX)));
        io_try!(_file_lock.try_lock_exclusive());

        // recover page location index
        let (page_table, metadata_log) = MetadataLog::recover(config.path.join(PT_DIR_SUFFIX))?;

        // NB LSN should initially be 1, not 0, because 0 represents
        // a page being free.
        let recovered_pt_lsn = page_table.get(PT_LSN_KEY).load(Ordering::Acquire).max(1);

        // parse file names
        // calculate file tenancy

        let mut fams = BTreeMap::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        for entry_res in io_try!(fs::read_dir(heap_dir)) {
            let entry = io_try!(entry_res);
            let path = entry.path();
            let name = path
                .file_name()
                .expect("file without name encountered in internal directory")
                .to_str()
                .expect("non-utf8 file name encountered in internal directory");

            // remove files w/ temp name
            if name.ends_with("tmp") {
                eprintln!(
                    "removing heap file that was not fully written before the last crash: {:?}",
                    entry.path()
                );

                io_try!(fs::remove_file(entry.path()));
                continue;
            }

            let splits: Vec<&str> = name.split("-").collect();
            if splits.len() != 4 {
                eprintln!(
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

            // remove files that are ahead of the recovered page location index
            if lsn > recovered_pt_lsn {
                eprintln!(
                    "removing heap file that has an lsn of {}, \
                    which is higher than the recovered page table lsn of {}",
                    lsn, recovered_pt_lsn,
                );
                io_try!(fs::remove_file(entry.path()));
                continue;
            }

            let mut options = OpenOptions::new();
            options.read(true);

            let file = io_try!(options.open(entry.path()));
            let location = DiskLocation(lsn.try_into().unwrap());

            let file_size = io_try!(entry.metadata()).len();
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

            assert!(fams.insert(location, fam).is_none());
        }

        let next_file_lsn = max_file_lsn + max_file_size + 1;

        // initialize file tenancy from pt

        for pid in 0..page_table.approximate_max_child_count() {
            let location = page_table.get(pid).load(Ordering::Acquire);
            if location != 0 {
                let (_, fam) = fams.range(..=DiskLocation(location.try_into().unwrap())).next_back().unwrap();
                fam.len.fetch_add(1, Ordering::Acquire);
            }
        }

        if let Some((_, lsn_fam)) = fams.range(..=DiskLocation(recovered_pt_lsn.try_into().unwrap())).next_back() {
            lsn_fam.len.fetch_add(1, Ordering::Acquire);
        }

        Ok(Marble {
            page_table,
            fams: RwLock::new(fams),
            next_file_lsn_and_metadata_log: Mutex::new((next_file_lsn, metadata_log)),
            config,
            _file_lock,
            rowex: Default::default(),
        })
    }

    pub fn read(&self, pid: PageId) -> io::Result<Option<Vec<u8>>> {
        let fams = self.fams.read().unwrap();

        let lsn = self.page_table.get(pid.0.get()).load(Ordering::Acquire);
        if lsn == 0 {
            return Ok(None);
        }

        let location = DiskLocation(lsn.try_into().expect("unknown page location"));

        let (base_location, fam) = fams.range(..=location).next_back()
            .expect("no possible storage file for page - likely file corruption");

        let file_offset = lsn - base_location.0.get();

        let mut header_buf = [0_u8; HEADER_LEN];
        io_try!(fam.file.read_exact_at(&mut header_buf, file_offset));

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

        let mut page_buf = Vec::with_capacity(len);
        unsafe {
            page_buf.set_len(len);
        }

        let page_offset = file_offset + HEADER_LEN as u64;
        io_try!(fam.file.read_exact_at(&mut page_buf, page_offset));

        drop(fams);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&len_buf);
        hasher.update(&pid_buf);
        hasher.update(&page_buf);
        let crc_actual: u32 = hasher.finalize();

        if crc_expected != crc_actual {
            eprintln!(
                "crc mismatch when reading page at offset {} in file {:?}",
                page_offset, file_offset
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, "crc mismatch"));
        }

        let read_pid = u64::from_le_bytes(pid_buf);

        assert_eq!(pid.0.get(), read_pid);

        Ok(Some(page_buf))
    }

    pub fn write_batch(&self, write_batch: HashMap<PageId, Option<Vec<u8>>>) -> io::Result<()> {
        let _mu = self.rowex.lock().unwrap();
        let gen = 0;
        self.shard_batch(write_batch, gen)
    }

    fn shard_batch(&self, write_batch: HashMap<PageId, Option<Vec<u8>>>, gen: u8) -> io::Result<()> {
        let mut shards: HashMap<u8, HashMap<PageId, Option<Vec<u8>>>> = HashMap::new();

        for (pid, data) in write_batch {
            let shard_id = (self.config.partition_function)(pid, data.as_ref().map(|v| v.len()).unwrap_or(0), gen);

            let shard = shards.entry(shard_id).or_default();
            shard.insert(pid, data);
        }

        for (shard, pages) in shards {
            self.write_batch_inner(pages, gen, shard)?;
        }

        Ok(())
    }

    fn write_batch_inner(
        &self,
        pages: HashMap<PageId, Option<Vec<u8>>>,
        gen: u8,
        shard: u8,
    ) -> io::Result<()> {
        // allocates unique temporary file names
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut new_locations: Vec<(PageId, Option<u64>)> = vec![];
        let mut buf = vec![];

        // NB capacity starts with 1 due to the max LSN key that is always included
        let mut capacity = 1;
        for (pid, raw_page_opt) in pages {
            let raw_page = if let Some(raw_page) = raw_page_opt {
                raw_page
            } else {
                new_locations.push((pid, None));
                continue
            };

            capacity += 1;

            let relative_address = buf.len() as u64;
            new_locations.push((pid, Some(relative_address)));

            let len_buf: [u8; 8] = (raw_page.len() as u64).to_le_bytes();
            let pid_buf: [u8; 8] = pid.0.get().to_le_bytes();

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&pid_buf);
            hasher.update(&raw_page);
            let crc: u32 = hasher.finalize();

            io_try!(buf.write_all(&crc.to_le_bytes()));
            io_try!(buf.write_all(&pid_buf));
            io_try!(buf.write_all(&len_buf));
            io_try!(buf.write_all(&raw_page));
        }

        let tmp_fname = format!("{}-tmp", TMP_COUNTER.fetch_add(1, Ordering::Relaxed));
        let tmp_path = self.config.path.join(HEAP_DIR_SUFFIX).join(tmp_fname);

        let mut file_options = OpenOptions::new();
        file_options.read(true).write(true).create(true);

        let mut file = io_try!(file_options.open(&tmp_path));

        io_try!(file.write_all(&buf));

        let buf_len = buf.len();
        drop(buf);

        // mv and fsync new file and directory

        io_try!(file.sync_all());

        let mut next_lsn_and_metadata_log = self.next_file_lsn_and_metadata_log.lock().unwrap();

        let lsn = next_lsn_and_metadata_log.0;
        next_lsn_and_metadata_log.0 += buf_len as u64 + 1;

        let fname = format!("{:02x}-{:016x}-{:01x}-{:016x}", shard, lsn, gen, capacity);
        let new_path = self.config.path.join(HEAP_DIR_SUFFIX).join(fname);

        io_try!(fs::rename(tmp_path, &new_path));

        // fsync directory to ensure new file is present
        io_try!(File::open(self.config.path.join(HEAP_DIR_SUFFIX)).and_then(|f| f.sync_all()));

        let fam = FileAndMetadata {
            file,
            capacity,
            len: capacity.into(),
            generation: gen,
            location: DiskLocation(lsn.try_into().unwrap()),
            path: new_path,
            rewrite_claim: false.into(),
        };

        assert!(self
            .fams
            .write()
            .unwrap()
            .insert(fam.location, fam)
            .is_none());

        // write a batch of updates to the page table

        let write_batch: Vec<(u64, Option<u64>)> = new_locations
            .into_iter()
            .map(|(pid, location)| {
                let key = pid.0.get();
                let value = location;
                (key, value)
            })
            .chain(std::iter::once({
                // always mark the lsn w/ the pt batch
                let key = PT_LSN_KEY;
                let value = Some(lsn);
                (key, value)
            }))
            .collect();

        next_lsn_and_metadata_log.1.log_batch(&write_batch)?;
        next_lsn_and_metadata_log.1.flush()?;

        let mut replaced_locations = vec![];

        for (pid, new_location_opt) in write_batch {
            let old = self.page_table.get(pid).swap(
                new_location_opt.unwrap_or(0),
                Ordering::Release,
            );

            if old != 0 {
                replaced_locations.push(old);
            }
        }

        // NB this mutex should be held for the pagetable location
        // installation above
        drop(next_lsn_and_metadata_log);

        let fams = self.fams.read().unwrap();
        for old_location in replaced_locations {
            let (_, fam) = fams
                .range(..=DiskLocation(old_location.try_into().unwrap()))
                .next_back()
                .unwrap();

            let old = fam.len.fetch_sub(1, Ordering::Relaxed);
            assert_ne!(old, 0);
        }

        Ok(())
    }

    pub fn maintenance(&self) -> io::Result<()> {
        let _mu = self.rowex.lock().unwrap();
        let mut paths_to_remove = vec![];
        let mut files_to_defrag: HashMap<u8, Vec<_>> = Default::default();

        let fams = self.fams.read().unwrap();
        for (_, meta) in &*fams {
            let len = meta.len.load(Ordering::Acquire);
            let cap = meta.capacity.max(1);

            if meta.rewrite_claim.swap(true, Ordering::SeqCst) {
                // try to exclusively claim this file for rewrite to
                // prevent concurrent attempts at rewriting its contents
                continue;
            }

            if len == 0 {
                paths_to_remove.push((meta.location, meta.path.clone()));
            } else if (len * 100) / cap < u64::from(self.config.file_compaction_percent) {
                paths_to_remove.push((meta.location, meta.path.clone()));

                let generation = meta.generation.saturating_add(1).min(MAX_GENERATION);

                let entry = files_to_defrag.entry(generation).or_default();
                entry.push((meta.location.0.get(), meta.path.clone()));
            }
        }
        drop(fams);

        // rewrite the live pages
        for (generation, files) in &files_to_defrag {
            if files.len() < self.config.min_compaction_files {
                // release claims on batch with too few files
                let fams = self.fams.read().unwrap();
                for (base_lsn, _) in files {
                    let dl = DiskLocation((*base_lsn).try_into().unwrap());
                    assert!(fams[&dl].rewrite_claim.swap(false, Ordering::SeqCst))
                }
                // skip rewrite of the files in this generation
                continue;
            }

            let mut batch = HashMap::new();

            for (base_lsn, path) in files {
                let file = io_try!(File::open(path));
                let mut bufreader = BufReader::new(file);

                let mut offset = 0;

                loop {
                    let lsn = base_lsn + offset as u64;
                    let mut header = [0_u8; HEADER_LEN];
                    let header_res = bufreader.read_exact(&mut header);

                    match header_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        other => return other,
                    }

                    let crc_expected = u32::from_le_bytes(header[0..4].try_into().unwrap());
                    let pid_buf = header[4..12].try_into().unwrap();
                    let pid = u64::from_le_bytes(pid_buf);
                    let len_buf = header[12..20].try_into().unwrap();
                    let len = usize::try_from(u64::from_le_bytes(len_buf)).unwrap();

                    if len > self.config.max_page_size {
                        eprintln!("corrupt page size detected: {} bytes", len);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "corrupt page size",
                        ));
                    }

                    let mut page_buf = Vec::with_capacity(len);

                    unsafe {
                        page_buf.set_len(len);
                    }

                    let page_res = bufreader.read_exact(&mut page_buf);

                    match page_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        other => return other,
                    }

                    let mut hasher = crc32fast::Hasher::new();
                    hasher.update(&len_buf);
                    hasher.update(&pid_buf);
                    hasher.update(&page_buf);
                    let crc_actual = hasher.finalize();

                    if crc_expected != crc_actual {
                        eprintln!(
                            "crc mismatch when reading page at offset {} in file {:?}",
                            offset, path
                        );
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "crc mismatch"));
                    }

                    let current_location = self.page_table.get(pid).load(Ordering::Acquire);

                    if lsn == current_location {
                        // can attempt to rewrite
                        batch.insert(PageId(pid.try_into().unwrap()), Some(page_buf));
                    } else {
                        // page has been rewritten, this one isn't valid
                    }

                    offset += HEADER_LEN + len;
                }
            }

            io_try!(self.shard_batch(batch, *generation));
        }

        // get writer file lock and remove the replaced fams

        let mut fams = self.fams.write().unwrap();

        for (location, _) in &paths_to_remove {
            fams.remove(location);
        }

        drop(fams);

        for (_, path) in paths_to_remove {
            io_try!(std::fs::remove_file(path));
        }

        Ok(())
    }
}

fn _auto_trait_assertions() {
    use core::panic::{RefUnwindSafe, UnwindSafe};

    fn f<T: Send + Sync + UnwindSafe + RefUnwindSafe>() {}

    f::<Marble>();
}

static _TEST_COUNTER: AtomicU64 = AtomicU64::new(u64::MAX);

fn _with_tmp_instance<F: FnOnce(Marble)>(f: F) {
    let config = test_conf(&format!("test_{}", _TEST_COUNTER.fetch_add(1, Ordering::SeqCst)));
    let marble = config.open().unwrap();

    f(marble);

    std::fs::remove_dir_all(config.path).unwrap();
}

fn _restart(marble: Marble) -> Marble {
    let config = marble.config.clone();
    drop(marble);
    config.open().unwrap()
}

#[doc(hidden)]
pub const TEST_DIR: &str = "testing_data_directories";

#[doc(hidden)]
pub fn test_conf(subdir: &str) -> Config {
    let path = std::path::Path::new(TEST_DIR).join(subdir);

    Config {
        path,
        ..Default::default()
    }
}


#[test]
fn test_00() {
    _with_tmp_instance(|mut marble| {
        let pid = PageId(1.try_into().unwrap());
        marble.write_batch([(pid, Some(vec![]))].into_iter().collect()).unwrap();
        assert!(marble.read(pid).unwrap().is_some());
        marble = _restart(marble);
        assert!(marble.read(pid).unwrap().is_some());
    });
}
