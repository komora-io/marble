use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

mod pt_lsm;
use pt_lsm::Lsm;

// converts io::Error to include the location of error creation
macro_rules! io_try {
    ($e:expr) => {
        match $e {
            Ok(ok) => ok,
            Err(e) => return Err(io::Error::new(
                e.kind(),
                format!("{} {}", file!(), line!()),
            )),
        }
    }
}

const HEAP_DIR_SUFFIX: &str = "heap";
const PT_DIR_SUFFIX: &str = "page_index";
const LOCK_SUFFIX: &str = "lock";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
// TODO make this 0, shift everything up by 1, so that there's
// no waste page?
const PT_LSN_KEY: u64 = u64::MAX;
const PT_LOGICAL_EPOCH_KEY: [u8; 8] = (u64::MAX - 1).to_be_bytes();
const HEADER_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PageId(pub u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct DiskLocation(u64);

#[derive(Debug)]
struct FileAndMetadata {
    file: File,
    location: DiskLocation,
    path: PathBuf,
    capacity: u64,
    len: AtomicU64,
    shard: u8,
    generation: u8,
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

    pub fn open(self) -> io::Result<Marble> {
        Marble::open_with_config(self)
    }
}

/// Garbage-collecting object store. A nice solution to back
/// a pagecache, for people building their own databases.
///
/// Serves concurrent reads, but expects a single writer.
pub struct Marble {
    // maps from PageId to DiskLocation
    pt: RwLock<Lsm>,
    fams: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    next_file_lsn: Mutex<u64>,
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

        let _file_lock = file_lock_opts.open(config.path.join(LOCK_SUFFIX))?;
        _file_lock.try_lock_exclusive()?;

        // recover page location index
        let pt = Lsm::recover(config.path.join(PT_DIR_SUFFIX))?;

        // NB LSN should initially be 1, not 0, because 0 represents
        // a page being free.
        let recovered_pt_lsn = pt.get(PT_LSN_KEY).load(Ordering::Acquire).max(1);

        // parse file names
        // calculate file tenancy

        let mut fams = BTreeMap::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        for entry_res in fs::read_dir(heap_dir)? {
            let entry = entry_res?;
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

                fs::remove_file(entry.path())?;
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

            let shard = u8::from_str_radix(&splits[0], 16)
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
                fs::remove_file(entry.path())?;
                continue;
            }

            let mut options = OpenOptions::new();
            options.read(true);

            let file = options.open(entry.path())?;
            let location = DiskLocation(lsn);

            let file_size = entry.metadata()?.len();
            max_file_size = max_file_size.max(file_size);
            max_file_lsn = max_file_lsn.max(lsn);

            let fam = FileAndMetadata {
                len: 0.into(),
                capacity,
                path: entry.path().into(),
                file,
                location,
                generation,
                shard,
            };

            assert!(fams.insert(location, fam).is_none());
        }

        let next_file_lsn = max_file_lsn + max_file_size + 1;

        // initialize file tenancy from pt

        for pid in 0..pt.approximate_max_child_count() {
            let location = pt.get(pid).load(Ordering::Acquire);
            if location != 0 {
                let (_, fam) = fams.range(..=DiskLocation(location)).next_back().unwrap();
                fam.len.fetch_add(1, Ordering::Acquire);
            }
        }

        if let Some((_, lsn_fam)) = fams.range(..=DiskLocation(recovered_pt_lsn)).next_back() {
            lsn_fam.len.fetch_add(1, Ordering::Acquire);
        }

        Ok(Marble {
            pt: RwLock::new(pt),
            fams: RwLock::new(fams),
            next_file_lsn: Mutex::new(next_file_lsn),
            config,
            _file_lock,
        })
    }

    pub fn read(&self, pid: PageId) -> io::Result<Arc<[u8]>> {
        let fams = self.fams.read().unwrap();

        let pt = self.pt.read().unwrap();
        let lsn = pt.get(pid.0).load(Ordering::Acquire);
        drop(pt);

        assert_ne!(lsn, 0);
        let location = DiskLocation(lsn);

        let (base_location, fam) = fams.range(..=location).next_back().unwrap();

        let file_offset = lsn - base_location.0;
        let page_offset = file_offset + HEADER_LEN as u64;
        let file = &fam.file;

        let mut header_buf = [0_u8; HEADER_LEN];
        file.read_exact_at(&mut header_buf, file_offset)?;

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

        let mut page_buf: Arc<[u8]> = vec![0_u8; len].into();
        let page_buf_ref = Arc::get_mut(&mut page_buf).unwrap();

        file.read_exact_at(page_buf_ref, page_offset)?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&len_buf);
        hasher.update(&pid_buf);
        hasher.update(&page_buf);
        let crc_actual: u32 = hasher.finalize();

        if crc_expected != crc_actual {
            eprintln!(
                "crc mismatch when reading page at offset {} in file {:?}",
                page_offset, fam.path
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, "crc mismatch"));
        }

        Ok(page_buf)
    }

    /// Submit a new batch of pages
    pub fn write_batch(&self, pages: HashMap<PageId, Vec<u8>>) -> io::Result<()> {
        let gen = 0;
        self.shard_batch(pages, gen)
    }

    fn shard_batch(&self, pages: HashMap<PageId, Vec<u8>>, gen: u8) -> io::Result<()> {
        let mut shards: HashMap<u8, HashMap<PageId, Vec<u8>>> = HashMap::new();

        for (pid, data) in pages {
            let shard_id = (self.config.partition_function)(pid, data.len(), gen);

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
        pages: HashMap<PageId, Vec<u8>>,
        gen: u8,
        shard: u8,
    ) -> io::Result<()> {
        let mut next_file_lsn = self.next_file_lsn.lock().unwrap();
        let lsn = *next_file_lsn;

        let mut new_locations: Vec<(PageId, DiskLocation)> = vec![];
        let mut buf = vec![];

        // NB capacity starts with 1 due to the max LSN key that is always included
        let mut capacity = 1;
        for (pid, raw_page) in pages {
            capacity += 1;
            let address = DiskLocation(lsn + buf.len() as u64);
            new_locations.push((pid, address));

            let len_buf: [u8; 8] = (raw_page.len() as u64).to_le_bytes();
            let pid_buf: [u8; 8] = pid.0.to_le_bytes();

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&len_buf);
            hasher.update(&pid_buf);
            hasher.update(&raw_page);
            let crc: u32 = hasher.finalize();

            buf.write_all(&crc.to_le_bytes())?;
            buf.write_all(&pid_buf)?;
            buf.write_all(&len_buf)?;
            buf.write_all(&raw_page)?;
        }

        *next_file_lsn += buf.len() as u64 + 1;
        drop(next_file_lsn);

        let fname = format!("{:02x}-{:016x}-{:01x}-{:016x}", shard, lsn, gen, capacity);

        let tmp_fname = format!("{}-tmp", fname);

        let new_path = self.config.path.join(HEAP_DIR_SUFFIX).join(fname);
        let tmp_path = self.config.path.join(HEAP_DIR_SUFFIX).join(tmp_fname);

        let mut tmp_options = OpenOptions::new();
        tmp_options.read(false).write(true).create(true);

        let mut tmp_file = tmp_options.open(&tmp_path)?;

        tmp_file.write_all(&buf)?;
        drop(buf);

        // mv and fsync new file and directory

        tmp_file.sync_all()?;
        drop(tmp_file);

        fs::rename(tmp_path, &new_path)?;

        let mut new_options = OpenOptions::new();
        new_options.read(true);

        let new_file = new_options.open(&new_path)?;

        let fam = FileAndMetadata {
            file: new_file,
            capacity,
            len: capacity.into(),
            generation: gen,
            location: DiskLocation(lsn),
            path: new_path,
            shard,
        };

        assert!(self
            .fams
            .write()
            .unwrap()
            .insert(fam.location, fam)
            .is_none());

        File::open(self.config.path.join(HEAP_DIR_SUFFIX)).and_then(|f| f.sync_all())?;

        // write a batch of updates to the pt

        let write_batch: Vec<(u64, Option<u64>)> = new_locations
            .into_iter()
            .map(|(pid, location)| {
                let key = pid.0;
                let value = Some(location.0);
                (key, value)
            })
            .chain(std::iter::once({
                // always mark the lsn w/ the pt batch
                let key = PT_LSN_KEY;
                let value = Some(lsn);
                (key, value)
            }))
            .collect();

        let mut pt = self.pt.write().unwrap();
        let replaced_locations = pt.write_batch(&write_batch)?;
        pt.flush()?;
        drop(pt);

        let fams = self.fams.read().unwrap();
        for old_location in replaced_locations {
            let (_, fam) = fams
                .range(..=DiskLocation(old_location))
                .next_back()
                .unwrap();

            let old = fam.len.fetch_sub(1, Ordering::Relaxed);
            assert_ne!(old, 0);
        }

        Ok(())
    }

    pub fn maintenance(&self) -> io::Result<()> {
        // TODO make this concurrency-friendly, because right now it blocks everything

        // scan files, filter by fragmentation, group by
        // generation and size class

        let mut files_to_defrag = vec![];
        let mut paths_to_remove = vec![];

        let fams = self.fams.read().unwrap();
        for (_, meta) in &*fams {
            let len = meta.len.load(Ordering::Acquire);
            let cap = meta.capacity.max(1);

            // println!("file {:?} len: {} cap: {}", meta.path, len, cap);
            if len == 0 {
                paths_to_remove.push((meta.location, meta.path.clone()));
            } else if (len * 100) / cap < u64::from(self.config.file_compaction_percent) {
                paths_to_remove.push((meta.location, meta.path.clone()));
                files_to_defrag.push((meta.location.0, meta.path.clone()));
            }
        }

        let pt = self.pt.read().unwrap();

        let mut batch = HashMap::new();

        // rewrite the live pages
        for (base_lsn, path) in &files_to_defrag {
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

                let current_location = pt.get(pid).load(Ordering::Acquire);

                if lsn == current_location {
                    // can attempt to rewrite
                    batch.insert(PageId(pid), page_buf);
                } else {
                    //
                }

                offset += HEADER_LEN + len;
            }
        }

        drop(pt);
        drop(fams);

        io_try!(self.write_batch(batch));

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

#[test]
fn test_01() {
    // fs::remove_dir_all("test_01");
    let mut m = Marble::open("test_01").unwrap();

    for i in 0_u64..10 {
        let start = i * 10;
        let end = (i + 1) * 10;

        let mut batch = HashMap::new();
        for pid in start..end {
            let value = pid
                .to_be_bytes()
                .into_iter()
                .cycle()
                .take(pid as usize)
                .collect();
            batch.insert(PageId(pid), value);
        }

        m.write_batch(batch).unwrap();
    }

    for pid in 0..100 {
        let read = m.read(PageId(pid)).unwrap();
        let expected = pid
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(pid as usize)
            .collect::<Vec<_>>();
        assert_eq!(&*read, &expected[..]);
    }

    for i in 0_u64..10 {
        let start = i * 10;
        let end = (i + 1) * 10;

        let mut batch = HashMap::new();
        for pid in start..end {
            let value = pid
                .to_be_bytes()
                .into_iter()
                .cycle()
                .take(pid as usize)
                .collect();
            batch.insert(PageId(pid), value);
        }

        m.write_batch(batch).unwrap();
    }

    m.maintenance().unwrap();

    drop(m);
    m = Marble::open("test_01").unwrap();

    for pid in 0..100 {
        let read = m.read(PageId(pid)).unwrap();
        let expected = pid
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(pid as usize)
            .collect::<Vec<_>>();
        assert_eq!(&*read, &expected[..]);
    }
}
