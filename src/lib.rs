use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex, RwLock,
};

mod pt_lsm;
use pt_lsm::Lsm;

// live percentage of a file before it's considered rewritabe
const HEAP_DIR_SUFFIX: &str = "heap";
const PT_DIR_SUFFIX: &str = "page_index";
const LOCK_SUFFIX: &str = "lock";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const PT_LSN_KEY: [u8; 8] = u64::MAX.to_be_bytes();
const PT_LOGICAL_EPOCH_KEY: [u8; 8] = (u64::MAX - 1).to_be_bytes();

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct PageId(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct DiskLocation(u64);

const LOCATION_SZ: usize = std::mem::size_of::<DiskLocation>();

#[derive(Debug)]
struct FileAndMetadata {
    file: File,
    shard: u8,
    location: DiskLocation,
    path: PathBuf,
    capacity: u64,
    len: AtomicU64,
    size_class: u8,
    generation: u8,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub target_file_size: u64,
    pub file_compaction_percent: u8,
    /// A partitioning function for pages based on
    /// page ID, page size, and page rewrite generation.
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

    fn open(self) -> io::Result<Marble> {
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
    files: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    next_file_lsn: Mutex<u64>,
    config: Config,
    file_lock: File,
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

        let file_lock = file_lock_opts.open(config.path.join(LOCK_SUFFIX))?;
        file_lock.try_lock_exclusive()?;

        // recover page location index
        let pt = Lsm::recover(config.path.join(PT_DIR_SUFFIX))?;
        let recovered_pt_lsn = if let Some(max) = pt.get(&PT_LSN_KEY) {
            u64::from_le_bytes(*max)
        } else {
            0
        };

        // parse file names
        // calculate file tenancy

        let mut files = BTreeMap::new();
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
            if splits.len() != 5 {
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
            let size_class = u8::from_str_radix(&splits[2], 16)
                .expect("encountered garbage filename in internal directory");
            let generation = u8::from_str_radix(splits[3], 16)
                .expect("encountered garbage filename in internal directory");
            let capacity = u64::from_str_radix(&splits[4], 16)
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

            let file_and_metadata = FileAndMetadata {
                len: 0.into(),
                capacity,
                path: entry.path().into(),
                file,
                location,
                size_class,
                generation,
                shard,
            };

            files.insert(location, file_and_metadata);
        }

        let next_file_lsn = max_file_lsn + max_file_size + 1;

        Ok(Marble {
            pt: RwLock::new(pt),
            files: RwLock::new(files),
            next_file_lsn: Mutex::new(next_file_lsn),
            config,
            file_lock,
        })
    }

    pub fn read(&self, pid: PageId) -> io::Result<Box<[u8]>> {
        let files = self.files.read().unwrap();

        let pt = self.pt.read().unwrap();
        println!("pt: {:?}", pt[&[0; 8]]);
        let lsn_bytes = *pt.get(&pid.0.to_be_bytes()).unwrap();
        drop(pt);

        let lsn = u64::from_be_bytes(lsn_bytes);
        assert_ne!(lsn, 0);
        let location = DiskLocation(lsn);

        dbg!(&files);

        let (base_location, file_and_metadata) = files.range(..=location).next_back().unwrap();

        dbg!(base_location, file_and_metadata);

        let shard = 0; // todo!();
        let size_class = 0; // todo!();
        let generation = 0; // todo!();

        let file_offset = lsn - base_location.0;
        const HEADER_LEN: usize = 20;
        let page_offset = file_offset + HEADER_LEN as u64;
        let file = &file_and_metadata.file;

        let mut header_buf = [0_u8; HEADER_LEN];
        file.read_exact_at(&mut header_buf, file_offset)?;

        let crc_expected_buf: [u8; 4] = header_buf[0..4].try_into().unwrap();
        let pid_buf: [u8; 8] = header_buf[4..12].try_into().unwrap();
        let len_buf: [u8; 8] = header_buf[12..].try_into().unwrap();

        let crc_expected = u32::from_le_bytes(crc_expected_buf);
        let pid = PageId(u64::from_le_bytes(pid_buf));
        let len: usize = if let Ok(len) = u64::from_le_bytes(len_buf).try_into() {
            len
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "corrupted length detected",
            ));
        };

        let mut page_buf = vec![0; len].into_boxed_slice();

        file.read_exact_at(&mut page_buf, page_offset)?;

        Ok(page_buf)
    }

    pub fn write_batch(&self, pages: HashMap<PageId, Vec<u8>>) -> io::Result<()> {
        let mut next_file_lsn = self.next_file_lsn.lock().unwrap();
        let lsn = *next_file_lsn;
        let size_class = 0; // todo!();
        let gen = 0; // todo!();
        let shard = 0; // todo!();

        let mut new_locations: Vec<(PageId, DiskLocation)> = vec![];
        let mut buf = vec![];

        let mut capacity = 0;
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

        let fname = format!(
            "{:02x}-{:016x}-{:01x}-{:01x}-{:016x}",
            shard, lsn, size_class, gen, capacity
        );

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
            size_class,
        };

        self.files.write().unwrap().insert(fam.location, fam);

        // TODO add new file to self.files with its metadata

        File::open(self.config.path.join(HEAP_DIR_SUFFIX)).and_then(|f| f.sync_all())?;

        // write a batch of updates to the pt

        let write_batch: Vec<([u8; 8], Option<[u8; 8]>)> = new_locations
            .into_iter()
            .map(|(pid, location)| {
                let key = pid.0.to_be_bytes();
                let value = Some(location.0.to_be_bytes());
                (key, value)
            })
            .chain(std::iter::once({
                // always mark the lsn w/ the pt batch
                let key = PT_LSN_KEY;
                let value = Some(lsn.to_le_bytes());
                (key, value)
            }))
            .collect();

        let mut pt = self.pt.write().unwrap();
        pt.write_batch(&write_batch)?;
        pt.flush()?;
        drop(pt);

        Ok(())
    }

    pub fn maintenance(&self) -> io::Result<()> {
        // TODO make this concurrency-friendly, because right now it blocks everything

        // scan files, filter by fragmentation, group by
        // generation and size class

        let mut defrag_shards: HashMap<u8, Vec<PathBuf>> = Default::default();
        let mut locations_to_remove = vec![];
        let mut paths_to_remove = vec![];

        let files = self.files.read().unwrap();
        for (_, meta) in &*files {
            let len = meta.len.load(Ordering::Acquire);
            let cap = meta.capacity.max(1);

            if len == 0 {
                paths_to_remove.push(meta.path.clone());
            } else if (len * 100) / cap < u64::from(self.config.file_compaction_percent) {
                paths_to_remove.push(meta.path.clone());
                locations_to_remove.push(meta.location);
            }
        }

        let pt = self.pt.read().unwrap();

        // rewrite the live pages
        let page_rewrite_iter = FilteredPageRewriteIter::new(&pt, &paths_to_remove);

        let batch = page_rewrite_iter.collect();

        drop(pt);
        drop(files);

        self.write_batch(batch)?;

        // get writer file lock and remove the replaced files

        let mut files = self.files.write().unwrap();

        for location in locations_to_remove {
            files.remove(&location);
        }

        drop(files);

        for path in paths_to_remove {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }
}

struct FilteredPageRewriteIter<'a> {
    pt: &'a Lsm,
    files: Vec<PathBuf>,
}

impl<'a> FilteredPageRewriteIter<'a> {
    fn new(pt: &Lsm, files: &Vec<PathBuf>) -> FilteredPageRewriteIter<'a> {
        todo!()
    }
}

impl<'a> Iterator for FilteredPageRewriteIter<'a> {
    type Item = (PageId, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

#[test]
fn test_01() {
    fs::remove_dir_all("test_01");
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
        println!("{}", pid);
        let read = m.read(PageId(pid)).unwrap();
        let expected = pid
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(pid as usize)
            .collect::<Vec<_>>();
        assert_eq!(&*read, &expected[..]);
    }

    // m.maintenance().unwrap();

    drop(m);
    m = Marble::open("test_01").unwrap();

    for pid in 0..100 {
        println!("{}", pid);
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
