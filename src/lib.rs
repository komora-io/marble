use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

mod pt_lsm;
use pt_lsm::Lsm;

// live percentage of a file before it's considered rewritabe
const MAX_FRAGMENTATION: u64 = 60;
const HEAP_DIR: &str = "heap";
const PT_DIR: &str = "heap";
const PT_LSN_KEY: [u8; 8] = u64::MAX.to_le_bytes();

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub struct PageId(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
struct DiskLocation(u64);

const LOCATION_SZ: usize = std::mem::size_of::<DiskLocation>();

struct FileAndMetadata {
    shard: u8,
    file: File,
    location: DiskLocation,
    path: PathBuf,
    capacity: u64,
    len: AtomicU64,
    size_class: u8,
    generation: u8,
}

struct Marble {
    // maps from PageId to DiskLocation
    pt: Lsm<8, 8>,
    files: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    next_file_lsn: u64,
}

impl Marble {
    fn recover<P: AsRef<Path>>(path: P) -> io::Result<Marble> {
        // recover pt
        // remove files w/ temp name or higher LSN than pt max
        // parse file names
        // calculate file tenancy

        let pt = Lsm::<8, 8>::recover(PT_DIR)?;
        let recovered_pt_lsn = if let Some(max) = pt.get(&PT_LSN_KEY) {
            u64::from_le_bytes(*max)
        } else {
            0
        };

        let mut files = BTreeMap::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        for entry_res in fs::read_dir(HEAP_DIR)? {
            let entry = entry_res?;
            let path = entry.path();
            let name = path
                .file_name()
                .expect("file without name encountered in internal directory")
                .to_str()
                .expect("non-utf8 file name encountered in internal directory");

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

            let shard: u8 = splits[0]
                .parse()
                .expect("encountered garbage filename in internal directory");
            let lsn: u64 = splits[1]
                .parse()
                .expect("encountered garbage filename in internal directory");
            let size_class: u8 = splits[2]
                .parse()
                .expect("encountered garbage filename in internal directory");
            let generation: u8 = splits[3]
                .parse()
                .expect("encountered garbage filename in internal directory");
            let capacity: u64 = splits[4]
                .parse()
                .expect("encountered garbage filename in internal directory");

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
            pt,
            files: RwLock::new(files),
            next_file_lsn,
        })
    }

    fn read(&self, pid: PageId) -> io::Result<Vec<u8>> {
        let files = self.files.read().unwrap();
        todo!()
    }

    fn write_batch(&self, pages: Vec<(PageId, Vec<u8>)>) -> io::Result<()> {
        todo!()
    }

    fn maintenance(&mut self) -> io::Result<()> {
        // TODO make this concurrency-friendly, because right now it blocks everything

        // scan files, filter by fragmentation, group by
        // generation and size class

        let mut files_to_defrag: Vec<&FileAndMetadata> = vec![];
        let mut paths_to_remove = vec![];

        let files = self.files.read().unwrap();
        for (_, meta) in &*files {
            let len = meta.len.load(Ordering::Acquire);
            let cap = meta.capacity.max(1);

            if len == 0 {
                paths_to_remove.push(meta.path.clone());
            } else if (len * 100) / cap < MAX_FRAGMENTATION {
                files_to_defrag.push(meta);
            }
        }

        // write the live pages into a new file w/ temp name

        let shard = 0; // todo!();
        let lsn = self.next_file_lsn;
        let size_class = 0; // todo!();
        let gen = 0; // todo!();

        let page_rewrite_iter = FilteredPageRewriteIter::new(&self.pt, &files_to_defrag);

        let mut new_locations: Vec<(PageId, DiskLocation)> = vec![];
        let mut buf = vec![];

        let mut capacity = 0;
        for (pid, raw_page) in page_rewrite_iter {
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

        self.next_file_lsn += buf.len() as u64 + 1;

        let fname = format!(
            "{:02x}-{:016x}-{:01x}-{:01x}-{:016x}",
            shard, lsn, size_class, gen, capacity
        );

        let tmp_fname = format!("{}-tmp", fname);

        let new_path = Path::new(HEAP_DIR).join(fname);
        let tmp_path = Path::new(HEAP_DIR).join(tmp_fname);

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
        new_options.read(true).write(false).create(false);

        let new_file = new_options.open(new_path)?;
        // TODO add new file to self.files with its metadata

        File::open(HEAP_DIR).and_then(|f| f.sync_all())?;

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

        self.pt.write_batch(&write_batch)?;
        self.pt.flush()?;

        drop(files);

        // get writer file lock and remove the replaced files

        let mut files = self.files.write().unwrap();

        for file in files_to_defrag {
            paths_to_remove.push(file.path);
            files.remove(&file.location);
        }

        drop(files);

        for path in paths_to_remove {
            std::fs::remove_file(path)?;
        }

        Ok(())
    }
}

struct FilteredPageRewriteIter<'a> {
    pt: &'a Lsm<8, 8>,
    files: Vec<&'a FileAndMetadata>,
}

impl<'a> FilteredPageRewriteIter<'a> {
    fn new(pt: &Lsm<8, 8>, files: &Vec<&FileAndMetadata>) -> FilteredPageRewriteIter<'a> {
        todo!()
    }
}

impl<'a> Iterator for FilteredPageRewriteIter<'a> {
    type Item = (PageId, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
