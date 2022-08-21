use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    RwLock,
};

use fault_injection::fallible;

type Map<K, V> = std::collections::HashMap<K, V>;

mod config;
mod debug_delay;
#[cfg(feature = "runtime_validation")]
mod debug_history;
mod disk_location;
mod gc;
mod location_table;
mod readpath;
mod recovery;
mod trailer;
mod writepath;
mod zstd_dict;

pub use config::Config;
use debug_delay::debug_delay;
use disk_location::{DiskLocation, RelativeDiskLocation};
use location_table::LocationTable;
use trailer::{read_trailer, write_trailer};
use zstd_dict::ZstdDict;

const HEADER_LEN: usize = 20;

type ObjectId = u64;

fn hash(len_buf: [u8; 8], pid_buf: [u8; 8], object_buf: &[u8]) -> [u8; 4] {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&len_buf);
    hasher.update(&pid_buf);
    hasher.update(&object_buf);
    let crc: u32 = hasher.finalize();
    crc.to_le_bytes()
}

/// Statistics for file contents, to base decisions around
/// calls to `maintenance`.
#[derive(Debug)]
pub struct Stats {
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

#[derive(Default, Debug, Clone, Copy)]
struct Metadata {
    lsn: u64,
    trailer_items: u64,
    present_objects: u64,
    generation: u8,
}

impl Metadata {
    fn parse(name: &str) -> Option<Metadata> {
        let mut splits = name.split("-");

        Some(Metadata {
            lsn: u64::from_str_radix(&splits.next()?, 16).ok()?,
            trailer_items: u64::from_str_radix(&splits.next()?, 16).ok()?,
            present_objects: u64::from_str_radix(&splits.next()?, 16).ok()?,
            generation: u8::from_str_radix(splits.next()?, 16).ok()?,
        })
    }

    fn to_file_name(&self) -> String {
        let ret = format!(
            "{:016x}-{:016x}-{:016x}-{:01x}",
            self.lsn, self.trailer_items, self.present_objects, self.generation
        );
        ret
    }
}

#[derive(Debug)]
struct FileAndMetadata {
    file: File,
    location: DiskLocation,
    path: PathBuf,
    metadata: Metadata,
    trailer_offset: u64,
    len: AtomicU64,
    generation: u8,
    rewrite_claim: AtomicBool,
    synced: AtomicBool,
    zstd_dict: ZstdDict,
}

/// Shard based on rough size ranges corresponding to SSD
/// page and block sizes
pub fn default_partition_function(_object_id: u64, size: usize) -> u8 {
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

/// Open the system with default configuration at the
/// provided path.
pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Marble> {
    let config = Config {
        path: path.as_ref().into(),
        ..Config::default()
    };

    config.open()
}

/// Garbage-collecting object store. A nice solution to back
/// a pagecache, for people building their own databases.
///
/// Writes should generally be performed by some background
/// process whose job it is to clean logs etc...
pub struct Marble {
    // maps from ObjectId to DiskLocation
    location_table: LocationTable,
    next_file_lsn: AtomicU64,
    fams: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    config: Config,
    directory_lock: File,
    #[cfg(feature = "runtime_validation")]
    debug_history: std::sync::Mutex<debug_history::DebugHistory>,
}

impl Marble {
    /// Statistics about current files, intended to inform
    /// decisions about when to call `maintenance` based on
    /// desired write and space amplification
    /// characteristics.
    #[doc(alias = "file_statistics")]
    #[doc(alias = "statistics")]
    #[doc(alias = "stats")]
    pub fn file_stats(&self) -> Stats {
        let mut live_objects = 0;
        let mut stored_objects = 0;

        for (_, fam) in &*self.fams.read().unwrap() {
            live_objects += fam.len.load(SeqCst);
            stored_objects += fam.metadata.present_objects;
        }

        Stats {
            live_objects,
            stored_objects,
            dead_objects: stored_objects - live_objects,
        }
    }

    fn prune_empty_fams(&self) -> io::Result<()> {
        // get writer file lock and remove the empty fams
        let mut paths_to_remove = vec![];

        let read_fams = self.fams.read().unwrap();

        for (location, fam) in &*read_fams {
            debug_delay();
            if fam.len.load(SeqCst) == 0 && !fam.rewrite_claim.swap(true, SeqCst) {
                log::trace!("fam at location {location:?} is empty, marking it for removal",);
                paths_to_remove.push((*location, fam.path.clone()));
            }
        }

        drop(read_fams);

        let mut fams = self.fams.write().unwrap();

        for (location, _) in &paths_to_remove {
            log::trace!("removing fam at location {:?}", location);

            self.verify_file_uninhabited(*location, &fams);

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
    /// If `Config::fsync_each_batch` is `false`, this
    /// method can be called at a desired interval to
    /// ensure that the written batches are durable on
    /// disk.
    pub fn sync_all(&self) -> io::Result<()> {
        let fams = self.fams.read().unwrap();

        let mut synced_files = false;
        for fam in fams.values() {
            if !fam.synced.load(SeqCst) {
                fam.file.sync_all()?;
                fam.synced.store(true, SeqCst);
                synced_files = true;
            }
        }

        if synced_files {
            fallible!(self.directory_lock.sync_all());
        }

        Ok(())
    }

    fn verify_file_uninhabited(
        &self,
        _location: DiskLocation,
        _fams: &BTreeMap<DiskLocation, FileAndMetadata>,
    ) {
        #[cfg(feature = "runtime_validation")]
        {
            let fam = &_fams[&_location];
            let next_location = DiskLocation::new_fam(_location.lsn() + fam.trailer_offset);
            let present: Vec<(ObjectId, DiskLocation)> = self
                .location_table
                .iter()
                .filter(|(_oid, loc)| *loc >= _location && *loc < next_location)
                .collect();

            if !present.is_empty() {
                panic!(
                    "orphaned object location pairs in location table: {present:?}, which map to \
                     the file we're about to delete: {_location:?} which is lower than the next \
                     highest location {next_location:?}"
                );
            }
        }
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
                debug_delay();
                assert!(fam.rewrite_claim.swap(false, SeqCst));
            }
        }
    }
}

fn _auto_trait_assertions() {
    use core::panic::{RefUnwindSafe, UnwindSafe};

    fn f<T: Send + Sync + UnwindSafe + RefUnwindSafe>() {}

    f::<Marble>();
}
