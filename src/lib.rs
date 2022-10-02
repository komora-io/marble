//! # Marble
//!
//! Marble is a low-level object store that can be used
//! to build your own storage engines and databases on
//! top of.
//!
//! At a high-level, it supports atomic batch writes and
//! single-object reads. Garbage collection is manual.
//! All operations are blocking. Nothing is cached
//! in-memory except for zstd dictionaries and file
//! handles to all storage files. Objects may be
//! sharded upon GC by providing a custom
//! `Config::partition_function`. Partitioning
//! is not performed on the write batch when it
//! is initially written, because the write batch
//! must be stored in a single file for it to be atomic.
//! But when future calls to `Marble::maintenance`
//! defragment the storage files by rewriting objects
//! that are still live, it will use this function
//! to assign the rewritten objects into a particular
//! partition.
//!
//! You should think of Marble as the heap that you
//! flush your write-ahead logs into periodically.
//! It will create a new file for each write batch,
//! and this might actually expand to more files after
//! garbage collection if the batch is significantly
//! larger than the `Config::target_file_size`.
//!
//! Marble does not create any threads or call
//! `Marble::maintenance` automatically under any
//! conditions. You should probably create a background
//! thread that calls this periodically.
//!
//! Pretty much the only "fancy" thing that Marble does
//! is that it can be configured to create a zstd dictionary
//! that is tuned specifically to your write batches.
//! This is disabled by default and can be configured
//! by setting the `Config::zstd_compression_level` to
//! something other than `None` (the level is passed
//! directly to zstd during compression). Compression is
//! bypassed if batches have fewer than 8 items or the
//! average item length is less than or equal to 8.
//!
//! # Examples
//!
//! ```
//! let marble = marble::open("heap").unwrap();
//!
//! // Write new data keyed by a `u64` object ID.
//! // Batches contain insertions and deletions
//! // based on whether the value is a Some or None.
//! marble.write_batch([
//!     (0_u64, Some(&[32_u8] as &[u8])),
//!     (4_u64, None),
//! ]).unwrap();
//!
//! // read it back
//! assert_eq!(marble.read(0).unwrap(), Some(vec![32].into_boxed_slice()));
//! assert_eq!(marble.read(4).unwrap(), None);
//! assert_eq!(marble.read(6).unwrap(), None);
//!
//! // after a few more batches that may have caused fragmentation
//! // by overwriting previous objects, perform maintenance which
//! // will defragment the object store based on `Config` settings.
//! let objects_defragmented = marble.maintenance().unwrap();
//!
//! // print out system statistics
//! dbg!(marble.stats());
//! # drop(marble);
//! # std::fs::remove_dir_all("heap").unwrap();
//! ```
//!
//! which prints out something like
//! ```txt,no_run
//! marble.stats() = Stats {
//!     live_objects: 1048576,
//!     stored_objects: 1181100,
//!     dead_objects: 132524,
//!     live_percent: 88,
//!     files: 11,
//! }
//! ```
//!
//! If you want to customize the settings passed to Marble,
//! you may specify your own `Config`:
//!
//! ```
//! let config = marble::Config {
//!     path: "my_path".into(),
//!     zstd_compression_level: Some(7),
//!     fsync_each_batch: true,
//!     target_file_size: 64 * 1024 * 1024,
//!     file_compaction_percent: 50,
//!     ..Default::default()
//! };
//!
//! let marble = config.open().unwrap();
//! # drop(marble);
//! # std::fs::remove_dir_all("my_path").unwrap();
//! ```
//!
//! A custom GC sharding function may be provided
//! for partitioning objects based on the object ID
//! and size. This may be useful if your higher-level
//! system allocates certain ranges of object IDs for
//! certain types of objects that you would like to
//! group together in the hope of grouping items together
//! that have similar fragmentation properties (similar
//! expected lifespan etc...). This will only shard
//! objects when they are defragmented through the
//! `Marble::maintenance` method, because each new
//! write batch must be written together in one
//! file to retain write batch atomicity in the
//! face of crashes.
//!
//! ```
//! // This function shards objects into partitions
//! // similarly to a slab allocator that groups objects
//! // into size buckets based on powers of two.
//! fn shard_by_size(object_id: u64, object_size: usize) -> u8 {
//!     let next_po2 = object_size.next_power_of_two();
//!     u8::try_from(next_po2.trailing_zeros()).unwrap()
//! }
//!
//! let config = marble::Config {
//!     path: "my_sharded_path".into(),
//!     partition_function: shard_by_size,
//!     ..Default::default()
//! };
//!
//! let marble = config.open().unwrap();
//! # drop(marble);
//! # std::fs::remove_dir_all("my_sharded_path").unwrap();
//! ```
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    RwLock,
};

use fault_injection::fallible;

#[cfg(not(feature = "runtime_validation"))]
type Map<K, V> = std::collections::HashMap<K, V>;

#[cfg(feature = "runtime_validation")]
type Map<K, V> = std::collections::BTreeMap<K, V>;

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
mod zstd;

pub use config::Config;
use debug_delay::debug_delay;
use disk_location::{DiskLocation, RelativeDiskLocation};
use location_table::LocationTable;
use trailer::{read_trailer, write_trailer};
use zstd::ZstdDict;

const HEADER_LEN: usize = 20;
const NEW_WRITE_BATCH_BIT: u64 = 1 << 62;
const NEW_WRITE_BATCH_MASK: u64 = u64::MAX - NEW_WRITE_BATCH_BIT;

type ObjectId = u64;

fn uninit_boxed_slice(len: usize) -> Box<[u8]> {
    use std::alloc::{alloc, Layout};

    let layout = Layout::array::<u8>(len).unwrap();

    unsafe {
        let ptr = alloc(layout);
        let slice = std::slice::from_raw_parts_mut(ptr, len);
        Box::from_raw(slice)
    }
}

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
    /// The percentage of all objects on disk that are
    /// live. This is another way of expressing fragmentation.
    pub live_percent: u8,
    /// The number of backing storage files that exist and are
    /// being held open.
    pub files: usize,
}

#[derive(Default, Debug, Clone, Copy)]
struct Metadata {
    lsn: u64,
    trailer_offset: u64,
    present_objects: u64,
    generation: u8,
}

impl Metadata {
    fn parse(name: &str) -> Option<Metadata> {
        let mut splits = name.split("-");

        Some(Metadata {
            lsn: u64::from_str_radix(&splits.next()?, 16).ok()?,
            trailer_offset: u64::from_str_radix(&splits.next()?, 16).ok()?,
            present_objects: u64::from_str_radix(&splits.next()?, 16).ok()?,
            generation: u8::from_str_radix(splits.next()?, 16).ok()?,
        })
    }

    fn to_file_name(&self) -> String {
        let ret = format!(
            "{:016x}-{:016x}-{:016x}-{:01x}",
            self.lsn, self.trailer_offset, self.present_objects, self.generation
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

impl std::fmt::Debug for Marble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Marble")
            .field("stats", &self.stats())
            .finish()
    }
}

impl std::fmt::Display for Marble {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Marble {{ ... }}")
    }
}

impl Marble {
    /// Statistics about current files, intended to inform
    /// decisions about when to call `maintenance` based on
    /// desired write and space amplification
    /// characteristics.
    #[doc(alias = "file_statistics")]
    #[doc(alias = "statistics")]
    #[doc(alias = "metrics")]
    #[doc(alias = "info")]
    pub fn stats(&self) -> Stats {
        let mut live_objects = 0;
        let mut stored_objects = 0;

        let fams = self.fams.read().unwrap();

        for (_, fam) in &*fams {
            live_objects += fam.len.load(SeqCst);
            stored_objects += fam.metadata.present_objects;
        }

        Stats {
            live_objects,
            stored_objects,
            dead_objects: stored_objects - live_objects,
            live_percent: u8::try_from((live_objects * 100) / stored_objects.max(1)).unwrap(),
            files: fams.len(),
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

    /// Intended for use in recovery, to bootstrap a higher level object ID allocator.
    ///
    /// Returns a tuple of 1 higher than the current max allocated object ID,
    /// and an iterator over all object IDs beneath that which are
    /// currently deleted (due to being stored as a `None` in a write batch).
    pub fn free_object_ids<'a>(&'a self) -> (u64, impl 'a + Iterator<Item = u64>) {
        let max = self.location_table.max_object_id() + 1;
        let iter = (0..max).filter_map(|oid| {
            if self.location_table.load(oid).is_none() {
                Some(oid)
            } else {
                None
            }
        });
        (max, iter)
    }

    fn verify_file_uninhabited(
        &self,
        _location: DiskLocation,
        _fams: &BTreeMap<DiskLocation, FileAndMetadata>,
    ) {
        #[cfg(feature = "runtime_validation")]
        {
            let fam = &_fams[&_location];
            let next_location =
                DiskLocation::new_fam(_location.lsn() + fam.metadata.trailer_offset);
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
