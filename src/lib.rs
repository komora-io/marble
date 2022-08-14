use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::os::unix::fs::{FileExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    RwLock,
};

use fault_injection::{annotate, fallible, maybe};

type Map<K, V> = std::collections::BTreeMap<K, V>;

mod config;
mod disk_location;
mod location_table;

pub use config::Config;
use disk_location::{DiskLocation, RelativeDiskLocation};
use location_table::LocationTable;

const HEAP_DIR_SUFFIX: &str = "heap";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const HEADER_LEN: usize = 20;
const MAX_GENERATION: u8 = 3;

type ObjectId = u64;

/*
fn tn() -> String {
    std::thread::current()
        .name()
        .unwrap_or("unknown")
        .to_owned()
}
*/

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

fn read_trailer(
    file: &File,
    trailer_offset: u64,
    trailer_items: u64,
) -> io::Result<Vec<(ObjectId, RelativeDiskLocation)>> {
    let size = usize::try_from(4 + (trailer_items * 16)).unwrap();

    let mut buf = Vec::with_capacity(size);

    unsafe {
        buf.set_len(size);
    }

    fallible!(file.read_exact_at(&mut buf, trailer_offset));

    let expected_crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);

    let actual_crc = crc32fast::hash(&buf[4..]);

    if actual_crc != expected_crc {
        return Err(annotate!(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("crc mismatch for object file trailer, expected {expected_crc} but got {actual_crc} for buffer of length {} trailer items {trailer_items}", buf.len() - 4)
        )));
    }

    let mut ret = Vec::with_capacity(usize::try_from(trailer_items).unwrap());

    for sub_buf in buf[4..].chunks(16) {
        let object_id = u64::from_le_bytes(sub_buf[..8].try_into().unwrap());
        let raw_relative_loc = u64::from_le_bytes(sub_buf[8..].try_into().unwrap());
        let relative_loc = RelativeDiskLocation::from_raw(raw_relative_loc);

        ret.push((object_id, relative_loc));
    }

    Ok(ret)
}

fn write_trailer<'a>(
    file: &mut File,
    offset: u64,
    new_shifted_relative_locations: &Map<ObjectId, RelativeDiskLocation>,
) -> io::Result<()> {
    // space for overall crc + each (object_id, location) pair
    let mut buf = Vec::with_capacity(4 + (new_shifted_relative_locations.len() * 16));
    // space for crc
    buf.extend_from_slice(&[0; 4]);

    for (object_id, relative_location) in new_shifted_relative_locations {
        let object_id_bytes: &[u8; 8] = &object_id.to_le_bytes();
        let loc_bytes: &[u8; 8] = &relative_location.to_raw().to_le_bytes();
        buf.extend_from_slice(object_id_bytes);
        buf.extend_from_slice(loc_bytes)
    }

    let crc = crc32fast::hash(&buf[4..]);
    let crc_bytes = crc.to_le_bytes();

    buf[0..4].copy_from_slice(&crc_bytes);

    fallible!(file.write_all_at(&buf, offset));

    fallible!(file.flush());

    Ok(())
}

fn read_storage_directory(heap_dir: PathBuf) -> io::Result<Vec<(Metadata, u64, fs::DirEntry)>> {
    let mut files = vec![];
    // parse file names
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

        let metadata = match Metadata::parse(name) {
            Some(mn) => mn,
            None => {
                log::error!(
                    "encountered strange file in internal directory: {:?}",
                    entry.path(),
                );
                continue;
            }
        };

        let file_len = fallible!(entry.metadata()).len();
        let trailer_offset = file_len as u64 - (4 + (metadata.trailer_items * 16));

        files.push((metadata, trailer_offset, entry));
    }

    files.sort_by_key(|(metadata, _, _)| metadata.lsn);

    Ok(files)
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
    location_table: LocationTable,
    next_file_lsn: AtomicU64,
    fams: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    config: Config,
    directory_lock: File,
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

        let directory_lock = fallible!(File::open(config.path.join(HEAP_DIR_SUFFIX)));
        fallible!(directory_lock.try_lock_exclusive());

        let mut fams = BTreeMap::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        let mut recovery_page_table = Map::new();

        let files = read_storage_directory(heap_dir)?;

        for (metadata, trailer_offset, entry) in files {
            let mut options = OpenOptions::new();
            options.read(true);

            let mut file = fallible!(options.open(entry.path()));

            let trailer = read_trailer(&mut file, trailer_offset, metadata.trailer_items)?;

            for (object_id, relative_loc) in trailer {
                // add file base LSN to relative offset
                let location = relative_loc.to_absolute(metadata.lsn);

                if let Some(old) = recovery_page_table.insert(object_id, location) {
                    assert!(
                        old < location,
                        "must always apply locations in monotonic order"
                    );
                }
            }

            let file_size = fallible!(entry.metadata()).len();
            max_file_size = max_file_size.max(file_size);
            max_file_lsn = max_file_lsn.max(metadata.lsn);

            let file_location = DiskLocation::new_fam(metadata.lsn);

            assert_ne!(metadata.trailer_items, 0);

            let fam = FileAndMetadata {
                len: 0.into(),
                metadata: metadata,
                trailer_offset,
                path: entry.path().into(),
                file,
                location: file_location,
                generation: metadata.generation,
                rewrite_claim: false.into(),
                synced: true.into(),
            };

            log::debug!("inserting new fam at location {:?}", file_location);
            assert!(fams.insert(file_location, fam).is_none());
        }

        let location_table = LocationTable::default();

        // initialize fam utilization from page table
        for (object_id, disk_location) in recovery_page_table {
            let (_l, fam) = fams
                .range((Unbounded, Included(disk_location)))
                .next_back()
                .unwrap();
            fam.len.fetch_add(1, Ordering::Release);
            location_table.store(object_id, disk_location);
        }

        let next_file_lsn = AtomicU64::new(max_file_lsn + max_file_size + 1);

        Ok(Marble {
            location_table,
            fams: RwLock::new(fams),
            next_file_lsn,
            config,
            directory_lock,
        })
    }

    /// Read a object out of storage. If this object is
    /// unknown or has been removed, returns `Ok(None)`.
    ///
    /// May be called concurrently with background calls to
    /// `maintenance` and `write_batch`.
    pub fn read(&self, object_id: ObjectId) -> io::Result<Option<Vec<u8>>> {
        let fams = self.fams.read().unwrap();

        let location = if let Some(location) = self.location_table.load(object_id) {
            location
        } else {
            return Ok(None);
        };

        if location.is_delete() {
            return Ok(None);
        }

        let (base_location, fam) = fams
            .range((Unbounded, Included(location)))
            .next_back()
            .expect("no possible storage file for object - likely file corruption");

        let file_offset = location.lsn() - base_location.lsn();

        let mut header_buf = [0_u8; HEADER_LEN];
        fallible!(fam.file.read_exact_at(&mut header_buf, file_offset));

        let crc_expected: [u8; 4] = header_buf[0..4].try_into().unwrap();
        let pid_buf: [u8; 8] = header_buf[4..12].try_into().unwrap();
        let len_buf: [u8; 8] = header_buf[12..].try_into().unwrap();

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

        let crc_actual = hash(len_buf, pid_buf, &object_buf);

        if crc_expected != crc_actual {
            log::warn!(
                "crc mismatch when reading object at offset {} in file {:?}",
                object_offset,
                file_offset
            );
            return Err(annotate!(io::Error::new(
                io::ErrorKind::InvalidData,
                "crc mismatch",
            )));
        }

        let read_pid = u64::from_le_bytes(pid_buf);

        assert_eq!(object_id, read_pid);

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
            stored_objects += fam.metadata.present_objects;
        }

        FileStats {
            live_objects,
            stored_objects,
            dead_objects: stored_objects - live_objects,
        }
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
        let old_locations = Map::new();
        self.shard_batch(write_batch, gen, &old_locations)
    }

    fn shard_batch<B, I>(
        &self,
        write_batch: I,
        gen: u8,
        old_locations: &Map<ObjectId, DiskLocation>,
    ) -> io::Result<()>
    where
        B: AsRef<[u8]>,
        I: IntoIterator<Item = (ObjectId, Option<B>)>,
    {
        // maps from shard -> (shard size, map of object
        // id's to object data)
        let mut shards: Map<u8, (usize, Map<ObjectId, Option<B>>)> = Map::new();

        let mut fragmented_shards = vec![];

        for (object_id, data_opt) in write_batch {
            let (object_size, shard_id) = if let Some(ref data) = data_opt {
                let len = data.as_ref().len();
                let shard = if gen == 0 {
                    // only shard during gc defragmentation of
                    // rewritten items, otherwise we break
                    // writebatch atomicity
                    0
                } else {
                    (self.config.partition_function)(object_id, len)
                };
                (len + HEADER_LEN, shard)
            } else {
                (0, 0)
            };

            let shard = shards.entry(shard_id).or_default();

            let is_rewrite = gen > 0;
            let over_size_preference = shard.0 > self.config.target_file_size;

            if is_rewrite && over_size_preference {
                fragmented_shards.push((shard_id, std::mem::take(&mut shard.1)));
                shard.0 = 0;
            }

            shard.0 += object_size;
            shard.1.insert(object_id, data_opt);
        }

        let iter = shards
            .into_iter()
            .map(|(_shard, (_sz, objects))| objects)
            .chain(
                fragmented_shards
                    .into_iter()
                    .map(|(_shard, objects)| objects),
            );

        for objects in iter {
            self.write_batch_inner(objects, gen, &old_locations)?;
        }

        // fsync directory to ensure new file is present
        if self.config.fsync_each_batch {
            fallible!(self.directory_lock.sync_all());
        }

        Ok(())
    }

    fn write_batch_inner<B>(
        &self,
        objects: Map<ObjectId, Option<B>>,
        generation: u8,
        old_locations: &Map<ObjectId, DiskLocation>,
    ) -> io::Result<()>
    where
        B: AsRef<[u8]>,
    {
        // allocates unique temporary file names
        static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

        assert!(!objects.is_empty());

        // Common write path:
        // 1. write data to tmp
        // 2. assign LSN and add to fams
        // 3. attempt installation into pagetable
        // 4. create trailer based on pagetable installation success
        // 5. write trailer then rename file
        // 6. update replaced / contention-related failures

        // 1. write data to tmp
        let tmp_file_name = format!("{}-tmp", TMP_COUNTER.fetch_add(1, Ordering::AcqRel));
        let tmp_path = self.config.path.join(HEAP_DIR_SUFFIX).join(tmp_file_name);

        let mut file_options = OpenOptions::new();
        file_options.read(true).write(true).create(true);

        let file = fallible!(file_options.open(&tmp_path));
        let mut buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        let mut new_relative_locations: Map<ObjectId, RelativeDiskLocation> = Map::new();

        let mut written_bytes: u64 = 0;
        for (object_id, raw_object_opt) in &objects {
            let raw_object = if let Some(raw_object) = raw_object_opt {
                raw_object.as_ref()
            } else {
                let is_delete = true;
                new_relative_locations.insert(*object_id, RelativeDiskLocation::new(0, is_delete));
                continue;
            };

            if raw_object.len() > self.config.max_object_size {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "{:?} in write batch has a size of {}, which is larger than the \
                         configured `max_object_size` of {}. If this is intentional, please \
                         increase the configured `max_object_size`.",
                        object_id,
                        raw_object.len(),
                        self.config.max_object_size,
                    ),
                ));
            }

            let relative_address = written_bytes;

            let is_delete = false;
            let relative_location = RelativeDiskLocation::new(relative_address, is_delete);
            new_relative_locations.insert(*object_id, relative_location);

            let len_buf: [u8; 8] = (raw_object.len() as u64).to_le_bytes();
            let pid_buf: [u8; 8] = object_id.to_le_bytes();

            let crc = hash(len_buf, pid_buf, &raw_object);

            log::trace!(
                "writing object {} at offset {} with crc {:?}",
                object_id,
                written_bytes,
                crc
            );

            fallible!(buf_writer.write_all(&crc));
            fallible!(buf_writer.write_all(&pid_buf));
            fallible!(buf_writer.write_all(&len_buf));
            fallible!(buf_writer.write_all(&raw_object));

            written_bytes += (HEADER_LEN + raw_object.len()) as u64;
        }

        assert_eq!(new_relative_locations.len(), objects.len());

        fallible!(buf_writer.flush());

        let file: File = buf_writer
            .into_inner()
            .expect("BufWriter::into_inner should not fail after an explicit flush");

        let mut file_2: File = fallible!(file.try_clone());

        if self.config.fsync_each_batch {
            fallible!(file.sync_all());
        }

        // 2. assign LSN and add to fams
        let mut fams = self.fams.write().unwrap();

        let lsn = self
            .next_file_lsn
            .fetch_add(written_bytes + 1, Ordering::AcqRel);

        let location = DiskLocation::new_fam(lsn);
        let initial_capacity = new_relative_locations.len() as u64;

        let fam = FileAndMetadata {
            file,
            metadata: Metadata::default(),
            trailer_offset: written_bytes,
            len: initial_capacity.into(),
            generation,
            location,
            synced: self.config.fsync_each_batch.into(),
            // set path to empty and rewrite_claim to true so
            // that nobody tries to concurrently do maintenance
            // on us until we're done with our write operation.
            path: PathBuf::new(),
            rewrite_claim: true.into(),
        };

        log::debug!("inserting new fam at lsn {lsn} location {location:?}",);

        assert!(fams.insert(location, fam).is_none());

        drop(fams);

        assert_ne!(lsn, 0);

        // 3. attempt installation into pagetable

        let mut replaced_locations: Vec<DiskLocation> =
            Vec::with_capacity(usize::try_from(initial_capacity).unwrap());
        let mut failed_installations = vec![];

        for (object_id, new_relative_location) in &new_relative_locations {
            let new_location = new_relative_location.to_absolute(lsn);

            let (success, replaced_location_opt): (bool, Option<DiskLocation>) = if let Some(
                old_location,
            ) =
                old_locations.get(&object_id)
            {
                // CAS it
                let res = self
                    .location_table
                    .cas(*object_id, *old_location, new_location);

                match res {
                    Ok(replaced_opt) => {
                        log::trace!("cas of {object_id} from old location {old_location:?} to new location {new_location:?} successful");
                        (true, Some(replaced_opt))
                    }
                    Err(_current_opt) => {
                        log::trace!("cas of {object_id} from old location {old_location:?} to new location {new_location:?} failed");
                        (false, None)
                    }
                }
            } else {
                // fetch_max it
                let res = self.location_table.fetch_max(*object_id, new_location);
                if let Ok(old_opt) = res {
                    log::trace!(
                        "fetch_max of {object_id} to new location {new_location:?} successful"
                    );
                    (true, old_opt)
                } else {
                    log::trace!("fetch_max of {object_id} to new location {new_location:?} failed");
                    (false, None)
                }
            };

            if let Some(replaced_location) = replaced_location_opt {
                assert!(success);

                log::trace!(
                    "updating metadata for object_id {} from {:?} to {:?}.",
                    object_id,
                    replaced_location_opt,
                    new_location,
                );
                replaced_locations.push(replaced_location);
            } else if !success {
                log::trace!(
                    "NOT updating metadata for object_id {} from {:?} to {:?}.",
                    object_id,
                    replaced_location_opt,
                    new_location,
                );
                failed_installations.push(*object_id);
            }
        }

        for failed_installation in &failed_installations {
            new_relative_locations.remove(failed_installation).unwrap();
        }

        let trailer_items = new_relative_locations.len();

        if trailer_items == 0 {
            let mut fams = self.fams.write().unwrap();

            self.verify_file_uninhabited(location, &fams);

            fams.remove(&location).unwrap();
            fallible!(fs::remove_file(tmp_path));
            return Ok(());
        }

        // 5. write trailer then rename file
        let metadata = Metadata {
            lsn,
            trailer_items: trailer_items as u64,
            present_objects: objects.len() as u64,
            generation,
        };

        let file_name = metadata.to_file_name();
        let new_path = self.config.path.join(HEAP_DIR_SUFFIX).join(file_name);

        log::trace!(
            "writing trailer for {lsn} at offset {}, trailer items {trailer_items}",
            written_bytes,
        );
        let res = write_trailer(&mut file_2, written_bytes, &new_relative_locations)
            .and_then(|_| maybe!(file_2.sync_all()))
            .and_then(|_| maybe!(fs::rename(&tmp_path, &new_path)));

        let file_len = fallible!(file_2.metadata()).len();
        let expected_file_len = written_bytes + 4 + (16 * new_relative_locations.len() as u64);

        assert_eq!(file_len, expected_file_len);
        assert_eq!(trailer_items, new_relative_locations.len());

        fallible!(fs::set_permissions(
            &new_path,
            fs::Permissions::from_mode(0o400)
        ));

        if let Err(e) = res {
            self.fams.write().unwrap().remove(&location).unwrap();
            fallible!(fs::remove_file(tmp_path));
            log::error!("failed to write new file: {:?}", e);
            return Err(e);
        };

        log::trace!("renamed file to {:?}", new_path);

        // 6. update replaced / contention-related failures
        let fams = self.fams.read().unwrap();

        for replaced_location in replaced_locations.into_iter() {
            let (_, fam) = fams
                .range((Unbounded, Included(replaced_location)))
                .next_back()
                .unwrap();

            let old = fam.len.fetch_sub(1, Ordering::AcqRel);
            log::trace!(
                "subtracting one from fam {:?}, current len is {}",
                replaced_location,
                old - 1
            );
            assert_ne!(old, 0);
        }
        drop(fams);

        let mut fams = self.fams.write().unwrap();

        let fam = fams.get_mut(&location).unwrap();
        fam.metadata = metadata;
        assert_ne!(fam.metadata.trailer_items, 0);
        let old = fam
            .len
            .fetch_sub(failed_installations.len() as u64, Ordering::Release);

        assert!(old >= failed_installations.len() as u64);

        fam.path = new_path;
        assert!(fam.rewrite_claim.swap(false, Ordering::AcqRel));
        log::trace!(
            "fams: {:?}",
            fams.iter()
                .map(|(dl, f)| (
                    dl,
                    f.len.load(Ordering::Acquire),
                    f.metadata.trailer_items,
                    f.rewrite_claim.load(Ordering::Acquire),
                ))
                .collect::<Vec<_>>()
        );

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
                log::trace!("fam at location {location:?} is empty, marking it for removal",);
                paths_to_remove.push((*location, fam.path.clone()));
            }
        }

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

        let mut files_to_defrag: Map<u8, Vec<_>> = Default::default();

        let fams = self.fams.read().unwrap();

        for (location, fam) in &*fams {
            assert_eq!(*location, fam.location);
            let len = fam.len.load(Ordering::Acquire);
            let trailer_items = fam.metadata.trailer_items;

            if len != 0
                && (len * 100) / trailer_items.max(1)
                    < u64::from(self.config.file_compaction_percent)
            {
                let already_claimed = fam.rewrite_claim.swap(true, Ordering::SeqCst);
                if already_claimed {
                    // try to exclusively claim this file
                    // for rewrite to
                    // prevent concurrent attempts at
                    // rewriting its contents
                    continue;
                }
                assert_ne!(trailer_items, 0);

                defer_unclaim.claims.push(*location);

                log::trace!(
                    "fam at location {:?} is ready to be compacted",
                    fam.location
                );

                let generation = fam.generation.saturating_add(1).min(MAX_GENERATION);

                let entry = files_to_defrag.entry(generation).or_default();
                entry.push((
                    *location,
                    fam.path.clone(),
                    fam.file.try_clone()?,
                    fam.trailer_offset,
                    trailer_items,
                ));
            }
        }
        drop(fams);

        let mut rewritten_objects = 0;

        // use this old_locations Map in the outer loop to reuse the allocation
        // and avoid resizing as often.
        let mut old_locations: Map<ObjectId, DiskLocation> = Map::new();

        // rewrite the live objects
        for (generation, file_to_defrag) in files_to_defrag {
            old_locations.clear();

            log::trace!(
                "compacting files {:?} with generation {}",
                file_to_defrag,
                generation
            );

            if file_to_defrag.len() < self.config.min_compaction_files {
                // skip batch with too few files (claims
                // auto-released by Drop of DeferUnclaim
                continue;
            }

            let mut batch = Map::new();
            let mut rewritten_fam_locations = vec![];

            for (base_location, path, mut file, trailer_offset, trailer_items) in file_to_defrag {
                log::trace!(
                    "rewriting any surviving objects in file at location {base_location:?}"
                );
                rewritten_fam_locations.push(base_location);

                use std::io::Seek;
                fallible!(file.seek(io::SeekFrom::Start(0)));

                let mut buf_reader = BufReader::new(file);

                let mut offset = 0_u64;

                while offset < trailer_offset {
                    let mut header = [0_u8; HEADER_LEN];
                    let header_res = buf_reader.read_exact(&mut header);

                    match header_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(other) => return Err(other),
                    }

                    let crc_expected: [u8; 4] = header[0..4].try_into().unwrap();
                    let pid_buf = header[4..12].try_into().unwrap();
                    let object_id = u64::from_le_bytes(pid_buf);
                    let len_buf = header[12..20].try_into().unwrap();
                    let len = usize::try_from(u64::from_le_bytes(len_buf)).unwrap();

                    if len >= self.config.max_object_size {
                        log::warn!("corrupt object size detected: {} bytes", len);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "corrupt object size",
                        ));
                    }

                    let current_location = self
                        .location_table
                        .load(object_id)
                        .expect("anything being rewritten should exist in the location table");

                    let rewritten_location =
                        RelativeDiskLocation::new(offset, false).to_absolute(base_location.lsn());

                    // all objects present before the trailer are not deletes
                    let mut object_buf = Vec::with_capacity(len);

                    unsafe {
                        object_buf.set_len(len);
                    }

                    let object_res = buf_reader.read_exact(&mut object_buf);

                    match object_res {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(other) => return Err(other),
                    }

                    let crc_actual = hash(len_buf, pid_buf, &object_buf);

                    if crc_expected != crc_actual {
                        log::error!(
                            "crc mismatch when reading object {} at offset {} in file {:?} - expected {:?} actual {:?}",
                            object_id,
                            offset,
                            path,
                            crc_expected,
                            crc_actual,
                        );
                        return Err(annotate!(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "crc mismatch in maintenance routine",
                        )));
                    }

                    if rewritten_location == current_location {
                        // can attempt to rewrite
                        log::trace!("rewriting object {object_id} at rewritten location {rewritten_location:?}");
                        batch.insert(object_id, Some(object_buf));
                        old_locations.insert(object_id, rewritten_location);
                    } else {
                        log::trace!("not rewriting object {object_id}, as the location being defragmented {rewritten_location:?} does not match the current location in the location table {current_location:?}");
                    }

                    offset += (HEADER_LEN + len) as u64;
                }

                let mut file: File = buf_reader.into_inner();

                log::trace!(
                    "trying to read trailer at file for lsn {} offset {trailer_offset} items {trailer_items}",
                    base_location.lsn(),
                );

                let trailer = read_trailer(&mut file, trailer_offset, trailer_items)?;

                for (object_id, relative_location) in trailer {
                    if relative_location.is_delete() {
                        let rewritten_location = relative_location.to_absolute(base_location.lsn());
                        let current_location = self
                            .location_table
                            .load(object_id)
                            .expect("anything being rewritten should exist in the location table");

                        if rewritten_location == current_location {
                            // can attempt to rewrite
                            log::trace!("rewriting object {object_id} at rewritten location {rewritten_location:?}");
                            batch.insert(object_id, None);
                            old_locations.insert(object_id, rewritten_location);
                        } else {
                            log::trace!("not rewriting object {object_id}, as the location being defragmented {rewritten_location:?} does not match the current location in the location table {current_location:?}");
                        }
                    }
                }
            }

            rewritten_objects += batch.len();

            log::trace!("{rewritten_objects}, {}", batch.len());

            self.shard_batch(batch, generation, &old_locations)?;

            let fams = self.fams.read().unwrap();
            for location in rewritten_fam_locations {
                self.verify_file_uninhabited(location, &fams);
                let fam = &fams[&location];
                assert_eq!(fam.len.load(Ordering::Acquire), 0, "fam: {fam:?}")
            }
        }

        drop(defer_unclaim);

        self.prune_empty_fams()?;

        Ok(rewritten_objects)
    }

    /// If `Config::fsync_each_batch` is `false`, this method can
    /// be called periodically to ensure that the written
    /// batches are durable on disk.
    pub fn sync_all(&self) -> io::Result<()> {
        let fams = self.fams.read().unwrap();

        let mut synced_files = false;
        for fam in fams.values() {
            if !fam.synced.load(Ordering::Acquire) {
                fam.file.sync_all()?;
                fam.synced.store(true, Ordering::Release);
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
        location: DiskLocation,
        fams: &Map<DiskLocation, FileAndMetadata>,
    ) {
        // TODO make this test-only
        let next_location = fams
            .range((Excluded(location), Unbounded))
            .next()
            .unwrap()
            .0;

        let present: Vec<(ObjectId, DiskLocation)> = self
            .location_table
            .iter()
            .filter(|(_oid, loc)| *loc >= location && loc < next_location)
            .collect();

        if !present.is_empty() {
            panic!("orphaned object location pairs in location table: {present:?}, which map to the file we're about to delete: {location:?} which is lower than the next highest location {next_location:?}");
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
