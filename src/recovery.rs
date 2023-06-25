use std::cmp::Reverse;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Bound::{Included, Unbounded};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicPtr, AtomicU64, Ordering::SeqCst},
    Arc,
};

use concurrent_map::ConcurrentMap;
use fault_injection::fallible;

use crate::{
    read_trailer, Config, DiskLocation, FileAndMetadata, FileMap, LocationTable, Map, Marble,
    Metadata, NEW_WRITE_BATCH_MASK,
};

const HEAP_DIR_SUFFIX: &str = "heap";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const LEGEND: &str = "             lsn   trailer_offset  present_objects generation";

impl Config {
    pub fn open(&self) -> io::Result<Marble> {
        let config = self.clone();

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

        let _ = File::create(config.path.join(HEAP_DIR_SUFFIX).join(LEGEND));
        let _ = File::create(config.path.join(WARN));

        let mut file_lock_opts = OpenOptions::new();
        file_lock_opts.create(true).read(true).write(true);

        let directory_lock = fallible!(File::open(config.path.join(HEAP_DIR_SUFFIX)));
        fallible!(directory_lock.try_lock_exclusive());

        let fams = ConcurrentMap::default();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        let mut recovery_page_table = Map::default();

        let files = read_storage_directory(heap_dir)?;

        for (metadata, entry) in files {
            let mut options = OpenOptions::new();
            options.read(true);

            let mut file = fallible!(options.open(entry.path()));

            let (trailer, zstd_dict) =
                read_trailer(&mut file, metadata.trailer_offset, metadata.file_size)?;

            for (object_id, relative_loc) in trailer {
                // add file base LSN to relative offset
                let location = relative_loc.to_absolute(metadata.lsn);

                log::trace!("inserting object_id {object_id} at location {location:?}");
                if let Some(old) = recovery_page_table.insert(object_id, location) {
                    assert!(
                        (old.lsn() & NEW_WRITE_BATCH_MASK)
                            < (location.lsn() & NEW_WRITE_BATCH_MASK),
                        "must always apply locations in monotonic order. old {old:?} should be < \
                         new {location:?}"
                    );
                }
            }

            let file_size = fallible!(entry.metadata()).len();
            max_file_size = max_file_size.max(file_size);
            max_file_lsn = max_file_lsn.max(metadata.lsn & NEW_WRITE_BATCH_MASK);

            let file_location = DiskLocation::new_fam(metadata.lsn);

            let fam = FileAndMetadata {
                len: 0.into(),
                metadata: AtomicPtr::default(),
                path: AtomicPtr::default(),
                file: file,
                location: file_location,
                generation: metadata.generation,
                rewrite_claim: false.into(),
                synced: true.into(),
                zstd_dict: zstd_dict,
            };

            fam.install_metadata_and_path(metadata, entry.path().into());

            log::debug!("inserting new fam at location {:?}", file_location);
            assert!(fams.insert(Reverse(file_location), Arc::new(fam)).is_none());
        }

        let location_table: LocationTable = LocationTable::default();
        #[cfg(feature = "runtime_validation")]
        let mut debug_history = crate::debug_history::DebugHistory::default();

        // initialize fam utilization from page table
        let mut max_object_id = 0;
        for (object_id, disk_location) in recovery_page_table {
            max_object_id = max_object_id.max(object_id);
            #[cfg(feature = "runtime_validation")]
            debug_history.mark_add(object_id, disk_location);
            let (_l, fam) = fams
                .range((Included(Reverse(disk_location)), Unbounded))
                .next()
                .unwrap();
            fam.len.fetch_add(1, SeqCst);
            location_table.store(object_id, disk_location);
        }

        let next_file_lsn = AtomicU64::new(max_file_lsn + max_file_size + 1);

        Ok(Marble {
            location_table,
            max_object_id: Arc::new(max_object_id.into()),
            file_map: FileMap {
                fams: fams,
                next_file_lsn: Arc::new(next_file_lsn),
            },
            config,
            directory_lock: Arc::new(directory_lock),
            #[cfg(feature = "runtime_validation")]
            debug_history: Arc::new(debug_history.into()),
        })
    }
}

fn read_storage_directory(heap_dir: PathBuf) -> io::Result<Vec<(Metadata, fs::DirEntry)>> {
    let mut files = vec![];
    // parse file names
    for entry_res in fallible!(fs::read_dir(heap_dir)) {
        let entry = fallible!(entry_res);
        let file_size = fallible!(entry.metadata()).len();
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

        let metadata = match Metadata::parse(name, file_size) {
            Some(mn) => mn,
            None => {
                if name != LEGEND {
                    log::error!(
                        "encountered strange file in internal directory: {:?}",
                        entry.path(),
                    );
                }
                continue;
            }
        };

        files.push((metadata, entry));
    }

    files.sort_by_key(|(metadata, _)| metadata.lsn & NEW_WRITE_BATCH_MASK);

    Ok(files)
}
