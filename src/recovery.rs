use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Bound::{Included, Unbounded};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering::SeqCst},
    RwLock,
};

use fault_injection::fallible;

use crate::{
    read_trailer, Config, DiskLocation, FileAndMetadata, LocationTable, Map, Marble, Metadata,
};

const HEAP_DIR_SUFFIX: &str = "heap";
const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";

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

        let _ = File::create(config.path.join(WARN));

        let mut file_lock_opts = OpenOptions::new();
        file_lock_opts.create(true).read(true).write(true);

        let directory_lock = fallible!(File::open(config.path.join(HEAP_DIR_SUFFIX)));
        fallible!(directory_lock.try_lock_exclusive());

        let mut fams = Map::new();
        let mut max_file_lsn = 0;
        let mut max_file_size = 0;

        let mut recovery_page_table = Map::new();

        let files = read_storage_directory(heap_dir)?;

        for (metadata, trailer_offset, entry) in files {
            let mut options = OpenOptions::new();
            options.read(true);

            let mut file = fallible!(options.open(entry.path()));

            let (trailer, zstd_dict) =
                read_trailer(&mut file, trailer_offset, metadata.trailer_items)?;

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
                zstd_dict,
            };

            log::debug!("inserting new fam at location {:?}", file_location);
            assert!(fams.insert(file_location, fam).is_none());
        }

        let location_table = LocationTable::default();
        #[cfg(feature = "runtime_validation")]
        let mut debug_history = debug_history::DebugHistory::default();

        // initialize fam utilization from page table
        for (object_id, disk_location) in recovery_page_table {
            #[cfg(feature = "runtime_validation")]
            debug_history.mark_add(object_id, disk_location);
            let (_l, fam) = fams
                .range((Unbounded, Included(disk_location)))
                .next_back()
                .unwrap();
            fam.len.fetch_add(1, SeqCst);
            location_table.store(object_id, disk_location);
        }

        let next_file_lsn = AtomicU64::new(max_file_lsn + max_file_size + 1);

        Ok(Marble {
            location_table,
            fams: RwLock::new(fams),
            next_file_lsn,
            config,
            directory_lock,
            #[cfg(feature = "runtime_validation")]
            debug_history: debug_history.into(),
        })
    }
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
