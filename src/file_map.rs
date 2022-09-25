use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::ops::Bound::{Included, Unbounded};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering::SeqCst},
    Arc, RwLock,
};

use fault_injection::{fallible, maybe};

use crate::{
    debug_delay, Config, DiskLocation, FileAndMetadata, Map, Metadata, ObjectId, Stats, ZstdDict,
    NEW_WRITE_BATCH_BIT,
};

// `DeferUnclaim` exists because it was surprisingly
// leak-prone to try to manage fams that were claimed by a
// maintenance thread but never used. This ensures fams
// always get unclaimed after this function returns.
pub(crate) struct DeferUnclaim<'a> {
    file_map: &'a FileMap,
    claims: Vec<DiskLocation>,
}

impl<'a> Drop for DeferUnclaim<'a> {
    fn drop(&mut self) {
        let fams = self.file_map.inner.read().unwrap();
        for claim in &self.claims {
            if let Some(fam) = fams.get(claim) {
                debug_delay();
                assert!(fam.rewrite_claim.swap(false, SeqCst));
            }
        }
    }
}

pub(crate) struct FileMapGuard<'a> {
    garbage_epoch: Vec<DiskLocation>,
    file_map: &'a FileMap,
}

pub(crate) struct FileMap {
    pub(crate) inner: RwLock<BTreeMap<DiskLocation, FileAndMetadata>>,
    pub(crate) next_file_lsn: AtomicU64,
}

type GcTuple = (DiskLocation, PathBuf, Arc<File>, u64, u64);

impl FileMap {
    pub fn files_to_defrag<'a>(
        &'a self,
        config: &Config,
    ) -> io::Result<(Map<u8, Vec<GcTuple>>, DeferUnclaim<'a>)> {
        const MAX_GENERATION: u8 = 3;

        let mut claims = DeferUnclaim {
            file_map: self,
            claims: vec![],
        };

        let mut files_to_defrag: Map<u8, Vec<GcTuple>> = Map::new();

        let fams = self.inner.read().unwrap();

        for (location, fam) in &*fams {
            assert_eq!(*location, fam.location);
            let len = fam.len.load(SeqCst);
            let present_objects = fam.metadata.present_objects;

            if len != 0
                && (len * 100) / present_objects.max(1) < u64::from(config.file_compaction_percent)
            {
                debug_delay();
                let already_locked = fam.rewrite_claim.swap(true, SeqCst);
                if already_locked {
                    // try to exclusively claim this file
                    // for rewrite to
                    // prevent concurrent attempts at
                    // rewriting its contents
                    continue;
                }
                assert_ne!(present_objects, 0);

                claims.claims.push(*location);

                log::trace!(
                    "fam at location {:?} is ready to be compacted",
                    fam.location
                );

                let generation = fam.generation.saturating_add(1).min(MAX_GENERATION);

                let entry = files_to_defrag.entry(generation).or_default();
                entry.push((
                    *location,
                    fam.path.clone(),
                    fam.file.clone(),
                    fam.metadata.trailer_offset,
                    fam.metadata.file_size,
                ));
            }
        }

        Ok((files_to_defrag, claims))
    }

    pub fn pin(&self) -> FileMapGuard {
        FileMapGuard {
            garbage_epoch: vec![],
            file_map: self,
        }
    }

    pub fn file_and_dict_for_object(
        &self,
        location: DiskLocation,
    ) -> (DiskLocation, Arc<File>, Arc<ZstdDict>) {
        let fams = self.inner.read().unwrap();
        let (location, fam) = fams
            .range((Unbounded, Included(location)))
            .next_back()
            .expect("no possible storage file for object - likely file corruption");

        (*location, fam.file.clone(), fam.zstd_dict.clone())
    }

    pub fn insert<'a>(
        &'a self,
        file: File,
        written_bytes: u64,
        initial_capacity: u64,
        generation: u8,
        is_gc: bool,
        config: &Config,
        decompressor: ZstdDict,
    ) -> (DiskLocation, DeferUnclaim<'a>) {
        let mut fams = self.inner.write().unwrap();

        // NB this fetch_add should always happen while fams write
        // lock is being held
        let lsn_base = self.next_file_lsn.fetch_add(written_bytes + 1, SeqCst);

        let lsn = if is_gc {
            lsn_base
        } else {
            lsn_base | NEW_WRITE_BATCH_BIT
        };

        let location = DiskLocation::new_fam(lsn);
        log::debug!("inserting new fam at lsn {lsn} location {location:?}",);

        let fam = FileAndMetadata {
            file: Arc::new(file),
            metadata: Metadata::default(),
            len: initial_capacity.into(),
            generation,
            location,
            synced: config.fsync_each_batch.into(),
            // set path to empty and rewrite_claim to true so
            // that nobody tries to concurrently do maintenance
            // on us until we're done with our write operation.
            path: PathBuf::new(),
            rewrite_claim: true.into(),
            zstd_dict: Arc::new(decompressor),
        };

        assert!(fams.insert(location, fam).is_none());

        let claim = DeferUnclaim {
            file_map: self,
            claims: vec![location],
        };

        drop(fams);

        assert_ne!(lsn, 0);

        (DiskLocation::new_fam(lsn), claim)
    }

    pub fn sync_all(&self) -> io::Result<bool> {
        let fams = self.inner.read().unwrap();

        let mut synced_files = false;
        for fam in fams.values() {
            if !fam.synced.load(SeqCst) {
                fam.file.sync_all()?;
                fam.synced.store(true, SeqCst);
                synced_files = true;
            }
        }

        Ok(synced_files)
    }

    pub fn prune_empty_files<'a>(&'a self) -> io::Result<()> {
        // get writer file lock and remove the empty fams
        let mut paths_to_remove = vec![];

        let mut claims = DeferUnclaim {
            file_map: self,
            claims: vec![],
        };
        let read_fams = self.inner.read().unwrap();

        for (location, fam) in read_fams.iter() {
            debug_delay();
            if fam.len.load(SeqCst) == 0 {
                let already_claimed = fam.rewrite_claim.swap(true, SeqCst);
                if !already_claimed {
                    claims.claims.push(*location);
                    log::trace!("fam at location {location:?} is empty, marking it for removal");
                    paths_to_remove.push((*location, fam.path.clone()));
                }
            }
        }

        drop(read_fams);

        let mut fams = self.inner.write().unwrap();

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

        drop(claims);

        Ok(())
    }

    pub fn verify_files_uninhabited(&self, locations: &[DiskLocation]) {
        let fams = self.inner.read().unwrap();
        for location in locations {
            self.verify_file_uninhabited(*location, &fams);
        }
    }

    pub fn stats(&self) -> Stats {
        let mut live_objects = 0;
        let mut stored_objects = 0;

        let fams = self.inner.read().unwrap();

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

    pub fn remove_fam(&self, location: DiskLocation) -> io::Result<()> {
        let mut fams = self.inner.write().unwrap();

        self.verify_file_uninhabited(location, &fams);

        let fam = fams.remove(&location).unwrap();

        maybe!(std::fs::remove_file(fam.path))
    }

    pub fn finalize_fam(
        &self,
        location: DiskLocation,
        metadata: Metadata,
        subtract_from_len: u64,
        new_path: PathBuf,
    ) {
        let mut fams = self.inner.write().unwrap();

        let fam = fams.get_mut(&location).unwrap();
        fam.metadata = metadata;
        let old = fam.len.fetch_sub(subtract_from_len, SeqCst);

        assert!(
            old >= subtract_from_len,
            "expected old {old} to be >= subtract_from_len {subtract_from_len}"
        );

        fam.path = new_path;

        debug_delay();
    }

    pub fn decrement_evacuated_fams(
        &self,
        new_base_location: DiskLocation,
        replaced_locations: Vec<(ObjectId, DiskLocation)>,
    ) {
        let fams = self.inner.read().unwrap();

        for (_object_id, replaced_location) in replaced_locations.into_iter() {
            let (fam_location, fam) = fams
                .range((Unbounded, Included(replaced_location)))
                .next_back()
                .unwrap();

            assert_ne!(*fam_location, new_base_location);

            let old = fam.len.fetch_sub(1, SeqCst);
            log::trace!(
                "subtracting one from fam {:?}, current len is {}",
                replaced_location,
                old - 1
            );
            assert!(old >= 1, "expected old {old} to be >= 1");
        }
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
