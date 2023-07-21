use std::cmp::Reverse;
use std::fs::File;
use std::io;
use std::ops::Bound::{Included, Unbounded};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicPtr, AtomicU64, Ordering::SeqCst},
    Arc,
};

use concurrent_map::{ConcurrentMap, Maximum};

use crate::{
    debug_delay, Config, DiskLocation, FileAndMetadata, LocationTable, Map, Metadata, ObjectId,
    ZstdDict, NEW_WRITE_BATCH_BIT,
};

impl Maximum for DiskLocation {
    const MAX: Self = DiskLocation::MAX;
}

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
        for claim in &self.claims {
            if let Some(fam) = self.file_map.fams.get(&Reverse(*claim)) {
                debug_delay();
                assert!(fam.rewrite_claim.swap(false, SeqCst));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct FileMap {
    pub(crate) fams: ConcurrentMap<Reverse<DiskLocation>, Arc<FileAndMetadata>, 16, 1>,
    pub(crate) next_file_lsn: Arc<AtomicU64>,
}

impl FileMap {
    pub fn files_to_defrag<'a>(
        &'a self,
        config: &Config,
    ) -> io::Result<(Map<u8, Vec<Arc<FileAndMetadata>>>, DeferUnclaim<'a>)> {
        const MAX_GENERATION: u8 = 3;

        let mut claims = DeferUnclaim {
            file_map: self,
            claims: vec![],
        };

        let mut files_to_defrag: Map<u8, Vec<Arc<FileAndMetadata>>> = Map::default();

        let approximate_fam_len = self.fams.len();

        for (location, fam) in &self.fams {
            assert_eq!(location.0, fam.location);

            let metadata: &Metadata = if let Some(m) = fam.metadata() {
                m
            } else {
                // metadata not yet initialized
                continue;
            };

            let live_objects = fam.live_objects.load(SeqCst);
            let live_and_dead_objects = metadata.present_objects;

            let non_empty = live_objects != 0;
            let live_percent = (live_objects * 100) / live_and_dead_objects.max(1);
            let candidate_by_percent = live_percent < u64::from(config.file_compaction_percent);
            let is_small_file = (metadata.file_size * config.min_compaction_files as u64)
                < config.target_file_size as u64;
            let over_small_file_cleanup_threshold =
                config.small_file_cleanup_threshold <= approximate_fam_len;
            let candidate_by_size = over_small_file_cleanup_threshold && is_small_file;

            if non_empty && (candidate_by_percent || candidate_by_size) {
                debug_delay();
                let already_locked = fam.rewrite_claim.swap(true, SeqCst);
                if already_locked {
                    // try to exclusively claim this file
                    // for rewrite to
                    // prevent concurrent attempts at
                    // rewriting its contents
                    continue;
                }
                assert_ne!(live_and_dead_objects, 0);

                claims.claims.push(location.0);

                let generation = fam.generation.saturating_add(1).min(MAX_GENERATION);

                log::trace!(
                    "fam at location {:?} generation {generation} is ready to be compacted, \
                    live objects {live_objects} dead objects {} \
                    live percent {live_percent} candidate by percent {candidate_by_percent} \
                    candidate by size {candidate_by_size} (metadata size: {})",
                    fam.location,
                    live_and_dead_objects - live_objects,
                    metadata.file_size
                );

                let entry = files_to_defrag.entry(generation).or_default();
                entry.push(fam);
            }
        }

        Ok((files_to_defrag, claims))
    }

    pub fn fam_for_location(&self, location: DiskLocation) -> Arc<FileAndMetadata> {
        let (_, fam) = self
            .fams
            .range((Included(Reverse(location)), Unbounded))
            .next()
            .expect("no possible storage file for object - likely file corruption");

        fam
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
        let lsn_base = self.next_file_lsn.fetch_add(written_bytes + 1, SeqCst);

        let lsn = if is_gc {
            lsn_base
        } else {
            lsn_base | NEW_WRITE_BATCH_BIT
        };

        let location = DiskLocation::new_fam(lsn);
        log::debug!("inserting new fam at lsn {lsn} location {location:?}",);

        let fam = Arc::new(FileAndMetadata {
            file: file,
            live_objects: initial_capacity.into(),
            generation,
            location,
            synced: config.fsync_each_batch.into(),
            metadata: AtomicPtr::default(),
            // set path to empty and rewrite_claim to true so
            // that nobody tries to concurrently do maintenance
            // on us until we're done with our write operation.
            path: AtomicPtr::default(),
            rewrite_claim: true.into(),
            zstd_dict: decompressor,
        });

        assert!(self.fams.insert(Reverse(location), fam).is_none());

        let claim = DeferUnclaim {
            file_map: self,
            claims: vec![location],
        };

        assert_ne!(lsn, 0);

        (DiskLocation::new_fam(lsn), claim)
    }

    pub fn sync_all(&self) -> io::Result<bool> {
        let mut synced_files = false;
        for fam in self.fams.iter().map(|(_k, v)| v) {
            if !fam.synced.load(SeqCst) {
                fam.file.sync_all()?;
                fam.synced.store(true, SeqCst);
                synced_files = true;
            }
        }

        Ok(synced_files)
    }

    pub fn prune_empty_files<'a>(&'a self, location_table: &LocationTable) -> io::Result<()> {
        // remove the empty fams
        let mut paths_to_remove = vec![];

        let mut claims = DeferUnclaim {
            file_map: self,
            claims: vec![],
        };

        for (location, fam) in &self.fams {
            debug_delay();
            let path = if let Some(p) = fam.path() {
                p
            } else {
                // fam is being initialized still
                continue;
            };

            if fam.live_objects.load(SeqCst) == 0 {
                let already_claimed = fam.rewrite_claim.swap(true, SeqCst);
                if !already_claimed {
                    claims.claims.push(location.0);
                    log::trace!("fam at location {location:?} is empty, marking it for removal");
                    paths_to_remove.push((location.0, path.clone()));
                }
            }
        }

        for (location, _) in &paths_to_remove {
            log::trace!("removing fam at location {:?}", location);

            self.verify_file_uninhabited(*location, location_table);

            self.fams.remove(&Reverse(*location)).unwrap();
        }

        drop(claims);

        Ok(())
    }

    pub fn verify_files_uninhabited(
        &self,
        locations: &[DiskLocation],
        location_table: &LocationTable,
    ) {
        for location in locations {
            self.verify_file_uninhabited(*location, location_table);
        }
    }

    /// Returns the counts of (files, total file size, total stored objects, live objects)
    pub(crate) fn stats(&self) -> (usize, u64, u64, u64) {
        let mut live_objects = 0;
        let mut stored_objects = 0;

        let mut fams_len = 0;
        let mut total_file_size = 0;
        for (_, fam) in &self.fams {
            if let Some(metadata) = fam.metadata() {
                fams_len += 1;
                total_file_size += metadata.file_size;
                live_objects += fam.live_objects.load(SeqCst);
                stored_objects += metadata.present_objects;
            }
        }

        (fams_len, total_file_size, stored_objects, live_objects)
    }

    pub fn delete_partially_installed_fam(&self, location: DiskLocation, tmp_path: PathBuf) {
        let fam = self.fams.remove(&Reverse(location)).unwrap();
        fam.live_objects.store(0, SeqCst);

        let path_ptr = Box::into_raw(Box::new(tmp_path));
        let old_path_ptr = fam.path.swap(path_ptr, SeqCst);
        assert!(old_path_ptr.is_null());
    }

    pub fn finalize_fam(
        &self,
        location: DiskLocation,
        metadata: Metadata,
        subtract_from_len: u64,
        new_path: PathBuf,
    ) {
        let fam = self.fams.get(&Reverse(location)).unwrap();

        fam.install_metadata_and_path(metadata, new_path);

        let old = fam.live_objects.fetch_sub(subtract_from_len, SeqCst);
        assert!(
            old >= subtract_from_len,
            "expected old {old} to be >= subtract_from_len {subtract_from_len}"
        );

        debug_delay();
    }

    pub fn decrement_evacuated_fams(
        &self,
        new_base_location: DiskLocation,
        replaced_locations: Vec<(ObjectId, DiskLocation)>,
    ) {
        for (_object_id, replaced_location) in replaced_locations.into_iter() {
            let (fam_location, fam) = self
                .fams
                .range((Included(Reverse(replaced_location)), Unbounded))
                .next()
                .unwrap();

            assert_ne!(fam_location.0, new_base_location);

            let old = fam.live_objects.fetch_sub(1, SeqCst);
            log::trace!(
                "subtracting one from fam {:?}, current len is {}",
                replaced_location,
                old - 1
            );
            assert!(old >= 1, "expected old {old} to be >= 1");
        }
    }

    fn verify_file_uninhabited(&self, _location: DiskLocation, _location_table: &LocationTable) {
        #[cfg(feature = "runtime_validation")]
        {
            let fam = &self.fams.get(&Reverse(_location)).unwrap();
            let metadata = fam
                .metadata()
                .expect("any fam being deleted should have metadata set");
            let next_location = DiskLocation::new_fam(_location.lsn() + metadata.trailer_offset);
            let present: Vec<(ObjectId, DiskLocation)> = _location_table
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
