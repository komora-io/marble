use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::ops::Bound::{Included, Unbounded};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use fault_injection::{fallible, maybe};

use crate::{
    debug_delay, hash, write_trailer, DiskLocation, FileAndMetadata, Map, Marble, Metadata,
    ObjectId, RelativeDiskLocation, ZstdDict, HEADER_LEN, NEW_WRITE_BATCH_BIT,
};

const HEAP_DIR_SUFFIX: &str = "heap";
const NEW_WRITE_GENERATION: u8 = 0;

impl Marble {
    /// Write a batch of objects to disk. This function is
    /// crash-atomic but NOT runtime atomic. If you are
    /// concurrently serving reads, and require atomic batch
    /// semantics, you should serve reads out of an
    /// in-memory cache until this function returns. Creates
    /// one file per call. May perform several fsync
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
        let old_locations = Map::new();
        self.shard_batch(write_batch, NEW_WRITE_GENERATION, &old_locations)
    }

    pub(crate) fn shard_batch<B, I>(
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
                let shard = if gen == NEW_WRITE_GENERATION {
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

            // only split shards on rewrite, otherwise we lose batch
            // atomicity
            let is_rewrite = gen > NEW_WRITE_GENERATION;
            let over_size_preference = shard.0 > self.config.target_file_size;

            if is_rewrite && over_size_preference {
                fragmented_shards.push((shard_id, std::mem::take(&mut shard.1)));
                shard.0 = 0;
            }

            shard.0 += object_size;
            if let Some(Some(replaced)) = shard.1.insert(object_id, data_opt) {
                shard.0 -= replaced.as_ref().len();
            }
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

        let is_gc = if generation == NEW_WRITE_GENERATION {
            assert!(old_locations.is_empty());
            false
        } else {
            assert!(!old_locations.is_empty());
            true
        };

        // Common write path:
        // 1. write data to tmp
        // 2. assign LSN and add to fams
        // 3. attempt installation into pagetable
        // 4. create trailer based on pagetable installation success
        // 5. write trailer then rename file
        // 6. update replaced / contention-related failures

        // 1. write data to tmp
        let tmp_file_name = format!("{}-tmp", TMP_COUNTER.fetch_add(1, SeqCst));
        let tmp_path = self.config.path.join(HEAP_DIR_SUFFIX).join(tmp_file_name);

        let mut file_options = OpenOptions::new();
        file_options.read(true).write(true).create(true);

        let file = fallible!(file_options.open(&tmp_path));
        let mut buf_writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

        let zstd_dict = if self.config.zstd_compression_level.is_some() {
            ZstdDict::from_samples(&objects)
        } else {
            ZstdDict::default()
        };

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

            let compressed_object =
                if let Some(zstd_compression_level) = self.config.zstd_compression_level {
                    Some(zstd_dict.compress(raw_object, zstd_compression_level))
                } else {
                    None
                };

            let is_delete = false;
            let relative_location = RelativeDiskLocation::new(relative_address, is_delete);
            new_relative_locations.insert(*object_id, relative_location);

            let output_object: &[u8] = compressed_object
                .as_ref()
                .map(AsRef::as_ref)
                .unwrap_or(raw_object);

            let len_buf: [u8; 8] = (output_object.len() as u64).to_le_bytes();
            let pid_buf: [u8; 8] = object_id.to_le_bytes();

            let crc = hash(len_buf, pid_buf, &output_object);

            log::trace!(
                "writing object {} at offset {} with crc {:?}",
                object_id,
                written_bytes,
                crc
            );

            fallible!(buf_writer.write_all(&crc));
            fallible!(buf_writer.write_all(&pid_buf));
            fallible!(buf_writer.write_all(&len_buf));
            fallible!(buf_writer.write_all(&output_object));

            written_bytes += (HEADER_LEN + output_object.len()) as u64;
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

        let initial_capacity = new_relative_locations.len() as u64;

        let fam = FileAndMetadata {
            file,
            metadata: Metadata::default(),
            len: initial_capacity.into(),
            generation,
            location,
            synced: self.config.fsync_each_batch.into(),
            // set path to empty and rewrite_claim to true so
            // that nobody tries to concurrently do maintenance
            // on us until we're done with our write operation.
            path: PathBuf::new(),
            rewrite_claim: true.into(),
            zstd_dict: zstd_dict.clone(),
        };

        assert!(fams.insert(location, fam).is_none());

        drop(fams);

        assert_ne!(lsn, 0);

        // 3. attempt installation into pagetable

        let mut replaced_locations: Vec<(ObjectId, DiskLocation)> = vec![];
        let mut failed_gc_locations = vec![];
        let mut subtract_from_len = 0;

        for (object_id, new_relative_location) in &new_relative_locations {
            // history debug must linearize with actual atomic
            // operations below
            #[cfg(feature = "runtime_validation")]
            let mut debug_history = self.debug_history.lock().unwrap();

            let new_location = new_relative_location.to_absolute(lsn);

            if let Some(old_location) = old_locations.get(&object_id) {
                // CAS it
                let res = self
                    .location_table
                    .cas(*object_id, *old_location, new_location);

                match res {
                    Ok(()) => {
                        log::trace!(
                            "cas of {object_id} from old location {old_location:?} to new \
                             location {new_location:?} successful"
                        );

                        #[cfg(feature = "runtime_validation")]
                        {
                            debug_history.mark_add(*object_id, new_location);
                            debug_history.mark_remove(*object_id, *old_location);
                        }

                        replaced_locations.push((*object_id, *old_location));
                    }
                    Err(_current_opt) => {
                        log::trace!(
                            "cas of {object_id} from old location {old_location:?} to new \
                             location {new_location:?} failed"
                        );
                        failed_gc_locations.push(*object_id);
                        subtract_from_len += 1;
                    }
                }
            } else {
                // fetch_max it
                //
                // NB spooky concurrency stuff here:
                // even if we fail to install the item due to data races,
                // we still need to include it in the trailer and make it
                // potentially available to be recovered, because we can't
                // guarantee that the write batch that happened after ours
                // will actually write before crashing. But we must preserve
                // batch atomicity, even when we can't actually install the
                // item at runtime due to conflicts with "the future" that
                // is not recoverable.
                let res = self.location_table.fetch_max(*object_id, new_location);

                if let Ok(old_opt) = res {
                    log::trace!(
                        "fetch_max of {object_id} to new location {new_location:?} successful"
                    );

                    #[cfg(feature = "runtime_validation")]
                    debug_history.mark_add(*object_id, new_location);

                    if let Some(old) = old_opt {
                        replaced_locations.push((*object_id, old));

                        #[cfg(feature = "runtime_validation")]
                        debug_history.mark_remove(*object_id, old);
                    }
                } else {
                    log::trace!("fetch_max of {object_id} to new location {new_location:?} failed");

                    subtract_from_len += 1;
                }
            };
        }

        for failed_gc_location in &failed_gc_locations {
            new_relative_locations.remove(failed_gc_location).unwrap();
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
            trailer_offset: written_bytes,
            present_objects: objects.len() as u64,
            generation,
        };

        let file_name = metadata.to_file_name();
        let new_path = self.config.path.join(HEAP_DIR_SUFFIX).join(file_name);

        log::trace!(
            "writing trailer for {lsn} at offset {}, trailer items {trailer_items}",
            written_bytes,
        );

        let res = write_trailer(
            &mut file_2,
            written_bytes,
            &new_relative_locations,
            &zstd_dict,
        )
        .and_then(|_| maybe!(file_2.sync_all()))
        .and_then(|_| maybe!(fs::rename(&tmp_path, &new_path)));

        let file_len = fallible!(file_2.metadata()).len();
        let expected_file_len = written_bytes
            + 4
            + 8
            + 8
            + (16 * new_relative_locations.len() as u64)
            + zstd_dict.as_bytes().len() as u64;

        assert_eq!(file_len, expected_file_len);
        assert_eq!(trailer_items, new_relative_locations.len());

        if let Err(e) = res {
            // we're in a pretty unfortunate spot because we have
            // already installed items into the location table
            // and reads may be going there already, but we
            // can at least attempt to undo each of the replaced
            // locations before removing the fam and file to mitigate
            // additional damage.
            for (object_id, old_location) in replaced_locations {
                let new_relative_location = new_relative_locations.get(&object_id).unwrap();
                let new_location = new_relative_location.to_absolute(lsn);
                let _dont_care = self
                    .location_table
                    .cas(object_id, new_location, old_location);
            }
            let mut fams = self.fams.write().unwrap();
            self.verify_file_uninhabited(location, &fams);
            fams.remove(&location).unwrap();
            fallible!(fs::remove_file(tmp_path));
            log::error!("failed to write new file: {:?}", e);
            return Err(e);
        };

        log::trace!("renamed file to {:?}", new_path);

        // 6. update replaced / contention-related failures
        let fams = self.fams.read().unwrap();

        for (_object_id, replaced_location) in replaced_locations.into_iter() {
            let (fam_location, fam) = fams
                .range((Unbounded, Included(replaced_location)))
                .next_back()
                .unwrap();

            assert_ne!(*fam_location, location);

            let old = fam.len.fetch_sub(1, SeqCst);
            log::trace!(
                "subtracting one from fam {:?}, current len is {}",
                replaced_location,
                old - 1
            );
            assert!(old >= 1, "expected old {old} to be >= 1");
        }

        drop(fams);

        let mut fams = self.fams.write().unwrap();

        let fam = fams.get_mut(&location).unwrap();
        fam.metadata = metadata;
        let old = fam.len.fetch_sub(subtract_from_len, SeqCst);

        assert!(
            old >= subtract_from_len,
            "expected old {old} to be >= subtract_from_len {subtract_from_len}"
        );

        fam.path = new_path;

        debug_delay();
        assert!(fam.rewrite_claim.swap(false, SeqCst));

        Ok(())
    }
}
