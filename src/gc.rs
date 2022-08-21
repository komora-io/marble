use std::fs::File;
use std::io::{self, BufReader, Read};
use std::sync::atomic::Ordering::SeqCst;

use fault_injection::{annotate, fallible};

use crate::{
    debug_delay, hash, read_trailer, DeferUnclaim, DiskLocation, Map, Marble, ObjectId,
    RelativeDiskLocation, HEADER_LEN,
};

const MAX_GENERATION: u8 = 3;

impl Marble {
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
            let len = fam.len.load(SeqCst);
            let trailer_items = fam.metadata.trailer_items;

            if len != 0
                && (len * 100) / trailer_items.max(1)
                    < u64::from(self.config.file_compaction_percent)
            {
                debug_delay();
                let already_claimed = fam.rewrite_claim.swap(true, SeqCst);
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

        // use this old_locations Map in the outer loop to reuse the
        // allocation and avoid resizing as often.
        let mut old_locations: Map<ObjectId, DiskLocation> = Map::new();

        let mut rewritten_objects = 0;

        // rewrite the live objects
        for (generation, file_to_defrag) in files_to_defrag {
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
                    buf_reader.read_exact(&mut header)?;

                    let crc_expected: [u8; 4] = header[0..4].try_into().unwrap();
                    let pid_buf = header[4..12].try_into().unwrap();
                    let object_id = u64::from_le_bytes(pid_buf);
                    let len_buf = header[12..20].try_into().unwrap();
                    let len = usize::try_from(u64::from_le_bytes(len_buf)).unwrap();

                    if len >= self.config.max_object_size {
                        log::warn!("corrupt object size detected: {} bytes", len);
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "corrupt object size or configured max_object_size has gone down \
                             since this object was written",
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

                    buf_reader.read_exact(&mut object_buf)?;

                    let crc_actual = hash(len_buf, pid_buf, &object_buf);

                    if crc_expected != crc_actual {
                        log::error!(
                            "crc mismatch when reading object {} at offset {} in file {:?} - \
                             expected {:?} actual {:?}",
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
                        log::trace!(
                            "rewriting object {object_id} at rewritten location \
                             {rewritten_location:?}"
                        );
                        batch.insert(object_id, Some(object_buf));
                        old_locations.insert(object_id, rewritten_location);
                    } else {
                        log::trace!(
                            "not rewriting object {object_id}, as the location being defragmented \
                             {rewritten_location:?} does not match the current location in the \
                             location table {current_location:?}"
                        );
                    }

                    offset += (HEADER_LEN + len) as u64;
                }

                let mut file: File = buf_reader.into_inner();

                log::trace!(
                    "trying to read trailer at file for lsn {} offset {trailer_offset} items \
                     {trailer_items}",
                    base_location.lsn(),
                );

                let (trailer, _zstd_dict_opt) =
                    read_trailer(&mut file, trailer_offset, trailer_items)?;

                for (object_id, relative_location) in trailer {
                    if relative_location.is_delete() {
                        let rewritten_location = relative_location.to_absolute(base_location.lsn());
                        let current_location = self
                            .location_table
                            .load(object_id)
                            .expect("anything being rewritten should exist in the location table");

                        if rewritten_location == current_location {
                            // can attempt to rewrite
                            log::trace!(
                                "rewriting object {object_id} at rewritten location \
                                 {rewritten_location:?}"
                            );
                            batch.insert(object_id, None);
                            old_locations.insert(object_id, rewritten_location);
                        } else {
                            log::trace!(
                                "not rewriting object {object_id}, as the location being \
                                 defragmented {rewritten_location:?} does not match the current \
                                 location in the location table {current_location:?}"
                            );
                        }
                    }
                }
            }

            rewritten_objects += batch.len();

            log::trace!("{rewritten_objects}, {}", batch.len());

            self.shard_batch(batch, generation, &old_locations)?;
            old_locations.clear();

            let fams = self.fams.read().unwrap();
            for location in rewritten_fam_locations {
                self.verify_file_uninhabited(location, &fams);
                //let fam = &fams[&location];
                //assert_eq!(fam.len.load(SeqCst), 0, "bug
                // with length tracking, because we have
                // verified that this file is actually
                // uninhabited: fam: {fam:?}")
            }
        }

        drop(defer_unclaim);

        self.prune_empty_fams()?;

        Ok(rewritten_objects)
    }
}
