use std::io::{self, Read};

use fault_injection::annotate;

use crate::{
    hash, read_range_at, read_trailer_from_buf, uninit_boxed_slice, DiskLocation, Map, Marble,
    ObjectId, RelativeDiskLocation, HEADER_LEN,
};

impl Marble {
    /// Defragments backing storage files, blocking
    /// concurrent calls to `write_batch` but not
    /// blocking concurrent calls to `read`. Returns the
    /// number of rewritten objects.
    pub fn maintenance(&self) -> io::Result<usize> {
        log::debug!("performing maintenance");

        let (files_to_defrag, claims): (Map<u8, Vec<_>>, _) =
            self.file_map.files_to_defrag(&self.config)?;

        // use this old_locations Map in the outer loop to reuse the
        // allocation and avoid resizing as often.
        let mut old_locations: Map<ObjectId, DiskLocation> = Map::default();

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

            let mut batch = Map::default();
            let mut rewritten_fam_locations = vec![];

            for fam in file_to_defrag {
                log::trace!(
                    "rewriting any surviving objects in file at location {:?}",
                    fam.location
                );
                rewritten_fam_locations.push(fam.location);
                let metadata: &crate::Metadata = fam
                    .metadata()
                    .expect("anything being defragged should have metadata already set");

                let path: &std::path::PathBuf = fam.path().unwrap();

                // TODO handle trailer read using full buf
                let file_buf = read_range_at(&fam.file, 0, metadata.file_size)?;

                let trailer = read_trailer_from_buf(
                    &file_buf[usize::try_from(metadata.trailer_offset).unwrap()..],
                )?;

                let mut buf_reader = std::io::Cursor::new(file_buf);

                let mut offset = 0_u64;

                while offset < metadata.trailer_offset {
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
                        RelativeDiskLocation::new(offset, false).to_absolute(fam.location.lsn());

                    // all objects present before the trailer are not deletes
                    let mut object_buf = uninit_boxed_slice(len);

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

                log::trace!(
                    "trying to read trailer at file for lsn {} offset {} items",
                    metadata.trailer_offset,
                    fam.location.lsn(),
                );

                for (object_id, relative_location) in trailer {
                    if relative_location.is_delete() {
                        let rewritten_location = relative_location.to_absolute(fam.location.lsn());
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

            self.file_map
                .verify_files_uninhabited(&rewritten_fam_locations, &self.location_table);
        }

        drop(claims);

        self.prune_empty_files()?;

        Ok(rewritten_objects)
    }
}
