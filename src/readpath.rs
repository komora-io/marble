use std::io;
use std::os::unix::fs::FileExt;
use std::sync::atomic::Ordering;

use fault_injection::{annotate, fallible};

use crate::{hash, uninit_boxed_slice, Marble, ObjectId, HEADER_LEN};

impl Marble {
    /// Read a object out of storage. If this object is
    /// unknown or has been removed, returns `Ok(None)`.
    /// If there is an IO problem, returns Err.
    pub fn read(&self, object_id: ObjectId) -> io::Result<Option<Box<[u8]>>> {
        let location = if let Some(location) = self.location_table.load(object_id) {
            location
        } else {
            return Ok(None);
        };

        if location.is_delete() {
            return Ok(None);
        }

        let fam = self.file_map.fam_for_location(location);

        let file_offset = location.lsn() - fam.location.lsn();

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

        let mut compressed_buf: Box<[u8]> = uninit_boxed_slice(len);

        let object_offset = file_offset + HEADER_LEN as u64;
        fallible!(fam.file.read_exact_at(&mut compressed_buf, object_offset));

        let crc_actual = hash(len_buf, pid_buf, &compressed_buf);

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

        self.compressed_bytes_read
            .fetch_add(compressed_buf.len() as u64, Ordering::Relaxed);

        let decompressed_buf = fam.zstd_dict.decompress(compressed_buf);

        self.decompressed_bytes_read
            .fetch_add(decompressed_buf.len() as u64, Ordering::Relaxed);

        Ok(Some(decompressed_buf))
    }
}
