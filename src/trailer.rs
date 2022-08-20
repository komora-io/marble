use std::fs::File;
use std::io;
use std::io::Write;
use std::os::unix::fs::FileExt;

use fault_injection::{annotate, fallible};

use crate::{Map, ObjectId, RelativeDiskLocation};

pub(crate) fn read_trailer(
    file: &File,
    trailer_offset: u64,
    trailer_items: u64,
) -> io::Result<Vec<(ObjectId, RelativeDiskLocation)>> {
    let trailer_size = usize::try_from(4 + (trailer_items * 16)).unwrap();

    let mut buf = Vec::with_capacity(trailer_size);

    unsafe {
        buf.set_len(trailer_size);
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

pub(crate) fn write_trailer<'a>(
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
