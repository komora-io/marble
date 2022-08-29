use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;

use fault_injection::{annotate, fallible};

use crate::{Map, ObjectId, RelativeDiskLocation, ZstdDict};

pub(crate) fn read_trailer(
    file: &mut File,
    trailer_offset: u64,
) -> io::Result<(Vec<(ObjectId, RelativeDiskLocation)>, ZstdDict)> {
    let mut buf = vec![];

    fallible!(file.seek(SeekFrom::Start(trailer_offset)));
    fallible!(file.read_to_end(&mut buf));

    if buf.len() < 20 {
        return Err(annotate!(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("trailer at offset {trailer_offset} is smaller than the minimum possible size")
        )));
    }

    let expected_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());

    let actual_crc = crc32fast::hash(&buf[4..]);

    if actual_crc != expected_crc {
        return Err(annotate!(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "crc mismatch for object file trailer, expected {expected_crc} but got \
                 {actual_crc} for buffer of length {}",
                buf.len()
            )
        )));
    }

    log::trace!(
        "read trailer of length {} at offset {trailer_offset} with crc {}",
        buf.len(),
        actual_crc
    );

    let offsets = usize::try_from(u64::from_le_bytes(buf[4..12].try_into().unwrap())).unwrap();
    let zstd_dict_size =
        usize::try_from(u64::from_le_bytes(buf[12..20].try_into().unwrap())).unwrap();

    let offsets_begin = 4 + 8 + 8;
    let offsets_end = offsets_begin + (offsets * 16);
    let zstd_dict_buf_start = offsets_end;
    let zstd_dict_buf_end = zstd_dict_buf_start + zstd_dict_size;

    let mut ret = vec![];

    log::trace!("reading offsets at trailer offset {}", offsets_begin);

    for sub_buf in buf[offsets_begin..offsets_end].chunks(16) {
        let object_id = u64::from_le_bytes(sub_buf[..8].try_into().unwrap());
        let raw_relative_loc = u64::from_le_bytes(sub_buf[8..].try_into().unwrap());
        let relative_loc = RelativeDiskLocation::from_raw(raw_relative_loc);

        ret.push((object_id, relative_loc));
    }

    let zstd_dict_buf = if zstd_dict_size > 0 {
        let dict_buf = buf[zstd_dict_buf_start..zstd_dict_buf_end].to_vec();
        log::trace!(
            "read zstd dict with crc {} between offsets {} and {}",
            crc32fast::hash(&dict_buf),
            zstd_dict_buf_start,
            zstd_dict_buf_end
        );
        Some(dict_buf)
    } else {
        None
    };

    let zstd_dict = ZstdDict(zstd_dict_buf);

    Ok((ret, zstd_dict))
}

pub(crate) fn write_trailer<'a>(
    file: &mut File,
    trailer_offset: u64,
    new_shifted_relative_locations: &Map<ObjectId, RelativeDiskLocation>,
    zstd_dict: &ZstdDict,
) -> io::Result<()> {
    // space for overall crc + offset table + zstd dict + each
    // (object_id, location) pair
    let header_size = 4 + 8 + 8;
    let offsets = new_shifted_relative_locations.len();
    let zstd_dict_buffer = zstd_dict.as_bytes();
    let zstd_dict_size = zstd_dict_buffer.len();

    let mut buf = Vec::with_capacity(header_size + (offsets * 16) + zstd_dict_size);

    // space for crc
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&(offsets as u64).to_le_bytes());
    buf.extend_from_slice(&(zstd_dict_size as u64).to_le_bytes());

    log::trace!("writing offsets at trailer offset {}", buf.len());

    for (object_id, relative_location) in new_shifted_relative_locations {
        let object_id_bytes: &[u8; 8] = &object_id.to_le_bytes();
        let loc_bytes: &[u8; 8] = &relative_location.to_raw().to_le_bytes();
        buf.extend_from_slice(object_id_bytes);
        buf.extend_from_slice(loc_bytes)
    }

    let zstd_dict_buf_start = buf.len();
    buf.extend_from_slice(zstd_dict_buffer);
    let zstd_dict_buf_end = buf.len();

    let crc = crc32fast::hash(&buf[4..]);
    let crc_bytes = crc.to_le_bytes();

    buf[0..4].copy_from_slice(&crc_bytes);

    log::trace!(
        "wrote trailer of length {} at offset {trailer_offset} with crc {} between offsets {} and \
         {}",
        buf.len(),
        crc,
        zstd_dict_buf_start,
        zstd_dict_buf_end
    );

    fallible!(file.write_all_at(&buf, trailer_offset));

    log::trace!(
        "wrote zstd dict with crc {}",
        crc32fast::hash(zstd_dict_buffer)
    );
    fallible!(file.flush());

    Ok(())
}
