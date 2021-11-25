use std::{
    collections::HashMap,
    fs::File,
    io::{self, Error, ErrorKind, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use bincode::{config::DefaultOptions, Options as _};
use crc32fast::Hasher;
use lazy_static::lazy_static;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const CRC_BYTES: usize = 4;

lazy_static! {
    static ref BINCODE: DefaultOptions = DefaultOptions::new();
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct VersionedUpdate {
    lsn: Lsn,
    contents: PageUpdate,
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct LogSegment {
    frags: Vec<VersionedUpdate>,
}

#[derive(Serialize, Deserialize, Debug)]
enum PageUpdate {
    UpdatePage { pid: PageId, add_count: u64 },
}

impl Default for PageUpdate {
    fn default() -> PageUpdate {
        PageUpdate::UpdatePage {
            pid: PageId(0),
            add_count: 0,
        }
    }
}

fn read<T: DeserializeOwned>(file: &mut File, at: u64, limit: Option<usize>) -> io::Result<T> {
    let mut buf = vec![];

    if let Some(limit) = limit {
        buf = Vec::with_capacity(limit);
        unsafe {
            buf.set_len(limit);
        }
        file.read_exact_at(&mut buf, at)?;
    } else {
        assert_eq!(at, 0);
        file.read_to_end(&mut buf)?;
    }

    assert!(buf.len() >= CRC_BYTES);

    let mut hasher = Hasher::new();
    hasher.update(&buf[CRC_BYTES..]);
    let actual_crc: [u8; CRC_BYTES] = hasher.finalize().to_le_bytes();
    let expected_crc: [u8; CRC_BYTES] = buf[..CRC_BYTES].try_into().unwrap();

    if actual_crc != expected_crc {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "crc mismatch: data corruption",
        ));
    }

    let item: T = BINCODE
        .deserialize_from(&buf[CRC_BYTES..])
        .expect("item should deserialize, as its crc passed");

    Ok(item)
}

fn write<T: Serialize>(file: &mut File, at: u64, item: &T) -> io::Result<()> {
    // make enough space at the beginning for the crc
    let mut buf = vec![0; CRC_BYTES];

    BINCODE
        .serialize_into(&mut buf, item)
        .expect("possible allocator failure");

    let mut hasher = Hasher::new();
    hasher.update(&buf[CRC_BYTES..]);
    let hash: [u8; CRC_BYTES] = hasher.finalize().to_le_bytes();
    buf[..CRC_BYTES].copy_from_slice(&hash);

    file.write_all_at(&buf, at)?;
    file.sync_all()
}
