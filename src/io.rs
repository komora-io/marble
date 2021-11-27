use std::fs::File;
use std::io;

use backtrace::{Backtrace, BacktraceFrame};
use crc32fast::Hasher;
use serde::de::DeserializeOwned;

const CRC_BYTES: usize = 4;

#[derive(Debug)]
pub struct Error {
    thrower: String,
    io_error: io::Error,
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        let backtrace = Backtrace::new();
        let backtrace_frames: Vec<BacktraceFrame> = backtrace.into();
        let frame = backtrace_frames[6].symbols().first().unwrap();
        let thrower = format!(
            "{}: {}",
            frame
                .filename()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy(),
            frame.lineno().unwrap()
        );

        Error { thrower, io_error }
    }
}

impl std::ops::Deref for Error {
    type Target = io::Error;

    fn deref(&self) -> &io::Error {
        &self.io_error
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
