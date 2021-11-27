use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::{Result, WriteBatch};

/// Log segments are appended to at runtime, and deleted after the PageCache's
/// `stable_txid` rises above the final txid that is present in a segment.
pub struct Log {
    current_segment: LogSegment,
    uncompacted_segments: Vec<LogSegment>,
    file: File,
}

impl Log {
    pub fn open_or_create(path: &Path) -> Result<Log> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);

        let file = options.open(path)?;

        Ok(Log {
            current_segment: Default::default(),
            uncompacted_segments: vec![],
            file,
        })
    }

    pub fn recover(&mut self) -> LogSegment {
        bincode::deserialize_from(&mut self.file).unwrap()
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        Ok(())
    }

    pub fn queue_write(&mut self, batch: WriteBatch) {
        if let Some(last) = self.current_segment.write_batches.last() {
            assert!(last.txid.0 < batch.txid.0);
        }
        self.current_segment.write_batches.push(batch);
    }

    pub fn flush(&mut self) -> Result<()> {
        let to_write = std::mem::take(&mut self.current_segment);

        let res = bincode::serialize_into(&mut self.file, &to_write);

        self.uncompacted_segments.push(to_write);

        match res {
            Ok(()) => Ok(()),
            Err(e) => {
                if let bincode::ErrorKind::Io(e) = *e {
                    Err(e.into())
                } else {
                    panic!("unexpected serialization error: {:?}", e)
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct LogSegment {
    write_batches: Vec<WriteBatch>,
}
