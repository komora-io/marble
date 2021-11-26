use std::{collections::BTreeMap, io, path::Path};

use crate::{Page, PageId, Result, TxId, WriteBatch};

#[must_use]
pub struct Reservation<'a> {
    txid: TxId,
    log_segment: &'a mut LogSegment,
}

impl<'a> Reservation<'a> {
    fn complete(self, batch: WriteBatch) {
        self.log_segment.push(batch);
    }
}

/// Log segments are appended to at runtime, and deleted after the PageCache's
/// `stable_txid` rises above the final txid that is present in a segment.
pub struct Log {
    current_segment: LogSegment,
    stabilizing_segment: Option<LogSegment>,
}

impl Log {
    pub fn recover(path: &Path) -> impl Iterator<Item = Result<(PageId, Page)>> {
        vec![].into_iter()
    }

    pub fn create<P: AsRef<Path>>(path: P) -> Result<Log> {
        todo!()
    }

    pub fn iter(&self) -> impl Iterator<Item = (PageId, Page)> {
        vec![].into_iter()
    }

    fn reserve(&mut self, txid: TxId) -> Reservation {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LogSegment {
    write_batches: Vec<WriteBatch>,
}

impl LogSegment {
    fn push(&mut self, batch: WriteBatch) {
        if let Some(last) = self.write_batches.last() {
            assert!(last.txid.0 < batch.txid.0);
        }
        self.write_batches.push(batch);
    }
}
