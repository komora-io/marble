use std::path::Path;

use crate::{Claim, Page, PageId, Result, TxId, WriteBatch};

/// Log segments are appended to at runtime, and deleted after the PageCache's
/// `stable_txid` rises above the final txid that is present in a segment.
pub struct Log {
    current_segment: LogSegment,
    stabilizing_segment: Option<LogSegment>,
}

impl Log {
    pub fn recover(
        path: &Path,
    ) -> impl Iterator<Item = Result<(PageId, Page)>> {
        vec![].into_iter()
    }

    pub fn create<P: AsRef<Path>>(path: P, lowest_txid: TxId) -> Result<Log> {
        todo!()
    }

    pub fn push(&mut self, batch: WriteBatch, tx_claim: Claim<'_>) {
        if let Some(last) = self.current_segment.write_batches.last() {
            assert!(last.txid.0 < batch.txid.0);
        }
        self.current_segment.write_batches.push(batch);
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct LogSegment {
    write_batches: Vec<WriteBatch>,
}
