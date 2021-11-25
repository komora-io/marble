use std::collections::HashMap;

use serde::{Deserialize, Serialize};

mod error;
mod heap;
mod log;
mod pagecache;

pub use pagecache::PageCache;

use error::Error;
use heap::{Heap, HeapOffset, Reservation as HeapReservation};
use log::{Log, Reservation as LogReservation};

type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TxId(u64);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PageId(u64);

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteBatch {
    pub txid: TxId,
    pub updates: HashMap<PageId, Page>,
}

pub type Page = HeapOffset;
