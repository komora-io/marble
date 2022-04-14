use std::collections::HashMap;

use serde::{Deserialize, Serialize};

mod io;
mod log;
mod par;

// mod pagecache;
// mod heap;

use io::{Error, Io, Result};
use log::Log;

// use heap::{Heap, HeapOffset};
// use pagecache::Claim;
// pub use pagecache::PageCache;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TxId(u64);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PageId(u64);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[repr(align(4096))]
pub struct Page {}

#[derive(Serialize, Deserialize, Debug)]
pub struct PageBatch {
    pub min_txid: TxId,
    pub max_txid: TxId,
    pub updates: HashMap<PageId, Page>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeltaBatch {
    pub txid: TxId,
    pub updates: HashMap<PageId, Delta>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Delta {
    Set,
    Del,
    Split,
    Merge,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
#[repr(C)]
pub struct HeapOffset {
    pub index: u32,
    pub slab: u8,
    pub generation: u8,
}

impl HeapOffset {
    pub fn from_bytes(bytes: [u8; 8]) -> HeapOffset {
        todo!()
    }

    pub fn to_bytes(self) -> [u8; 8] {
        todo!()
    }
}
