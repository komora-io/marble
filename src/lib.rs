use std::collections::HashMap;

use serde::{Deserialize, Serialize};

mod io;
mod log;

// mod pagecache;
// mod heap;

use io::Error;
use log::Log;

// use heap::{Heap, HeapOffset};
// use pagecache::Claim;
// pub use pagecache::PageCache;

type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct TxId(u64);

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct PageId(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(align(4096))]
pub struct Page {
    raw: [u8; 4096],
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WriteBatch {
    pub txid: TxId,
    pub updates: HashMap<PageId, HeapOffset>,
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
