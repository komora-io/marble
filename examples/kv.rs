/// build with --features=serde,bincode
use std::collections::{BTreeMap, HashMap};
use std::io;

use bincode::serde::{decode_from_slice, encode_to_vec};
use marble::Marble;
use serde::{Deserialize, Serialize};

const BC_CONFIG: bincode::config::Configuration = bincode::config::standard()
    .with_little_endian()
    .with_variable_int_encoding();

fn deserialize<T: serde::de::DeserializeOwned>(
    buf: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    Ok(decode_from_slice(buf, BC_CONFIG)?.0)
}

fn serialize<T: Serialize>(t: &T) -> Vec<u8> {
    encode_to_vec(t, BC_CONFIG).unwrap()
}

type ObjectId = u64;

const INDEX_OBJECT_ID: ObjectId = 1;

#[derive(Serialize, Deserialize, Debug)]
struct Index {
    pages: BTreeMap<Vec<u8>, ObjectId>,
    max_pid: u64,
}

impl Default for Index {
    fn default() -> Self {
        Index {
            pages: Default::default(),
            max_pid: INDEX_OBJECT_ID + 1,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Page {
    hi: Option<Vec<u8>>,
    lo: Vec<u8>,
    kvs: BTreeMap<Vec<u8>, Vec<u8>>,
}

// A simple larger-than-memory key-value store.
// A "real" version would use a log of logical
// operations that then periodically gets
// compacted into a Marble write batch, along
// with an in-memory write cache that accumulates
// corresponding page mutations, and an in-memory
// read cache.
struct Kv {
    heap: Marble,
    index: Index,
}

impl Kv {
    fn recover(path: &str) -> io::Result<Kv> {
        let heap = marble::open(path)?;

        let index: Index = if let Some(index_data) = heap.read(INDEX_OBJECT_ID)? {
            deserialize(&index_data).unwrap()
        } else {
            Index::default()
        };

        let mut kv = Kv { index, heap };

        if kv.index.pages.is_empty() {
            let genesis_page = Page {
                hi: None,
                lo: vec![],
                kvs: BTreeMap::new(),
            };

            kv.allocate_leaf(genesis_page)?;
        }

        Ok(kv)
    }

    fn allocate_leaf(&mut self, leaf: Page) -> io::Result<()> {
        self.index.max_pid += 1;
        let object_id = self.index.max_pid;

        let previous = self.index.pages.insert(leaf.lo.clone(), object_id);
        assert!(previous.is_none());

        let write_batch = [(object_id, Some(serialize(&leaf)))];

        self.heap.write_batch(write_batch)
    }

    fn pid_for_key(&self, key: Vec<u8>) -> ObjectId {
        *self.index.pages.range(..=key).next_back().unwrap().1
    }

    fn get(&mut self, key: &Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let object_id = self.pid_for_key(key.clone());
        let leaf_data = self.heap.read(object_id)?.unwrap();
        let leaf: Page = deserialize(&leaf_data).unwrap();
        Ok(leaf.kvs.get(key).cloned())
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.mutate(key, Some(value))
    }

    fn remove(&mut self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.mutate(key, None)
    }

    fn mutate(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) -> io::Result<Option<Vec<u8>>> {
        let object_id = self.pid_for_key(key.clone());
        let leaf_data = self.heap.read(object_id)?.unwrap();
        let mut leaf: Page = deserialize(&leaf_data).unwrap();
        let ret = if let Some(v) = value {
            // TODO Page split logic when it becomes large
            leaf.kvs.insert(key, v)
        } else {
            // TODO Page merge logic when it becomes small
            leaf.kvs.remove(&key)
        };

        let write_batch = [(object_id, Some(serialize(&leaf)))];

        self.heap.write_batch(write_batch)?;

        let stats = self.heap.stats();

        if stats.dead_objects > stats.live_objects {
            self.heap.maintenance()?;
        }

        Ok(ret)
    }
}

fn main() {
    let mut kv = Kv::recover("kv_example").unwrap();

    kv.set(b"yo".to_vec(), b"hi".to_vec()).unwrap();
    assert_eq!(&*kv.get(&b"yo".to_vec()).unwrap().unwrap(), b"hi");

    kv.remove(b"yo".to_vec()).unwrap();
    assert!(&kv.get(&b"yo".to_vec()).unwrap().is_none());
}
