/// build with --features=serde,bincode
use std::collections::BTreeMap;
use std::io;

use bincode::{deserialize, serialize};
use marble::Marble;
use serde::{Deserialize, Serialize};

type ObjectId = u64;

#[derive(Debug)]
struct Index {
    pages: BTreeMap<Vec<u8>, ObjectId>,
    max_pid: u64,
}

impl Default for Index {
    fn default() -> Self {
        Index {
            pages: Default::default(),
            max_pid: 1,
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
        let (heap, recovered_index) = marble::recover(path)?;

        let mut index = Index::default();
        for (object_id, low_key) in recovered_index {
            index.max_pid = index.max_pid.max(object_id);
            index.pages.insert(low_key.to_vec(), object_id);
        }

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

        let write_batch = [(
            object_id,
            Some((leaf.lo.clone().into(), serialize(&leaf).unwrap())),
        )];

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

        let write_batch = [(
            object_id,
            Some((leaf.lo.clone().into(), serialize(&leaf).unwrap())),
        )];

        self.heap.write_batch(write_batch)?;

        let stats = self.heap.stats();

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
