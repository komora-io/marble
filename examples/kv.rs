/// build with --features=serde,bincode
use std::collections::{HashMap, BTreeMap};
use std::io;

use bincode::{deserialize, serialize};
use serde::{Serialize, Deserialize};
use marble::{PageId, Marble};

fn index_pid() -> PageId {
    PageId::new(1).unwrap()
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct Index {
    pages: BTreeMap<Vec<u8>, PageId>,
    max_pid: u64,
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
// compacted into a Marble write batch.
struct Kv {
    write_cache: HashMap<PageId, Page>,
    heap: Marble,
    index: Index,
}

impl Kv {
    fn recover(path: &str) -> io::Result<Kv> {
        let heap = Marble::open(path)?;

        let index: Index = if let Some(index_data) = heap.read(index_pid())? {
            deserialize(&index_data).unwrap()
        } else {
            Index::default()
        };

        let mut kv = Kv {
            write_cache: HashMap::new(),
            index,
            heap,
        };

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
        let pid = self.index.max_pid;

        let previous = self.index.pages.insert(leaf.lo.clone(), PageId::new(pid).unwrap());
        assert!(previous.is_none());

        let write_batch = [
            (PageId::new(pid).unwrap(), Some(serialize(&leaf).unwrap())),
            (index_pid(), Some(serialize(&self.index).unwrap())),
        ].into_iter().collect();

        self.heap.write_batch(write_batch)
    }

    fn pid_for_key(&self, key: &Vec<u8>) -> PageId {
        *self.index.pages.range(..=key).next_back().unwrap().1
    }

    fn get(&mut self, key: &Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let pid = self.pid_for_key(key);
        let leaf_data = self.heap.read(pid)?.unwrap();
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
        let pid = self.pid_for_key(&key);
        let leaf_data = self.heap.read(pid)?.unwrap();
        let mut leaf: Page = deserialize(&leaf_data).unwrap();
        let ret = if let Some(v) = value {
            // TODO Page split logic when it becomes large
            leaf.kvs.insert(key, v)
        } else {
            // TODO Page merge logic when it becomes small
            leaf.kvs.remove(&key)
        };

        let write_batch = [(pid, Some(serialize(&leaf).unwrap()))].into_iter().collect();

        self.heap.write_batch(write_batch)?;

        Ok(ret)
    }
}

fn main() {
    let mut kv = Kv::recover("kv_example").unwrap();
    kv.set(b"yo".into(), b"hi".into()).unwrap();
    assert_eq!(&*kv.get(&b"yo".to_vec()).unwrap().unwrap(), b"hi");
}
