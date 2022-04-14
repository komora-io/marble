use std::collections::HashMap;
use std::io;
use std::ops::{Deref, DerefMut};

mod ownership_sketch;

type PageId = u64;
type TxId = u64;
type Page = u8;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Cow<'a, T: Clone> {
    Owned(T),
    Borrowed(&'a T),
}

impl<'a, T: Clone> Deref for Cow<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match self {
            Cow::Owned(ref t) => t,
            Cow::Borrowed(t) => t,
        }
    }
}

impl<'a, T: Clone> DerefMut for Cow<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct VersionedPage {
    txid: TxId,
    page: (),
}

#[derive(Default, Debug)]
struct MemCache {
    next_txid: TxId,
    inner: HashMap<PageId, VersionedPage>,
}

impl Cache for MemCache {
    fn tx(&mut self) -> Tx<'_> {
        let txid = self.next_txid;
        self.next_txid += 1;

        Tx {
            txid,
            workspace: HashMap::new(),
            cache: self,
        }
    }

    fn stabilize(&mut self, txid: TxId) -> io::Result<()> {
        Ok(())
    }
}

trait Cache {
    fn tx(&mut self) -> Tx<'_>;
    fn stabilize(&mut self, txid: TxId) -> io::Result<()>;
}

struct Tx<'a> {
    txid: TxId,
    cache: &'a mut dyn Cache,
    workspace: HashMap<PageId, Cow<'a, VersionedPage>>,
}

impl<'a> Tx<'a> {
    fn allocate(&mut self, page: Page) -> PageId {
        todo!()
    }

    fn get(&mut self, pid: PageId) -> io::Result<&mut Cow<'a, Page>> {
        if !self.workspace.contains_key(&pid) {}
        todo!()
    }

    fn commit(mut self) -> io::Result<TxId> {
        for (pid, page) in &self.workspace {
            match page {
                Cow::Borrowed(p) => {}
                Cow::Owned(p) => {}
            }
        }

        todo!()
    }

    fn abort(mut self) {
        self.workspace.clear();
        self.commit();
    }
}

fn main() {
    let mut cache = MemCache::default();

    let mut tx = cache.tx();

    let a_pid = tx.allocate(0);
    let b_pid = tx.allocate(128);

    let txid = tx.commit().unwrap();
    cache.stabilize(txid).unwrap();

    /*
    {
        let mut tx = cache.tx();

        let mut a = tx.get(a_pid).unwrap();
        let mut b = tx.get(b_pid).unwrap();

        std::mem::swap(&mut a, &mut b);

        let txid = tx.commit().unwrap();
        cache.stabilize(txid).unwrap();
    }

    {
        let mut tx = cache.tx();

        let mut a = tx.get(a_pid).unwrap();
        let mut b = tx.get(b_pid).unwrap();

        assert_eq!(**a, 128_u8);
        assert_eq!(**b, 0);

        let txid = tx.commit().unwrap();
        cache.stabilize(txid).unwrap();
    }
    */
}
