/// writepath:
///     - copy page into active tx cache if not already there
///     - apply update to copy
///     - on commit, move page
///
/// readpath:
///     - check active segment cache
///     - check inactive segment cache
///     - check promoted window cache
///     - check entry window cache
///     - page-in from heap
///
/// segment rolling
///     - flush requested causes IO thread to wake up or get to it when ready
///
use std::{
    borrow::Cow,
    collections::HashMap,
    fs::OpenOptions,
    io,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use serde::{Deserialize, Serialize};

use crate::{Error, Heap, HeapOffset, Log, Page, PageId, TxId};

const CONFLICT_SPACE: usize = 1024;

/// Prospective writes destined for the log
pub struct AppendReservation;

/// Prospective writes destined for the heap
pub struct ReplaceReservation;

#[derive(Default, Debug)]
struct Cache {
    residents: HashMap<PageId, Page>,
    dirty_in_progress: (),
    dirty_flushing: (),
    read_entrance: (),
    read_window: (),
    entry_sketch: (),
}

/// Stores the on-disk locations for pages.
#[derive(Serialize, Deserialize, Debug)]
struct PageTable {
    offsets: Vec<HeapOffset>,
}

pub struct PageCache {
    table: PageTable,
    cache: Cache,
    conflict_detector: Arc<[AtomicU64; CONFLICT_SPACE]>,
    stable_txid: TxId,
    next_txid: TxId,
    next_page_id: PageId,
    log: Log,
    heap: Heap,
}

impl PageCache {
    pub fn recover_or_create<P: AsRef<Path>>(path: P) -> Result<PageCache, Error> {
        // create our directory if it doesn't exist
        if let Err(e) = std::fs::File::open(path.as_ref()) {
            if e.kind() == io::ErrorKind::NotFound {
                std::fs::create_dir(path.as_ref())?;
            }
        }

        // open or create heap
        let mut heap = Heap::recover_or_create(&path)?;

        let mut cache = Cache::default();

        // open and apply or create log
        let log_recovery_iterator = Log::recover(path.as_ref());
        for pid_page_res in log_recovery_iterator {
            let (pid, page) = pid_page_res?;
            // page-in
            let heap_offset = table.offsets.get(&pid.0).unwrap();
            if !cache.residents.contains(&pid) {
                let page = heap.read(&heap_offset)?;
                cache.residents.insert(pid, page);
            }

            let mut page = cache.residents.get_mut(&pid).unwrap();

            page.apply(update);
        }

        for (pid, page) in cache.dirty() {
            todo!("")
        }

        //

        todo!()
    }

    pub fn tx(&mut self) -> Transaction {
        let next_txid = self.next_txid;
        self.next_txid.0 += 1;

        Transaction {
            committed: false,
            cache: HashMap::new(),
            txid: next_txid,
            pagecache: self,
        }
    }

    fn page_in(&mut self, pid: PageId) -> io::Result<(TxId, &Page)> {
        todo!()
    }
}

pub struct TransactionReceipt<'a> {
    pagecache: &'a mut PageCache,
    txid: TxId,
}

impl<'a> TransactionReceipt<'a> {
    /// blocks until the committed transaction is stable on-disk.
    pub fn stabilize(&mut self) -> io::Result<()> {
        todo!()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionConflict;

struct CachedPage<'a> {
    read_txid: TxId,
    page: Cow<'a, Page>,
}

pub struct Transaction<'a> {
    cache: HashMap<PageId, CachedPage<'a>>,
    committed: bool,
    txid: TxId,
    pagecache: &'a mut PageCache,
}

impl<'a> Transaction<'a> {
    pub fn new_page(&mut self, page: Page) -> (PageId, Cow<Page>) {
        let next_page_id = self.pagecache.next_page_id;
        self.pagecache.next_page_id.0 += 1;

        todo!()
    }

    pub fn get(&'a mut self, pid: PageId) -> io::Result<&'a mut Cow<'a, Page>> {
        if !self.cache.contains_key(&pid) {
            let (read_txid, page_ref) = self.pagecache.page_in(pid)?;

            let cached_page = CachedPage {
                read_txid,
                page: Cow::Borrowed(page_ref),
            };

            self.cache.insert(pid, cached_page);
        }

        Ok(&mut self
            .cache
            .get_mut(&pid)
            .expect("just paged this in...")
            .page)
    }

    fn commit(self) -> io::Result<Result<TransactionReceipt<'a>, TransactionConflict>> {
        todo!()
    }

    fn abort(self) {
        drop(self);
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        todo!()
    }
}

#[test]
fn basic() {
    let mut pc = PageCache::recover_or_create("test_log").expect("recovery_or_create");

    let mut tx = pc.tx();

    let (pid, mut page) = tx.new_page(HeapOffset {
        slab: 0,
        index: 0,
        generation: 1,
    });

    assert_eq!(pid.0, 0);

    page.to_mut().generation += 1;

    dbg!(page);

    let mut receipt = tx.commit().expect("io").expect("committed");

    receipt.stabilize();

    std::fs::remove_dir_all("test_log").expect("cleanup");
}
