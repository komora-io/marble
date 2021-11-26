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

use crate::{
    Error, Heap, HeapOffset, Log, Page, PageId, Result, TxId, WriteBatch,
};

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
    concurrency_control: ConcurrencyControl,
    stable_txid: TxId,
    next_txid: TxId,
    next_page_id: PageId,
    log: Log,
    heap: Heap,
}

impl PageCache {
    pub fn recover_or_create<P: AsRef<Path>>(path: P) -> Result<PageCache> {
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
            /*
            let heap_offset = table.offsets.get(&pid.0).unwrap();
            if !cache.residents.contains(&pid) {
                let page = heap.read(&heap_offset)?;
                cache.residents.insert(pid, page);
            }

            let mut page = cache.residents.get_mut(&pid).unwrap();

            page.apply(update);
            */
        }

        /*
        for (pid, page) in cache.dirty() {
            todo!("")
        }
        */

        //

        todo!()
    }

    pub fn tx(&mut self) -> Transaction {
        let next_txid = self.next_txid;
        self.next_txid.0 += 1;

        Transaction {
            committed: false,
            concurrency_control: Default::default(),
            cache: HashMap::new(),
            txid: next_txid,
            pagecache: self,
        }
    }

    fn page_in(&mut self, pid: PageId) -> Result<(TxId, &Page)> {
        todo!()
    }
}

pub struct TransactionReceipt<'a> {
    pagecache: &'a mut PageCache,
    txid: TxId,
}

impl<'a> TransactionReceipt<'a> {
    /// blocks until the committed transaction is stable on-disk.
    pub fn stabilize(&mut self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionConflict;

#[derive(Debug)]
struct ConcurrencyControl {
    conflict_space: Arc<[AtomicU64; CONFLICT_SPACE]>,
}

impl Default for ConcurrencyControl {
    fn default() -> ConcurrencyControl {
        todo!()
    }
}

impl ConcurrencyControl {
    fn try_stage(
        &mut self,
        updates: &HashMap<PageId, CachedPage<'_>>,
    ) -> std::result::Result<Claim, TransactionConflict> {
        todo!("perform cc")
    }
}

pub struct Claim<'a> {
    concurrency_control: &'a mut ConcurrencyControl,
    updates: &'a HashMap<PageId, CachedPage<'a>>,
}

impl<'a> Drop for Claim<'a> {
    fn drop(&mut self) {
        todo!("clear claims now that logging is done")
    }
}

struct CachedPage<'a> {
    read_txid: TxId,
    page: Cow<'a, Page>,
}

#[must_use]
pub struct Transaction<'a> {
    cache: HashMap<PageId, CachedPage<'a>>,
    committed: bool,
    txid: TxId,
    pagecache: &'a mut PageCache,
    concurrency_control: ConcurrencyControl,
}

impl<'a> Transaction<'a> {
    pub fn new_page(&mut self, page: Page) -> (PageId, Cow<Page>) {
        let next_page_id = self.pagecache.next_page_id;
        self.pagecache.next_page_id.0 += 1;

        todo!()
    }

    pub fn get(&'a mut self, pid: PageId) -> Result<&'a mut Cow<'a, Page>> {
        if !self.cache.contains_key(&pid) {
            let (read_txid, page_ref) = self.pagecache.page_in(pid)?;

            let cached_page =
                CachedPage { read_txid, page: Cow::Borrowed(page_ref) };

            self.cache.insert(pid, cached_page);
        }

        Ok(&mut self.cache.get_mut(&pid).expect("just paged this in...").page)
    }

    pub fn commit(
        mut self,
    ) -> Result<std::result::Result<TransactionReceipt<'a>, TransactionConflict>>
    {
        let tx_claim =
            match self.pagecache.concurrency_control.try_stage(&self.cache) {
                Ok(tx_claim) => tx_claim,
                Err(conflict) => return Ok(Err(TransactionConflict)),
            };

        let cache = std::mem::take(&mut self.cache);

        let updates = cache
            .into_iter()
            .filter(|(pid, page)| matches!(&page.page, Cow::Owned(_)))
            .map(|(pid, page)| (pid, page.page.into_owned()))
            .collect();

        let write_batch = WriteBatch { txid: self.txid, updates };

        self.pagecache.log.push(write_batch, tx_claim);

        Ok(Ok(TransactionReceipt {
            txid: self.txid,
            pagecache: &mut *self.pagecache,
        }))
    }

    pub fn abort(mut self) -> Result<TransactionReceipt<'a>> {
        self.cache.clear();

        match self.commit() {
            Ok(Ok(tx_receipt)) => Ok(tx_receipt),
            Err(e) => Err(e),
            _ => unreachable!("empty commit batch should never conflict"),
        }
    }
}

#[test]
fn basic() {
    let mut pc =
        PageCache::recover_or_create("test_log").expect("recovery_or_create");

    let mut tx = pc.tx();

    let (pid, mut page) =
        tx.new_page(HeapOffset { slab: 0, index: 0, generation: 1 });

    assert_eq!(pid.0, 0);

    page.to_mut().generation += 1;

    dbg!(page);

    let mut receipt = tx.commit().expect("io").expect("committed");

    receipt.stabilize();

    std::fs::remove_dir_all("test_log").expect("cleanup");
}
