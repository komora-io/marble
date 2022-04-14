//! marx makes marks on ya diskz.
//! ready for ya teeth to fall out?

// owner -{deltas, page refs}-> log
// log -{bounded page refs}-> heap
// heap is just a slab allocator
// 4k default slab size
// a logged pagetable tracks frees, max pid, and bigger pages

pub struct Marx;

impl Marx {
    pub fn apply_delta(&mut self) {}
}

type Page = Vec<u8>;
type TxId = u64;
type PageId = u64;

struct Update<'a> {
    delta: Delta,
    pid: PageId,
    page: &'a Page,
}

struct WriteBatch<'a> {
    txid: TxId,
    updates: Vec<Update<'a>>,
}

struct PageTable {
    max_pid: PageId,
    non_default_pages: BTreeMap<PageId, SlabAddress>,
    free_pids: BTreeSet<PageId>,
    free_slots: [SlabManager; 32],
}

struct SlabAddress {
    slab: u8,
    slot: u32,
}

struct SlabManager {
    max_slot: u32,
    free_slots: BTreeSet<u32>,
}

enum LogDelta {
    Allocate(PageId),
    Free(PageId),

}

enum PageTableDelta {
    Place(PageId, Option<SlabAddress>),
    Free(PageId),


}

#[test]
fn general_test() {
    let mut marx = marx::open("general_test").unwrap();
    marx.log(
}
