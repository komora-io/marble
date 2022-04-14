use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const CONFLICT_SPACE: usize = 1024;

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
