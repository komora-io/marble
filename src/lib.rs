use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::io;
use std::num::NonZeroU64;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_queue::SegQueue;
use ebr::{Ebr, Guard};
use fault_injection::{fallible, maybe};
use fnv::FnvHashSet;
use fs2::FileExt as _;
use inline_array::InlineArray;
use metadata_store::MetadataStore;
use pagetable::PageTable;
use rayon::prelude::*;

const WARN: &str = "DO_NOT_PUT_YOUR_FILES_HERE";
const N_SLABS: usize = 32;

const fn slab_for_size(size: usize) -> u8 {
    size.next_power_of_two().trailing_zeros() as u8
}

#[derive(Debug, Clone)]
pub struct Stats {}

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "default.marble".into(),
        }
    }
}

impl Config {
    pub fn open(&self) -> io::Result<Marble> {
        let (marble, _user_data) = Marble::recover(&self.path)?;
        Ok(marble)
    }
}

struct SlabAddress {
    slab_id: u8,
    slab_slot: [u8; 7],
}

impl SlabAddress {
    fn from_slab_slot(slab: u8, slot: u64) -> SlabAddress {
        let slot_bytes = slot.to_be_bytes();

        assert_eq!(slot_bytes[0], 0);

        SlabAddress {
            slab_id: slab,
            slab_slot: slot_bytes[1..].try_into().unwrap(),
        }
    }

    fn slot(&self) -> u64 {
        u64::from_be_bytes([
            0,
            self.slab_slot[0],
            self.slab_slot[1],
            self.slab_slot[2],
            self.slab_slot[3],
            self.slab_slot[4],
            self.slab_slot[5],
            self.slab_slot[6],
        ])
    }
}

impl From<u64> for SlabAddress {
    fn from(i: u64) -> SlabAddress {
        let bytes = i.to_be_bytes();
        SlabAddress {
            slab_id: bytes[0],
            slab_slot: bytes[1..].try_into().unwrap(),
        }
    }
}

impl Into<u64> for SlabAddress {
    fn into(self) -> u64 {
        u64::from_be_bytes([
            self.slab_id,
            self.slab_slot[0],
            self.slab_slot[1],
            self.slab_slot[2],
            self.slab_slot[3],
            self.slab_slot[4],
            self.slab_slot[5],
            self.slab_slot[6],
        ])
    }
}

#[derive(Default, Debug)]
struct Allocator {
    free_and_pending: Mutex<BinaryHeap<Reverse<u64>>>,
    free_queue: SegQueue<u64>,
    next_to_allocate: AtomicU64,
}

impl Allocator {
    fn from_allocated(allocated: &FnvHashSet<u64>) -> Allocator {
        let mut heap = BinaryHeap::<Reverse<u64>>::default();
        let max = allocated.iter().copied().max();

        for i in 0..max.unwrap_or(0) {
            if !allocated.contains(&i) {
                heap.push(Reverse(i));
            }
        }

        Allocator {
            free_and_pending: Mutex::new(heap),
            free_queue: SegQueue::default(),
            next_to_allocate: max.map(|m| m + 1).unwrap_or(0).into(),
        }
    }

    fn allocate(&self) -> u64 {
        let pop_attempt = if let Ok(mut free) = self.free_and_pending.try_lock() {
            while let Some(free_id) = self.free_queue.pop() {
                free.push(Reverse(free_id));
            }
            free.pop()
        } else {
            None
        };

        if let Some(id) = pop_attempt {
            id.0
        } else {
            self.next_to_allocate.fetch_add(1, Ordering::Release)
        }
    }

    fn free(&self, id: u64) {
        if let Ok(mut free) = self.free_and_pending.try_lock() {
            while let Some(free_id) = self.free_queue.pop() {
                free.push(Reverse(free_id));
            }
            free.push(Reverse(id));
        } else {
            self.free_queue.push(id);
        }
    }
}

#[derive(Debug)]
struct Slab {
    file: fs::File,
    slot_size: usize,
    slot_allocator: Arc<Allocator>,
}

impl Slab {
    fn read(&self, slot: u64, _guard: &mut Guard<'_, DeferredFree, 1>) -> io::Result<Vec<u8>> {
        let mut ret = Vec::with_capacity(self.slot_size);
        unsafe {
            ret.set_len(self.slot_size);
        }

        let whence = self.slot_size as u64 * slot;

        self.file.read_exact_at(&mut ret, whence)?;

        Ok(ret)
    }

    fn write(&self, slot: u64, data: &[u8]) -> io::Result<()> {
        assert!(data.len() <= self.slot_size);

        let whence = self.slot_size as u64 * slot;

        self.file.write_all_at(&data, whence)
    }
}

struct DeferredFree {
    allocator: Arc<Allocator>,
    freed_slot: u64,
}

impl Drop for DeferredFree {
    fn drop(&mut self) {
        self.allocator.free(self.freed_slot)
    }
}

fn set_error(global_error: &AtomicPtr<(io::ErrorKind, String)>, error: &io::Error) {
    let kind = error.kind();
    let reason = error.to_string();

    let boxed = Box::new((kind, reason));
    let ptr = Box::into_raw(boxed);

    if global_error
        .compare_exchange(
            std::ptr::null_mut(),
            ptr,
            Ordering::SeqCst,
            Ordering::SeqCst,
        )
        .is_err()
    {
        // global fatal error already installed, drop this one
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }
}

#[derive(Clone)]
pub struct Marble {
    slabs: Arc<[Slab; N_SLABS]>,
    pt: PageTable<AtomicU64>,
    object_id_allocator: Arc<Allocator>,
    metadata_store: Arc<MetadataStore>,
    free_ebr: Ebr<DeferredFree, 1>,
    global_error: Arc<AtomicPtr<(io::ErrorKind, String)>>,
    #[allow(unused)]
    directory_lock: Arc<fs::File>,
}

impl Marble {
    fn check_error(&self) -> io::Result<()> {
        let err_ptr: *const (io::ErrorKind, String) = self.global_error.load(Ordering::Acquire);

        if err_ptr.is_null() {
            Ok(())
        } else {
            let deref: &(io::ErrorKind, String) = unsafe { &*err_ptr };
            Err(io::Error::new(deref.0, deref.1.clone()))
        }
    }

    fn set_error(&self, error: &io::Error) {
        set_error(&self.global_error, error);
    }

    pub fn recover<P: AsRef<Path>>(
        storage_directory: P,
    ) -> io::Result<(Marble, Vec<(u64, InlineArray)>)> {
        let path = storage_directory.as_ref();
        let slabs_dir = path.join("slabs");

        // initialize directories if not present
        for p in [&path.into(), &slabs_dir] {
            if let Err(e) = fs::read_dir(p) {
                if e.kind() == io::ErrorKind::NotFound {
                    fallible!(fs::create_dir_all(p));
                }
            }
        }

        let _ = fs::File::create(path.join(WARN));

        let mut file_lock_opts = fs::OpenOptions::new();
        file_lock_opts.create(false).read(false).write(false);
        let directory_lock = fallible!(fs::File::open(path));
        fallible!(directory_lock.try_lock_exclusive());

        let (metadata_store, recovered_metadata) = MetadataStore::recover(path.join("metadata"))?;

        let pt = PageTable::<AtomicU64>::default();
        let mut user_data = Vec::<(u64, InlineArray)>::with_capacity(recovered_metadata.len());
        let mut object_ids: FnvHashSet<u64> = Default::default();
        let mut slots_per_slab: [FnvHashSet<u64>; N_SLABS] = Default::default();
        for (k, location, data) in recovered_metadata {
            object_ids.insert(k);
            let slab_address = SlabAddress::from(location.get());
            slots_per_slab[slab_address.slab_id as usize].insert(slab_address.slot());
            pt.get(k).store(location.get(), Ordering::Relaxed);
            user_data.push((k, data.clone()));
        }

        let mut slabs = vec![];
        let mut slab_opts = fs::OpenOptions::new();
        slab_opts.create(true).read(true).write(true);
        for i in 0..N_SLABS {
            let slot_size = 1 << i;
            let slab_path = slabs_dir.join(format!("{}", slot_size));

            let file = fallible!(slab_opts.open(slab_path));

            slabs.push(Slab {
                slot_size,
                file,
                slot_allocator: Arc::new(Allocator::from_allocated(&slots_per_slab[i])),
            })
        }

        Ok((
            Marble {
                slabs: Arc::new(slabs.try_into().unwrap()),
                object_id_allocator: Arc::new(Allocator::from_allocated(&object_ids)),
                pt,
                metadata_store: Arc::new(metadata_store),
                directory_lock: Arc::new(directory_lock),
                free_ebr: Ebr::default(),
                global_error: Default::default(),
            },
            user_data,
        ))
    }

    pub fn maintenance(&self) -> io::Result<usize> {
        // TODO
        Ok(0)
    }

    pub fn stats(&self) -> Stats {
        Stats {}
    }

    pub fn read(&self, object_id: u64) -> io::Result<Option<Vec<u8>>> {
        self.check_error()?;

        let mut guard = self.free_ebr.pin();
        let location_u64 = self.pt.get(object_id).load(Ordering::Acquire);

        if location_u64 == 0 {
            return Ok(None);
        }

        let slab_address = SlabAddress::from(location_u64);

        let slab = &self.slabs[usize::from(slab_address.slab_id)];

        match slab.read(slab_address.slot(), &mut guard) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) => {
                self.set_error(&e);
                Err(e)
            }
        }
    }

    pub fn write_batch<B, I>(&self, batch: I) -> io::Result<()>
    where
        B: AsRef<[u8]> + Send,
        I: Sized + IntoIterator<Item = (u64, Option<(InlineArray, B)>)>,
    {
        self.check_error()?;
        let mut guard = self.free_ebr.pin();

        let batch: Vec<(u64, Option<(InlineArray, B)>)> = batch
            .into_iter()
            //.map(|(key, val_opt)| (key, val_opt.map(|(user_data, b)| (user_data, b.as_ref()))))
            .collect();

        let slabs = &self.slabs;
        let metadata_batch_res: io::Result<Vec<(u64, Option<(NonZeroU64, InlineArray)>)>> = batch
            .into_par_iter()
            .map(|(object_id, val_opt): (u64, Option<(InlineArray, B)>)| {
                let new_meta = if let Some((user_data, b)) = val_opt {
                    let bytes = b.as_ref();
                    let slab_id = slab_for_size(bytes.len());
                    let slab = &slabs[usize::from(slab_id)];
                    let slot = slab.slot_allocator.allocate();
                    let new_location = SlabAddress::from_slab_slot(slab_id, slot);
                    let new_location_u64: u64 = new_location.into();

                    let complete_durability_pipeline = maybe!(slab.write(slot, &bytes));

                    if let Err(e) = complete_durability_pipeline {
                        // can immediately free slot as the
                        slab.slot_allocator.free(slot);
                        return Err(e);
                    }
                    Some((NonZeroU64::new(new_location_u64).unwrap(), user_data))
                } else {
                    None
                };

                Ok((object_id, new_meta))
            })
            .collect();

        let metadata_batch = match metadata_batch_res {
            Ok(mb) => mb,
            Err(e) => {
                self.set_error(&e);
                return Err(e);
            }
        };

        if let Err(e) = self.metadata_store.insert_batch(metadata_batch.clone()) {
            self.set_error(&e);

            // this is very cold, so it's fine if it's not fast
            for (_object_id, value_opt) in metadata_batch {
                let (new_location_u64, _user_data) = value_opt.unwrap();
                let new_location = SlabAddress::from(new_location_u64.get());
                let slab_id = new_location.slab_id;
                let slab = &self.slabs[usize::from(slab_id)];
                slab.slot_allocator.free(new_location.slot());
            }
            return Err(e);
        }

        // now we can update in-memory metadata

        for (object_id, value_opt) in metadata_batch {
            let new_location = if let Some((nl, _user_data)) = value_opt {
                nl.get()
            } else {
                0
            };

            let last_u64 = self.pt.get(object_id).swap(new_location, Ordering::Release);

            if last_u64 != 0 {
                let last_address = SlabAddress::from(last_u64);

                guard.defer_drop(DeferredFree {
                    allocator: self.slabs[usize::from(last_address.slab_id)]
                        .slot_allocator
                        .clone(),
                    freed_slot: last_address.slot(),
                });
            }
        }

        Ok(())
    }

    pub fn allocate<B: AsRef<[u8]>>(&self, user_data: InlineArray, object: B) -> io::Result<u64> {
        let new_id = self.object_id_allocator.allocate();
        if let Err(e) = self.write_batch([(new_id, Some((user_data, object.as_ref())))]) {
            self.object_id_allocator.free(new_id);
            Err(e)
        } else {
            Ok(new_id)
        }
    }

    pub fn free(&self, object_id: u64) -> io::Result<()> {
        let mut guard = self.free_ebr.pin();
        if let Err(e) = self.metadata_store.insert_batch([(object_id, None)]) {
            self.set_error(&e);
            return Err(e);
        }
        let last_u64 = self.pt.get(object_id).swap(0, Ordering::Release);
        if last_u64 != 0 {
            let last_address = SlabAddress::from(last_u64);

            guard.defer_drop(DeferredFree {
                allocator: self.slabs[usize::from(last_address.slab_id)]
                    .slot_allocator
                    .clone(),
                freed_slot: last_address.slot(),
            });
        }

        Ok(())
    }
}
