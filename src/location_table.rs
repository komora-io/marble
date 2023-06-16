use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use concurrent_map::ConcurrentMap;

use crate::{DiskLocation, ObjectId};

const fn _test_impls() {
    const fn send<T: Send>() {}
    const fn clone<T: Clone>() {}
    send::<LocationTable>();
    clone::<LocationTable>();
}

#[derive(Default, Clone)]
pub struct LocationTable {
    pt: ConcurrentMap<u64, DiskLocation>,
    max_object_id: Arc<AtomicU64>,
}

impl LocationTable {
    pub fn load(&self, object_id: ObjectId) -> Option<DiskLocation> {
        self.pt.get(&object_id)
    }

    pub fn store(&self, object_id: ObjectId, location: DiskLocation) {
        self.max_object_id.fetch_max(object_id, Ordering::Release);

        self.pt.insert(object_id, location);
    }

    pub fn cas(
        &self,
        object_id: ObjectId,
        old_location: DiskLocation,
        new_location: DiskLocation,
    ) -> Result<(), DiskLocation> {
        self.max_object_id.fetch_max(object_id, Ordering::Release);

        self.pt
            .cas(object_id, Some(&old_location), Some(new_location))
            .map(|_| ())
            .map_err(|r| r.actual.unwrap())
    }

    pub fn fetch_max(
        &self,
        object_id: ObjectId,
        new_location: DiskLocation,
    ) -> Result<Option<DiskLocation>, Option<DiskLocation>> {
        self.max_object_id.fetch_max(object_id, Ordering::Release);

        let last_value_opt = self.pt.fetch_and_update(object_id, |current_opt| {
            Some(if let Some(current) = current_opt {
                (*current).max(new_location)
            } else {
                new_location
            })
        });

        match last_value_opt {
            None => Ok(None),
            Some(last_value) if last_value < new_location => Ok(Some(last_value)),
            Some(last_value) => {
                assert!(last_value > new_location);
                Err(Some(last_value))
            }
        }
    }

    pub fn max_object_id(&self) -> u64 {
        self.max_object_id.load(Ordering::Acquire)
    }

    #[cfg(feature = "runtime_validation")]
    pub fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (ObjectId, DiskLocation)> {
        (0..=self.max_object_id.load(Ordering::Acquire)).filter_map(|object_id| {
            if let Some(loc) = self.load(object_id) {
                Some((object_id, loc))
            } else {
                None
            }
        })
    }
}
