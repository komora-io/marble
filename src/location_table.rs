use std::sync::atomic::Ordering;

use crate::{DiskLocation, ObjectId};

#[derive(Default)]
pub struct LocationTable(pagetable::PageTable);

impl LocationTable {
    pub fn load(&self, object_id: ObjectId) -> Option<DiskLocation> {
        let raw = self.0.get(object_id).load(Ordering::Acquire);
        DiskLocation::from_raw(raw)
    }

    pub fn store(&self, object_id: ObjectId, location: DiskLocation) {
        self.0
            .get(object_id)
            .store(location.to_raw(), Ordering::Release);
    }

    pub fn cas(
        &self,
        object_id: ObjectId,
        old_location: DiskLocation,
        new_location: DiskLocation,
    ) -> Result<DiskLocation, DiskLocation> {
        self.0
            .get(object_id)
            .compare_exchange(
                old_location.to_raw(),
                new_location.to_raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|r| DiskLocation::from_raw(r).unwrap())
            .map_err(|r| DiskLocation::from_raw(r).unwrap())
    }

    pub fn fetch_max(
        &self,
        object_id: ObjectId,
        new_location: DiskLocation,
    ) -> Result<Option<DiskLocation>, Option<DiskLocation>> {
        let max_result = self
            .0
            .get(object_id)
            .fetch_max(new_location.to_raw(), Ordering::AcqRel);

        if max_result < new_location.to_raw() {
            Ok(DiskLocation::from_raw(max_result))
        } else {
            Err(DiskLocation::from_raw(max_result))
        }
    }
}
