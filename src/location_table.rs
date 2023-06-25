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
    table: ConcurrentMap<u64, DiskLocation>,
}

impl LocationTable {
    pub fn load(&self, object_id: ObjectId) -> Option<DiskLocation> {
        self.table.get(&object_id)
    }

    pub fn store(&self, object_id: ObjectId, location: DiskLocation) {
        self.table.insert(object_id, location);
    }

    pub fn cas(
        &self,
        object_id: ObjectId,
        old_location: DiskLocation,
        new_location: DiskLocation,
    ) -> Result<(), DiskLocation> {
        self.table
            .cas(object_id, Some(&old_location), Some(new_location))
            .map(|_| ())
            .map_err(|r| r.actual.unwrap())
    }

    pub fn fetch_max(
        &self,
        object_id: ObjectId,
        new_location: DiskLocation,
    ) -> Result<Option<DiskLocation>, Option<DiskLocation>> {
        let last_value_opt = self.table.fetch_max(object_id, new_location);

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
        self.table.last().map(|(k, _v)| k).unwrap_or(0)
    }

    #[cfg(feature = "runtime_validation")]
    pub fn iter<'a>(&'a self) -> impl 'a + Iterator<Item = (ObjectId, DiskLocation)> {
        self.table.iter()
    }
}
