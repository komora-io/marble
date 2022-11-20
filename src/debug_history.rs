use std::collections::HashSet;

use crate::{DiskLocation, Map, ObjectId};

/// `DebugHistory` asserts that when objects are added to
/// new files and marked as removed from old ones, that the
/// operation is unique and that removals only happen for
/// things that were already added.
#[derive(Default, Debug)]
pub struct DebugHistory {
    adds: Map<ObjectId, HashSet<DiskLocation>>,
    removes: Map<ObjectId, HashSet<DiskLocation>>,
}

impl DebugHistory {
    pub fn mark_add(&mut self, object_id: ObjectId, location: DiskLocation) {
        if let Some(removes) = self.removes.get(&object_id) {
            let present_in_removes = removes.contains(&location);
            assert!(!present_in_removes);
        }

        let entry = self.adds.entry(object_id).or_default();
        let new_addition = entry.insert(location);
        assert!(new_addition);
    }

    pub fn mark_remove(&mut self, object_id: ObjectId, location: DiskLocation) {
        let present_in_adds = self.adds[&object_id].contains(&location);
        assert!(present_in_adds);

        let entry = self.removes.entry(object_id).or_default();
        let new_addition = entry.insert(location);
        assert!(new_addition);
    }
}
