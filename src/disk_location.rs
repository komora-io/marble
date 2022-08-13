use std::num::NonZeroU64;

fn shift_location(location: u64, is_delete: bool) -> u64 {
    assert_eq!(location << 1 >> 1, location);
    let inner = if is_delete {
        location << 1
    } else {
        (location << 1) + 1
    };

    inner
}

fn unshift_location(location: u64) -> (u64, bool) {
    if location % 2 == 0 {
        (location >> 1, true)
    } else {
        (location >> 1, false)
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RelativeDiskLocation(u64);

impl RelativeDiskLocation {
    pub fn new(offset: u64, is_delete: bool) -> RelativeDiskLocation {
        RelativeDiskLocation(shift_location(offset, is_delete))
    }

    pub fn to_raw(&self) -> u64 {
        self.0
    }

    pub fn from_raw(raw: u64) -> RelativeDiskLocation {
        RelativeDiskLocation(raw)
    }

    pub fn to_absolute(&self, base: u64) -> DiskLocation {
        let (offset, is_delete) = unshift_location(self.0);
        let absolute = offset.checked_add(base).unwrap();
        DiskLocation::new(absolute, is_delete)
    }

    fn unshift(&self) -> (u64, bool) {
        unshift_location(self.0)
    }

    pub fn is_delete(&self) -> bool {
        self.unshift().1
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DiskLocation(NonZeroU64);

impl DiskLocation {
    pub fn new(location: u64, is_delete: bool) -> DiskLocation {
        DiskLocation(NonZeroU64::new(shift_location(location, is_delete)).unwrap())
    }

    pub fn from_raw(u: u64) -> Option<DiskLocation> {
        Some(DiskLocation(NonZeroU64::new(u)?))
    }

    pub fn to_raw(&self) -> u64 {
        self.0.get()
    }

    fn unshift(&self) -> (u64, bool) {
        unshift_location(self.0.get())
    }

    pub fn lsn(&self) -> u64 {
        self.unshift().0
    }

    pub fn is_delete(&self) -> bool {
        self.unshift().1
    }
}
