use std::path::Path;
use std::{fs, io};

use tiny_lsm::Lsm;

pub struct Location(u64);

impl From<[u8; 8]> for Location {
    fn from(buf: [u8; 8]) -> Location {
        Location(u64::from_le_bytes(buf))
    }
}

impl Into<[u8; 8]> for Location {
    fn into(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }
}

pub struct DiskSlab<const SZ: usize> {
    meta: Lsm<8, 8>,
    file: fs::File,
}

impl<const SZ: usize> DiskSlab<SZ> {
    pub fn recover<P: AsRef<Path>>(path: P) -> io::Result<DiskSlab<SZ>> {
        todo!()
    }

    pub fn insert(&mut self, page: [u8; SZ]) -> io::Result<Location> {
        todo!()
    }

    pub fn get(&mut self, location: Location) -> io::Result<[u8; SZ]> {
        todo!()
    }
}
