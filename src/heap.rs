use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use crate::Result;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
#[repr(C)]
pub struct HeapOffset {
    pub index: u32,
    pub slab: u8,
    pub generation: u8,
}

impl HeapOffset {
    fn from_bytes(bytes: [u8; 8]) -> HeapOffset {
        todo!()
    }

    fn to_bytes(self) -> [u8; 8] {
        todo!()
    }
}

fn slab_for_size(size: usize) -> u8 {
    u8::try_from(size.trailing_zeros().max(12) - 12).unwrap()
}

fn size_for_slab(index: u8) -> usize {
    4096 * (1 << (index as usize))
}

pub struct Heap {
    slabs: [Slab; 32],
}

impl Heap {
    pub fn recover_or_create<P: AsRef<Path>>(path: P) -> Result<Heap> {
        let mut options = OpenOptions::new();
        options.read(true).write(true);

        let mut path_buf = path.as_ref().to_path_buf();
        path_buf.push("heap");

        // create our directory if it doesn't exist
        if let Err(e) = std::fs::File::open(&path_buf) {
            if e.kind() == io::ErrorKind::NotFound {
                std::fs::create_dir(&path_buf)?;
            }
        }

        let mut files = match Heap::open(&path_buf, &options) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                options.create(true);
                Heap::open(&path_buf, &options)?
            }
            Err(e) => return Err(e.into()),
        };

        let slabs_vec: Vec<Slab> = files
            .into_iter()
            .map(|file| Slab { file, free: Default::default() })
            .collect();

        let slabs: [Slab; 32] = slabs_vec.try_into().unwrap();

        Ok(Heap { slabs })
    }

    fn open(path: &Path, options: &OpenOptions) -> Result<Vec<File>> {
        let mut files = vec![];

        for i in 0..32 {
            let full_path = path.join(format!("heap_{:02}", i));
            let f = options.open(full_path)?;

            files.push(f);
        }

        Ok(files)
    }
}

#[derive(Debug)]
struct Slab {
    free: BTreeSet<u32>,
    file: File,
}

impl Slab {}
