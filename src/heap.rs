use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use crate::Result;

pub struct Reservation;

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

pub struct Heap {
    slab_00: Slab<4_096>,
    slab_01: Slab<8_192>,
    slab_02: Slab<16_384>,
    slab_03: Slab<32_768>,
    slab_04: Slab<65_536>,
    slab_05: Slab<131_072>,
    slab_06: Slab<262_144>,
    slab_07: Slab<524_288>,
    slab_08: Slab<1_048_576>,
    slab_09: Slab<2_097_152>,
    slab_10: Slab<4_194_304>,
    slab_11: Slab<8_388_608>,
    slab_12: Slab<16_777_216>,
    slab_13: Slab<33_554_432>,
    slab_14: Slab<67_108_864>,
    slab_15: Slab<134_217_728>,
    slab_16: Slab<268_435_456>,
    slab_17: Slab<536_870_912>,
    slab_18: Slab<1_073_741_824>,
    slab_19: Slab<2_147_483_648>,
    slab_20: Slab<4_294_967_296>,
    slab_21: Slab<8_589_934_592>,
    slab_22: Slab<17_179_869_184>,
    slab_23: Slab<34_359_738_368>,
    slab_24: Slab<68_719_476_736>,
    slab_25: Slab<137_438_953_472>,
    slab_26: Slab<274_877_906_944>,
    slab_27: Slab<549_755_813_888>,
    slab_28: Slab<1_099_511_627_776>,
    slab_29: Slab<2_199_023_255_552>,
    slab_30: Slab<4_398_046_511_104>,
    slab_31: Slab<8_796_093_022_208>,
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

        Ok(Heap {
            slab_31: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_30: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_29: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_28: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_27: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_26: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_25: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_24: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_23: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_22: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_21: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_20: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_19: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_18: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_17: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_16: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_15: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_14: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_13: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_12: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_11: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_10: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_09: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_08: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_07: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_06: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_05: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_04: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_03: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_02: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_01: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
            slab_00: Slab {
                file: files.pop().unwrap(),
                free: Default::default(),
            },
        })
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

struct Slab<const SZ: u64> {
    free: BTreeSet<u32>,
    file: File,
}

impl<const SZ: u64> Slab<SZ> {}
