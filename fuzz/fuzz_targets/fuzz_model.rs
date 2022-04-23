#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate arbitrary;
extern crate marble;
extern crate uuid;

use std::collections::HashMap;

use arbitrary::Arbitrary;

use marble::{Config as MarbleConfig, PageId};

#[derive(Debug)]
struct Config(MarbleConfig);

impl<'a> Arbitrary<'a> for Config {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let path = std::path::Path::new("fuzz_model_dirs").join(uuid::Uuid::new_v4().to_string()).into();


        Ok(Config(MarbleConfig {
            path,
            target_file_size: u.int_in_range(1..=16384)?,
            file_compaction_percent: u.int_in_range(0..100)?,
            ..Default::default()
        }))
    }
}

#[derive(Debug)]
struct WriteBatch(HashMap<PageId, Vec<u8>>);

impl<'a> Arbitrary<'a> for WriteBatch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let pages: u8 = Arbitrary::arbitrary(u)?;

        let mut batch = HashMap::default();
        for _ in 0..pages {
            let pid: u8 = Arbitrary::arbitrary(u)?;
            let sz: u8 = Arbitrary::arbitrary(u)?;

            let page = vec![0b10101010; sz as usize];

            batch.insert(PageId(pid as u64), page);
        }

        Ok(WriteBatch(batch))
    }
}

#[derive(Debug, Arbitrary)]
enum Op {
    WriteBatch(WriteBatch),
    Restart,
}

fuzz_target!(|args: (Config, Vec<Op>)| {
    let (config, ops) = args;

    let mut marble = config.0.clone().open().unwrap();
    let mut model = std::collections::BTreeMap::new();

    /*
    println!();
    println!("~~~~~~~~~~~~~~~");
    println!();
    */
    for op in ops {
        // println!("op: {:?}", op);
        match op {
            Op::WriteBatch(write_batch) => {
                for (k, v) in &write_batch.0 {
                    model.insert(*k, v.clone());
                }
                marble.write_batch(write_batch.0).unwrap()
            }
            Op::Restart => {
                drop(marble);
                marble = config.0.clone().open().unwrap();
            }
        };

        for (pid, expected) in &model {
            assert_eq!(expected, &*marble.read(*pid).unwrap());
        }
    }

    drop(marble);

    std::fs::remove_dir_all(config.0.path).unwrap();
});

