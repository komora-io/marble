#![no_main]
#[macro_use]
extern crate libfuzzer_sys;
extern crate arbitrary;
extern crate marble;
extern crate uuid;

use std::collections::HashMap;

use arbitrary::Arbitrary;

use marble::Config as MarbleConfig;

const TEST_DIR: &str = "testing_data_directories";
const KEYSPACE: u64 = 64;
const BATCH_MIN_SZ: usize = 0;
const BATCH_MAX_SZ: usize = 16;

type ObjectId = u64;

#[derive(Debug)]
struct Config(MarbleConfig);

impl<'a> Arbitrary<'a> for Config {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let path = std::path::Path::new(TEST_DIR)
            .join("fuzz")
            .join(uuid::Uuid::new_v4().to_string())
            .into();

        Ok(Config(MarbleConfig {
            path,
            target_file_size: u.int_in_range(1..=16384).unwrap_or(1),
            file_compaction_percent: u.int_in_range(0..=99).unwrap_or(0),
            ..Default::default()
        }))
    }
}

#[derive(Debug)]
struct WriteBatch(HashMap<ObjectId, Option<Vec<u8>>>);

impl<'a> Arbitrary<'a> for WriteBatch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let pages: usize = u
            .int_in_range(BATCH_MIN_SZ..=BATCH_MAX_SZ)
            .unwrap_or(BATCH_MIN_SZ);

        let mut batch = HashMap::default();
        for _ in 0..pages {
            let pid: u64 = u.int_in_range(0..=KEYSPACE).unwrap_or(0);

            let page = if Arbitrary::arbitrary(u).unwrap_or(false) {
                let mut base = if Arbitrary::arbitrary(u).unwrap_or(false) {
                    vec![1, 2, 3, 4, 5, 6, 7]
                } else {
                    vec![]
                };
                let mut additional = Arbitrary::arbitrary(u).unwrap_or(vec![]);
                base.append(&mut additional);
                Some(base)
            } else {
                None
            };

            batch.insert(pid, page);
        }

        Ok(WriteBatch(batch))
    }
}

#[derive(Debug, Arbitrary)]
enum Op {
    WriteBatch(WriteBatch),
    Gc,
    Restart,
}

fuzz_target!(|args: (Config, Vec<Op>)| {
    let (config, ops) = args;

    let mut marble = config.0.clone().open().unwrap();
    let mut model = std::collections::BTreeMap::new();

    for op in ops {
        match op {
            Op::WriteBatch(write_batch) => {
                for (k, v) in &write_batch.0 {
                    model.insert(*k, v.clone());
                }
                marble.write_batch(write_batch.0).unwrap()
            }
            Op::Gc => {
                marble.maintenance().unwrap();
            }
            Op::Restart => {
                drop(marble);
                marble = config.0.clone().open().unwrap();
            }
        };

        for (pid, expected) in &model {
            let expected_ref: Option<&[u8]> = expected.as_deref();
            let actual: Option<Vec<u8>> = marble.read(*pid).unwrap();
            let actual_ref: Option<&[u8]> = actual.as_deref();
            assert_eq!(expected_ref, actual_ref);
        }
    }

    drop(marble);

    std::fs::remove_dir_all(config.0.path).unwrap();
});
