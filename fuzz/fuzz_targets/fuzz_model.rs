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

const KEYSPACE: u8 = 32;
const BATCH_MIN_SZ: u8 = 0;
const BATCH_MAX_SZ: u8 = 16;
const VALUE_MAX_SZ: u8 = 16;
const OPS: usize = 6;

type ObjectId = u64;

#[derive(Debug)]
struct Config(MarbleConfig);

impl<'a> Arbitrary<'a> for Config {
    fn arbitrary(_: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let path = std::path::Path::new(TEST_DIR)
            .join("fuzz")
            .join(uuid::Uuid::new_v4().to_string())
            .into();

        Ok(Config(MarbleConfig {
            path,
            ..Default::default()
        }))
    }
}

#[derive(Debug)]
struct WriteBatch(HashMap<ObjectId, Option<(marble::InlineArray, Vec<u8>)>>);

impl<'a> Arbitrary<'a> for WriteBatch {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let pages: u8 = u
            .int_in_range(BATCH_MIN_SZ..=BATCH_MAX_SZ)
            .unwrap_or(BATCH_MIN_SZ);

        let mut batch = HashMap::default();
        for pid_default in 0..pages {
            let pid: u8 = u.int_in_range(0..=KEYSPACE).unwrap_or(pid_default);

            let page = if Arbitrary::arbitrary(u).unwrap_or(true) {
                let len: u8 = u.int_in_range(0..=VALUE_MAX_SZ).unwrap_or(0);
                let user_data = u64::from(pid).to_le_bytes().as_ref().into();
                let value: Vec<u8> = u
                    .bytes(usize::from(len))
                    .unwrap_or(&[1, 2, 3, 4, 5, 6, 7, 8])
                    .into();
                Some((user_data, value))
            } else {
                None
            };

            batch.insert(u64::from(pid), page);
        }

        Ok(WriteBatch(batch))
    }
}

#[derive(Debug)]
enum Op {
    WriteBatch(WriteBatch),
    Gc,
    Restart,
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let choice: u8 = u.int_in_range(0..=2).unwrap_or(0);
        Ok(match choice {
            0 => Op::Gc,
            1 => Op::WriteBatch(
                WriteBatch::arbitrary(u).expect("WriteBatch::arbitrary should never fail"),
            ),
            2 => Op::Restart,
            _ => unreachable!(),
        })
    }
}

fuzz_target!(|args: (Config, [Op; OPS])| {
    let (config, ops) = args;

    let (mut marble, mut recovered_data) = config.0.clone().recover().unwrap();
    for (id, ud) in &recovered_data {
        assert_eq!(id.to_le_bytes().as_ref(), ud.as_ref());
    }
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
                (marble, recovered_data) = config.0.clone().recover().unwrap();
                for (id, ud) in &recovered_data {
                    assert_eq!(id.to_le_bytes().as_ref(), ud.as_ref());
                }
            }
        };

        for (pid, expected) in &model {
            let expected_ref: Option<&[u8]> = if let Some((_ud, d)) = expected {
                Some(d)
            } else {
                None
            };
            let actual: Option<Vec<u8>> = marble.read(*pid).unwrap();
            let actual_ref: Option<&[u8]> = actual.as_deref();
            assert_eq!(expected_ref, actual_ref);
        }
    }

    drop(marble);

    std::fs::remove_dir_all(&config.0.path).unwrap();
});
