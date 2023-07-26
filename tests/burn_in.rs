use rand::{thread_rng, Rng};

use marble::{recover, Marble};

mod common;

const KEYSPACE: u64 = 16;
const BATCH_SZ: usize = 3;
const VALUE_LEN: usize = 7;
const OPS: usize = 16;
const BATCHES: usize = OPS / BATCH_SZ;

fn run(marble: Marble) {
    let v = vec![0xFA; VALUE_LEN];

    let mut rng = thread_rng();

    for i in 0..BATCHES {
        let mut batch = std::collections::HashMap::new();

        for _ in 0..BATCH_SZ {
            let pid = rng.gen_range(0..KEYSPACE);
            let bytes = i.to_le_bytes();
            let user_data = marble::InlineArray::from(&bytes);
            batch.insert(pid, Some((user_data, v.clone())));
        }

        marble.write_batch(batch).unwrap();

        if i % 16 == 0 {
            let cleaned_up = marble.maintenance().unwrap();
            if cleaned_up != 0 {
                log::info!("defragmented {} objects", cleaned_up);
            }
        }
    }
}

#[test]
fn burn_in() {
    common::setup_logger();

    let concurrency: usize = std::thread::available_parallelism().unwrap().get() * 10;

    let marble = recover("testing_data_directories/burn_in").unwrap().0;

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for i in 0..concurrency {
        let marble = marble.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("thread-{i}"))
                .spawn(move || {
                    run(marble);
                })
                .unwrap(),
        )
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let total_ops = concurrency * BATCH_SZ * BATCHES;
    let bytes_written = total_ops * VALUE_LEN;
    let fault_injection_points =
        u64::MAX - fault_injection::FAULT_INJECT_COUNTER.load(std::sync::atomic::Ordering::Acquire);
    let elapsed = before.elapsed();
    let writes_per_second = (total_ops as u128 * 1000) / elapsed.as_millis();
    let bytes_per_second = (bytes_written as u128 / 1000) / elapsed.as_millis();

    log::info!(
        "wrote {} mb in {:?} with {} threads ({} writes per second, {} mb per second), {} fault \
         injection points",
        bytes_written / 1_000_000,
        elapsed,
        concurrency,
        writes_per_second,
        bytes_per_second,
        fault_injection_points,
    );

    let _ = std::fs::remove_dir_all("testing_data_directories/burn_in");
}
