use std::sync::Arc;

use rand::{thread_rng, Rng};

use marble::Marble;

const KEYSPACE: u64 = 1024 * 1024;
const BATCH_SZ: usize = 64 * 1024;
const VALUE_LEN: usize = 64;
const OPS_PER_THREAD: usize = 64 * 1024 * 1024;
const BATCHES_PER_THREAD: usize = OPS_PER_THREAD / BATCH_SZ;

fn run(marble: Arc<Marble>) {
    let v = vec![0xFA; VALUE_LEN];

    let mut rng = thread_rng();

    for i in 0..BATCHES_PER_THREAD {
        let mut batch = std::collections::HashMap::new();

        for _ in 0..BATCH_SZ {
            let pid = rng.gen_range(0..KEYSPACE);
            batch.insert(pid, Some(&v));
        }

        println!("starting write batch");
        marble.write_batch(batch).unwrap();
        println!("wrote batch");
    }
}

fn main() {
    env_logger::init();

    let concurrency: usize = std::thread::available_parallelism().unwrap().get();

    let config = marble::Config {
        path: "bench_data".into(),
        fsync_each_batch: false,
        ..Default::default()
    };

    let marble = Arc::new(config.open().unwrap());

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

    while threads.iter().any(|t| !t.is_finished()) {
        let cleaned_up = marble.maintenance().unwrap();
        if cleaned_up != 0 {
            println!("defragmented {} objects", cleaned_up);
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let cleaned_up = marble.maintenance().unwrap();
    if cleaned_up != 0 {
        println!("defragmented {} objects", cleaned_up);
    }

    let total_ops = concurrency * BATCH_SZ * BATCHES_PER_THREAD;
    let bytes_written = total_ops * VALUE_LEN;
    let fault_injection_points =
        u64::MAX - fault_injection::FAULT_INJECT_COUNTER.load(std::sync::atomic::Ordering::Acquire);
    let elapsed = before.elapsed();
    let writes_per_second = (total_ops as u128 * 1000) / elapsed.as_millis();
    let megabytes_per_second = (bytes_written as u128 / 1000) / elapsed.as_millis();
    let megabytes_written = bytes_written / 1_000_000;
    let kb_per_value = VALUE_LEN / 1_000;

    println!(
        "wrote {megabytes_written} mb in {elapsed:?} with {concurrency} threads ({writes_per_second} objects per second \
        of size {kb_per_value} kb each, {megabytes_per_second} mb per second), {fault_injection_points} fault injection points",
    )
}
