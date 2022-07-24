use std::sync::Arc;

use rand::{thread_rng, Rng};

use marble::Marble;

const KEYSPACE: u64 = 64 * 1024;
const BATCH_SZ: usize = 1024;
const VALUE_LEN: usize = 4096;
const OPS: usize = 24 * 1024 * 1024;
const BATCHES: usize = OPS / BATCH_SZ;

fn run(marble: Arc<Marble>) {
    let v = vec![0xFA; VALUE_LEN];

    let mut rng = thread_rng();

    for i in 0..BATCHES {
        let mut batch = std::collections::HashMap::new();

        for _ in 0..BATCH_SZ {
            let pid = rng.gen_range(0..KEYSPACE);
            batch.insert(pid, Some(&v));
        }

        marble.write_batch(batch).unwrap();

        if i % 16 == 0 {
            let cleaned_up = marble.maintenance().unwrap();
            if cleaned_up != 0 {
                println!("cleaned up {} files", cleaned_up);
            }
        }
    }
}

fn main() {
    let concurrency: usize = std::thread::available_parallelism().unwrap().get();

    let marble = Arc::new(Marble::open("bench_data").unwrap());

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for _ in 0..concurrency {
        let marble = marble.clone();
        threads.push(std::thread::spawn(move || {
            run(marble);
        }));
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

    println!(
        "wrote {} mb in {:?} with {} threads ({} writes per second, {} mb per second), {} fault injection points",
        bytes_written / 1_000_000,
        elapsed,
        concurrency,
        writes_per_second,
        bytes_per_second,
        fault_injection_points,
    )
}
