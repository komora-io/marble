use std::sync::Arc;

use marble::{Marble, ObjectId};

const MUL: u64 = 1;
const KEYSPACE: u64 = 64 * 1024;
const BATCH_SZ: usize = 1024;
const VALUE_LEN: usize = 4096;
const OPS: usize = 2 * 1024;
const BATCHES: usize = OPS / BATCH_SZ;

fn advance_lfsr(lfsr: &mut u16) {
    let bit = ((*lfsr >> 0) ^ (*lfsr >> 2) ^ (*lfsr >> 3) ^ (*lfsr >> 5)) & 1;
    *lfsr = (*lfsr >> 1) | (bit << 15);
}

fn run(marble: Arc<Marble>) {
    let v = vec![0xFA; VALUE_LEN];

    let mut lfsr: u16 = 0xACE1u16;

    for i in 0..BATCHES {
        advance_lfsr(&mut lfsr);

        let mut batch = std::collections::HashMap::new();

        for _ in 1..=BATCH_SZ {
            let pid = ObjectId::new(((lfsr as u64 * MUL) % KEYSPACE).max(1));
            batch.insert(pid, Some(&v));
        }

        marble.write_batch(batch).unwrap();

        if i % 16 == 0 {
            marble.maintenance().unwrap();
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
