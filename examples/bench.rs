use std::sync::Arc;

use marble::{PageId, Marble};

fn rdtsc () -> u64 {
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
}

fn run(marble: Arc<Marble>) {
    const MUL: u64 = 1;
    const MAX: u64 = 1024 * 1024;
    const KEYSPACE: u64 = 64 * 1024;
    const BATCH_SZ: u64 = 1024;

    for i in 0..(MAX / BATCH_SZ) {
        let mut batch = std::collections::HashMap::default();

        for _ in 1..=BATCH_SZ {
            let pid = PageId::new(((rdtsc() * MUL) % KEYSPACE).max(1)).unwrap();
            batch.insert(pid, Some(vec![0; 4 * 1024]));
        }

        marble.write_batch(batch).unwrap();

        if i % 16 == 0 {
            marble.maintenance().unwrap();
        }
    }
}

fn main() {
    const CONCURRENCY: usize = 16;

    let marble = Arc::new(Marble::open("bench_data").unwrap());

    let mut threads = vec![];

    for _ in 0..CONCURRENCY {
        let marble = marble.clone();
        threads.push(std::thread::spawn(move || {
            run(marble);
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    dbg!(u64::MAX - marble::FAULT_INJECT_COUNTER.load(std::sync::atomic::Ordering::Acquire));
}
