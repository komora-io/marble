use std::sync::Arc;

use marble::{PageId, Marble};

fn rdtsc () -> u64 {
    unsafe {
        core::arch::x86_64::_rdtsc()
    }
}

fn run(marble: Arc<Marble>) {
    const MAX: u64 = 100 * 1024 * 1024;
    const KEYSPACE: u64 = 1024;
    const BATCH_SZ: u64 = 127;

    for i in 0..(MAX / BATCH_SZ) {
        let mut batch = std::collections::HashMap::default();

        for _ in 1..=BATCH_SZ {
            let pid = PageId((rdtsc() % KEYSPACE).max(1).try_into().unwrap());
            batch.insert(pid, Some(vec![0; 1024]));
        }

        marble.write_batch(batch).unwrap();

        if i % 100 == 0 {
            marble.maintenance().unwrap();
        }
    }
}

fn main() {
    const CONCURRENCY: usize = 1;

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
}
