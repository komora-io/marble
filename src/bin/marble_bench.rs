use std::sync::Arc;

use marble::{PageId, Marble};

fn run(marble: Arc<Marble>) {
    const MAX: u64 = 100 * 1024; // * 1024;
    const KEYSPACE: u64 = 1024 * 1024;
    const BATCH_SZ: u64 = 10 * 1024;

    for i in 0..(MAX / BATCH_SZ) {
        let mut batch = std::collections::HashMap::default();

        let base = i * BATCH_SZ;
        for j in 1..=BATCH_SZ {
            let pid = PageId(((base + j) % KEYSPACE).try_into().unwrap());
            batch.insert(pid, Some(vec![0; 1024]));
        }

        marble.write_batch(batch).unwrap();

        if i % (KEYSPACE / BATCH_SZ) == 0 {
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
}
