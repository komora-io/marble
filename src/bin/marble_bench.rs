use marble::{PageId, Marble};

fn main() {
    let marble = Marble::open("bench").unwrap();

    const MAX: u64 = 100 * 1024 * 1024;
    const KEYSPACE: u64 = 1024 * 1024;
    const BATCH_SZ: u64 = 10 * 1024;

    for i in 0..(MAX / BATCH_SZ) {
        let mut batch = std::collections::HashMap::default();

        let base = i * BATCH_SZ;
        for j in 0..BATCH_SZ {
            let pid = PageId((base + j) % KEYSPACE);
            batch.insert(pid, vec![0; 1024]);
        }

        marble.write_batch(batch).unwrap();

        if i % (KEYSPACE / BATCH_SZ) == 0 {
            marble.maintenance().unwrap();
        }
    }
}
