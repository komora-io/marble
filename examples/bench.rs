use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use rand::{thread_rng, Rng, RngCore};

use marble::Marble;

const KEYSPACE: u64 = 1024 * 1024;
const BATCH_SZ: usize = 64 * 1024;
const VALUE_LEN: usize = 64;
const OPS_PER_THREAD: usize = 2 * 1024 * 1024;
const BATCHES_PER_THREAD: usize = OPS_PER_THREAD / BATCH_SZ;

static ANY_WRITERS_DONE: AtomicBool = AtomicBool::new(false);

fn run_writer(marble: Arc<Marble>) {
    let mut rng = thread_rng();

    for _ in 0..BATCHES_PER_THREAD {
        let mut batch = std::collections::HashMap::new();

        for _ in 0..BATCH_SZ {
            let pid = rng.gen_range(0..KEYSPACE);
            let v: Option<Vec<u8>> = if rng.gen_bool(0.1) {
                None
            } else {
                let len = rng.gen_range(0..VALUE_LEN * 2);
                let mut v = Vec::with_capacity(len);
                unsafe {
                    v.set_len(len);
                }
                rng.fill_bytes(&mut v);
                Some(v)
            };
            batch.insert(pid, v);
        }

        marble.write_batch(batch).unwrap();
    }

    ANY_WRITERS_DONE.store(true, Ordering::Release);
}

fn run_reader(marble: Arc<Marble>) {
    let mut rng = thread_rng();

    let mut worst_time = Duration::default();

    while !ANY_WRITERS_DONE.load(Ordering::Relaxed) {
        let before = Instant::now();
        let pid = rng.gen_range(0..KEYSPACE);
        marble.read(pid).unwrap();
        worst_time = worst_time.max(before.elapsed());
    }

    println!("worst read latency: {worst_time:?}");
}

fn main() {
    env_logger::init();

    let concurrency: usize = std::thread::available_parallelism().unwrap().get();

    let config = marble::Config {
        path: "bench_data".into(),
        fsync_each_batch: false,
        ..Default::default()
    };

    println!("beginning recovery");
    let marble = Arc::new(config.open().unwrap());
    println!("marble recovered {:?}", marble.file_stats());
    marble.maintenance().unwrap();
    println!("post initial maintenance: {:?}", marble.file_stats());

    let mut threads = vec![];

    let before = std::time::Instant::now();

    for i in 0..concurrency {
        {
            let marble = marble.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("writer-{i}"))
                    .spawn(move || {
                        run_writer(marble);
                    })
                    .unwrap(),
            );
        }

        let marble = marble.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("reader-{i}"))
                .spawn(move || {
                    run_reader(marble);
                })
                .unwrap(),
        );
    }

    while threads.iter().any(|t| !t.is_finished()) {
        let cleaned_up = marble.maintenance().unwrap();
        if cleaned_up != 0 {
            let stats = marble.file_stats();
            println!("defragmented {cleaned_up} objects. stats: {stats:?}",);
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }

    let cleaned_up = marble.maintenance().unwrap();
    if cleaned_up != 0 {
        let stats = marble.file_stats();
        println!("defragmented {cleaned_up} objects. stats: {stats:?}",);
    }
    let cleaned_up_2 = marble.maintenance().unwrap();
    assert_eq!(cleaned_up_2, 0);

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
        "wrote {megabytes_written} mb in {elapsed:?} with {concurrency} threads \
         ({writes_per_second} objects per second of size {kb_per_value} kb each, \
         {megabytes_per_second} mb per second), {fault_injection_points} fault injection points",
    )
}
