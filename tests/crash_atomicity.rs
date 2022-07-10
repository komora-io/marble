use std::env::{self, VarError};
use std::iter::once;
use std::process::{exit, Child, Command, ExitStatus};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::Rng;

use marble::{Config, Marble};

// test names, also used as dir names
const BATCHES_DIR: &str = "crash_batches";

const TESTS: &[(&str, fn())] = &[(BATCHES_DIR, crash_batches)];

const TEST_ENV_VAR: &str = "SLED_CRASH_TEST";
const N_TESTS: usize = 64;
const BATCH_SIZE: u32 = 13;
const CRASH_CHANCE: u32 = 250;

fn handle_child_wait_err(dir: &str, e: std::io::Error) {
    let _ = std::fs::remove_dir_all(dir);

    panic!("error waiting for {} test child: {}", dir, e);
}

fn handle_child_exit_status(dir: &str, status: ExitStatus) {
    let code = status.code();

    if code.is_none() || code.unwrap() != 9 {
        let _ = std::fs::remove_dir_all(dir);
        panic!("{} test child exited abnormally", dir);
    }
}

fn spawn_killah() {
    thread::spawn(|| {
        let runtime = rand::thread_rng().gen_range(0..600);
        thread::sleep(Duration::from_millis(runtime));
        exit(9);
    });
}

fn run_child_process(test_name: &str) -> Child {
    let bin = env::current_exe().expect("could not get test binary path");

    env::set_var(TEST_ENV_VAR, test_name);

    Command::new(bin)
        .env(TEST_ENV_VAR, test_name)
        .env("SLED_CRASH_CHANCE", CRASH_CHANCE.to_string())
        .spawn()
        .unwrap_or_else(|_| panic!("could not spawn child process for {} test", test_name))
}

fn crash_batches() {
    let dir = BATCHES_DIR;
    let _ = std::fs::remove_dir_all(dir);

    for _ in 0..N_TESTS {
        let mut child = run_child_process(dir);

        child
            .wait()
            .map(|status| handle_child_exit_status(dir, status))
            .map_err(|e| handle_child_wait_err(dir, e))
            .unwrap();
    }

    let _ = std::fs::remove_dir_all(dir);
}

fn run_crash_batches() {
    let crash_during_initialization = rand::thread_rng().gen_ratio(1, 10);

    if crash_during_initialization {
        spawn_killah();
    }

    let config = Config {
        path: BATCHES_DIR.into(),
        ..Default::default()
    };

    let m = Arc::new(config.open().unwrap());

    verify_batches(&m);

    let concurrency = 4;
    let mut threads = vec![];
    for i in 0..concurrency {
        let m = m.clone();
        let thread = thread::spawn(move || write_batches_inner(1000 * i as u32, m));
        threads.push(thread);
    }

    if !crash_during_initialization {
        spawn_killah();
    }

    for thread in threads.into_iter() {
        if let Err(e) = thread.join() {
            println!("worker thread failed: {:?}", e);
            std::process::exit(15);
        }
    }
}

fn write_batches_inner(start: u32, m: Arc<Marble>) {
    for i in start.. {
        let mut rng = rand::thread_rng();
        let value = if rng.gen_bool(0.1) {
            None
        } else {
            Some(i.to_le_bytes().to_vec())
        };

        let mut batch = vec![];
        for key in (0..BATCH_SIZE as u64).chain(once(u64::MAX)) {
            batch.push((key, value.clone()));
        }
        m.write_batch(batch).unwrap();
    }
}

/// Verifies that the keys in the tree are correctly recovered (i.e., equal).
/// Panics if they don't match up.
fn verify_batches(m: &Marble) {
    let values: Vec<Option<Vec<u8>>> = (0..BATCH_SIZE as u64)
        .chain(once(u64::MAX))
        .map(|i| {
            let object_id = i;
            m.read(object_id).unwrap()
        })
        .collect();
    let equal = values.windows(2).all(|w| w[0] == w[1]);
    println!("values: {:?}", values);

    assert!(equal, "values not equal: {:?}", values);
}

fn main() {
    match env::var(TEST_ENV_VAR) {
        Err(VarError::NotPresent) => {
            let filtered: Vec<(&'static str, fn())> = if let Some(filter) = std::env::args().nth(1)
            {
                TESTS
                    .iter()
                    .filter(|(name, _)| name.contains(&filter))
                    .cloned()
                    .collect()
            } else {
                TESTS.to_vec()
            };

            let filtered_len = filtered.len();

            println!();
            println!(
                "running {} test{}",
                filtered.len(),
                if filtered.len() == 1 { "" } else { "s" },
            );

            let mut tests = vec![];
            for (test_name, test_fn) in filtered.into_iter() {
                let test = thread::spawn(move || {
                    let res = std::panic::catch_unwind(test_fn);
                    println!(
                        "test {} ... {}",
                        test_name,
                        if res.is_ok() { "ok" } else { "panicked" }
                    );
                    res.unwrap();
                });
                tests.push(test);
            }

            for test in tests.into_iter() {
                test.join().unwrap();
            }

            println!();
            println!(
                "test result: ok. {} passed; {} filtered out",
                filtered_len,
                TESTS.len() - filtered_len,
            );
            println!();
        }

        Ok(ref s) if s == BATCHES_DIR => run_crash_batches(),
        Ok(_) | Err(_) => panic!("invalid crash test case"),
    }
}
