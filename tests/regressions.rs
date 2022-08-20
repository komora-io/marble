use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use marble::*;

mod common;

const TEST_DIR: &str = "testing_data_directories";

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn with_instance<F: FnOnce(&Config, Marble)>(config: Config, f: F) {
    let _ = common::setup_logger();

    let _ = std::fs::remove_dir_all(&config.path);

    let marble = config.open().unwrap();

    f(&config, marble);

    std::fs::remove_dir_all(config.path).unwrap();
}

fn with_default_instance<F: FnOnce(&Config, Marble)>(f: F) {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));
    let path = std::path::Path::new(TEST_DIR).join(subdir);

    let config = Config {
        path,
        ..Default::default()
    };

    with_instance(config, f)
}

fn restart(config: &Config, marble: Marble) -> Marble {
    drop(marble);
    config.open().unwrap()
}

#[test]
fn test_00() {
    with_default_instance(|config, mut marble| {
        let object_id = 1;
        marble
            .write_batch([(object_id, Some(vec![]))].into_iter())
            .unwrap();
        assert!(marble.read(object_id).unwrap().is_some());
        marble = restart(config, marble);
        assert!(marble.read(object_id).unwrap().is_some());
    });
}

#[test]
fn test_01() {
    with_default_instance(|config, mut marble| {
        let object_id_1 = 1;
        marble
            .write_batch([(object_id_1, Some(vec![]))].into_iter())
            .unwrap();
        let object_id_2 = 2;
        marble
            .write_batch([(object_id_2, Some(vec![]))].into_iter())
            .unwrap();
        assert!(marble.read(object_id_1).unwrap().is_some());
        assert!(marble.read(object_id_2).unwrap().is_some());
        marble = restart(config, marble);
        assert!(marble.read(object_id_1).unwrap().is_some());
        assert!(marble.read(object_id_2).unwrap().is_some());
    });
}

#[test]
fn test_02() {
    with_default_instance(|_config, marble| {
        let object_id_1 = 1;
        marble
            .write_batch([(object_id_1, Some(vec![]))].into_iter())
            .unwrap();
        let object_id_2 = 2;
        marble
            .write_batch([(object_id_2, Some(vec![]))].into_iter())
            .unwrap();
        assert!(marble.read(object_id_1).unwrap().is_some());
        assert!(marble.read(object_id_2).unwrap().is_some());
        marble.maintenance().unwrap();
        assert!(marble.read(object_id_1).unwrap().is_some());
        assert!(marble.read(object_id_2).unwrap().is_some());
    });
}

#[test]
fn test_03() {
    with_default_instance(|_config, marble| {
        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();
    });
}

#[test]
fn test_04() {
    with_default_instance(|_config, marble| {
        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();
    });
}

#[test]
fn test_05() {
    with_default_instance(|config, marble| {
        let object_id_1 = 1;
        marble
            .write_batch::<Vec<u8>, _>([(object_id_1, None)].into_iter())
            .unwrap();

        restart(config, marble);
    });
}

#[test]
fn test_06() {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));

    let config = Config {
        target_file_size: 1,
        max_object_size: 17179869184,
        fsync_each_batch: false,
        min_compaction_files: 2,
        path: std::path::Path::new(TEST_DIR).join(subdir),
        ..Default::default()
    };

    with_instance(config, |config, mut marble| {
        marble
            .write_batch([(1, Some([170, 170, 170].to_vec()))])
            .unwrap();
        marble.write_batch([(2, Some([170].to_vec()))]).unwrap();
        marble
            .write_batch([(3, Some([170, 170, 170, 170, 170].to_vec()))])
            .unwrap();

        marble = restart(config, marble);

        marble.maintenance().unwrap();

        assert_eq!(marble.read(1).unwrap().unwrap(), vec![170, 170, 170]);
        assert_eq!(marble.read(2).unwrap().unwrap(), vec![170]);
        assert_eq!(
            marble.read(3).unwrap().unwrap(),
            vec![170, 170, 170, 170, 170]
        );
    });
}

#[test]
fn test_07() {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));

    let config = Config {
        target_file_size: 6400,
        file_compaction_percent: 55,
        fsync_each_batch: true,
        min_compaction_files: 2,
        path: std::path::Path::new(TEST_DIR).join(subdir),
        ..Default::default()
    };

    with_instance(config, |_config, marble| {
        marble
            .write_batch([(1, Some(vec![])), (2, None), (3, None)])
            .unwrap();

        //marble = restart(config, marble);

        marble
            .write_batch([(1, None), (3, Some(vec![170; 9]))])
            .unwrap();

        let v: Option<Vec<u8>> = None;
        marble.write_batch([(1, v)]).unwrap();

        marble.maintenance().unwrap();

        //restart(config, marble);
    });
}

#[test]
fn test_08() {
    with_default_instance(|_config, marble| {
        marble
            .write_batch::<Vec<u8>, _>(
                [(1, Some(vec![])), (2, Some(vec![])), (3, Some(vec![]))].into_iter(),
            )
            .unwrap();
        marble
            .write_batch::<Vec<u8>, _>([(1, Some(vec![])), (2, Some(vec![]))].into_iter())
            .unwrap();
        marble
            .write_batch::<Vec<u8>, _>([(1, Some(vec![]))].into_iter())
            .unwrap();

        marble.maintenance().unwrap();
    });
}
