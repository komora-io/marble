use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use marble::*;

mod common;

const TEST_DIR: &str = "testing_data_directories";

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn with_instance<F: FnOnce(&Config, Marble)>(config: Config, f: F) {
    let _ = common::setup_logger();

    let _ = std::fs::remove_dir_all(&config.path);

    let marble = config.recover().unwrap().0;

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
    config.recover().unwrap().0
}

fn empty_value() -> Option<(marble::InlineArray, Vec<u8>)> {
    Some(([].as_ref().into(), vec![]))
}

#[test]
fn test_00() {
    with_default_instance(|config, mut marble| {
        let object_id = 1;
        marble.write_batch([(object_id, empty_value())]).unwrap();
        assert!(marble.read(object_id).unwrap().is_some());
        marble = restart(config, marble);
        assert!(marble.read(object_id).unwrap().is_some());
    });
}

#[test]
fn test_01() {
    with_default_instance(|config, mut marble| {
        let object_id_1 = 1;
        marble.write_batch([(object_id_1, empty_value())]).unwrap();
        let object_id_2 = 2;
        marble.write_batch([(object_id_2, empty_value())]).unwrap();
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
        marble.write_batch([(object_id_1, empty_value())]).unwrap();
        let object_id_2 = 2;
        marble.write_batch([(object_id_2, empty_value())]).unwrap();
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
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();
    });
}

#[test]
fn test_04() {
    with_default_instance(|_config, marble| {
        let object_id_1 = 1;
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();

        marble.maintenance().unwrap();

        let object_id_1 = 1;
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();
    });
}

#[test]
fn test_05() {
    with_default_instance(|config, marble| {
        let object_id_1 = 1;
        marble.write_batch::<_>([(object_id_1, None)]).unwrap();

        restart(config, marble);
    });
}

#[test]
fn test_06() {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));

    let config = Config {
        path: std::path::Path::new(TEST_DIR).join(subdir),
        ..Default::default()
    };

    with_instance(config, |config, mut marble| {
        marble
            .write_batch([(1, Some(([].as_ref().into(), [170, 170, 170].to_vec())))])
            .unwrap();
        marble
            .write_batch([(2, Some(([].as_ref().into(), [170].to_vec())))])
            .unwrap();
        marble
            .write_batch([(
                3,
                Some(([].as_ref().into(), [170, 170, 170, 170, 170].to_vec())),
            )])
            .unwrap();

        marble = restart(config, marble);

        marble.maintenance().unwrap();

        assert_eq!(&*marble.read(1).unwrap().unwrap(), vec![170, 170, 170]);
        assert_eq!(&*marble.read(2).unwrap().unwrap(), vec![170]);
        assert_eq!(
            &*marble.read(3).unwrap().unwrap(),
            vec![170, 170, 170, 170, 170]
        );
    });
}

#[test]
fn test_07() {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));

    let config = Config {
        path: std::path::Path::new(TEST_DIR).join(subdir),
        ..Default::default()
    };

    with_instance(config, |_config, marble| {
        marble
            .write_batch([
                (1, Some(([].as_ref().into(), vec![]))),
                (2, None),
                (3, None),
            ])
            .unwrap();

        //marble = restart(config, marble);

        marble
            .write_batch([(1, None), (3, Some((vec![].into(), vec![170; 9])))])
            .unwrap();

        marble.write_batch([(1, None)]).unwrap();

        marble.maintenance().unwrap();

        //restart(config, marble);
    });
}

#[test]
fn test_08() {
    with_default_instance(|_config, marble| {
        marble
            .write_batch::<_>([(1, empty_value()), (2, empty_value()), (3, empty_value())])
            .unwrap();
        marble
            .write_batch::<_>([(1, empty_value()), (2, empty_value())])
            .unwrap();
        marble.write_batch::<_>([(1, empty_value())]).unwrap();

        marble.maintenance().unwrap();
    });
}

#[test]
fn test_09() {
    with_default_instance(|config, mut marble| {
        // high entropy, should be very low compression
        let big_value: Vec<u8> = (0..1024 * 1024).map(|_| rand::random::<u8>()).collect();
        marble
            .write_batch::<_>([
                (1_u64, Some((vec![].into(), big_value.clone()))),
                (2_u64, Some((vec![].into(), big_value.clone()))),
                (3_u64, Some((vec![].into(), big_value.clone()))),
                (4_u64, Some((vec![].into(), big_value.clone()))),
                (5_u64, Some((vec![].into(), big_value.clone()))),
                (6_u64, Some((vec![].into(), big_value.clone()))),
                (7_u64, Some((vec![].into(), big_value.clone()))),
                (8_u64, Some((vec![].into(), big_value.clone()))),
            ])
            .unwrap();

        assert_eq!(&*marble.read(1).unwrap().unwrap(), big_value);

        marble = restart(config, marble);

        assert_eq!(&*marble.read(1).unwrap().unwrap(), big_value);

        marble.maintenance().unwrap();
    });
}

#[test]
fn test_10() {
    with_default_instance(|config, mut marble| {
        // low entropy, should be very high compression
        let big_value = vec![0xFA; 1024 * 1024];
        marble
            .write_batch::<_>([
                (1_u64, Some((vec![].into(), big_value.clone()))),
                (2_u64, Some((vec![].into(), big_value.clone()))),
                (3_u64, Some((vec![].into(), big_value.clone()))),
                (4_u64, Some((vec![].into(), big_value.clone()))),
                (5_u64, Some((vec![].into(), big_value.clone()))),
                (6_u64, Some((vec![].into(), big_value.clone()))),
                (7_u64, Some((vec![].into(), big_value.clone()))),
                (8_u64, Some((vec![].into(), big_value.clone()))),
            ])
            .unwrap();

        assert_eq!(&*marble.read(1).unwrap().unwrap(), big_value);

        marble = restart(config, marble);

        assert_eq!(&*marble.read(1).unwrap().unwrap(), big_value);

        marble.maintenance().unwrap();
    });
}

#[test]
fn test_11() {
    with_default_instance(|_config, marble| {
        marble.write_batch::<_>([]).unwrap();

        marble
            .write_batch::<_>([
                (1_u64, empty_value()),
                (2_u64, empty_value()),
                (3_u64, empty_value()),
                (4_u64, None),
                (5_u64, Some((vec![].into(), vec![0]))),
                (6_u64, Some((vec![].into(), vec![252]))),
                (7_u64, None),
                (8_u64, empty_value()),
                (9_u64, Some((vec![].into(), vec![255, 255, 35, 255, 2, 14]))),
            ])
            .unwrap();
    });
}

#[test]
fn test_12() {
    with_default_instance(|_config, marble| {
        marble
            .write_batch::<_>([
                (14_u64, Some((vec![].into(), vec![65]))),
                (3_u64, Some((vec![].into(), vec![139_u8]))),
                (19_u64, Some((vec![].into(), vec![2]))),
                (25_u64, Some((vec![].into(), vec![255]))),
                (17_u64, Some((vec![].into(), vec![253]))),
                (60_u64, Some((vec![].into(), vec![255]))),
                (46_u64, Some((vec![].into(), vec![0, 0]))),
            ])
            .unwrap();
    });
}

#[test]
fn test_13() {
    let subdir = format!("test_{}", TEST_COUNTER.fetch_add(1, SeqCst));

    let config = Config {
        path: std::path::Path::new(TEST_DIR).join(subdir),
        ..Default::default()
    };

    with_instance(config, |config, mut marble| {
        marble
            .write_batch::<_>([(56_u64, None), (46, None)])
            .unwrap();

        marble
            .write_batch::<_>([
                (46, None),
                (55, None),
                (50, None),
                (60, Some((vec![].into(), vec![255_u8, 50, 86, 255]))),
            ])
            .unwrap();

        assert_eq!(&*marble.read(60).unwrap().unwrap(), vec![255, 50, 86, 255]);

        marble
            .write_batch::<_>([
                (60, Some((vec![].into(), vec![1_u8, 2, 3, 4, 5, 6, 7, 0]))),
                (37, None),
            ])
            .unwrap();

        assert_eq!(
            &*marble.read(60).unwrap().unwrap(),
            vec![1_u8, 2, 3, 4, 5, 6, 7, 0]
        );

        marble
            .write_batch::<_>([(37_u64, None), (0_u64, None)])
            .unwrap();

        assert_eq!(
            &*marble.read(60).unwrap().unwrap(),
            vec![1_u8, 2, 3, 4, 5, 6, 7, 0]
        );

        marble.maintenance().unwrap();

        assert_eq!(
            &*marble.read(60).unwrap().unwrap(),
            vec![1_u8, 2, 3, 4, 5, 6, 7, 0]
        );

        marble = restart(config, marble);

        assert_eq!(
            &*marble.read(60).unwrap().unwrap(),
            vec![1_u8, 2, 3, 4, 5, 6, 7, 0]
        );
    });
}
