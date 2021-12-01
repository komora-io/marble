//! PT LSM is a simple in-memory LSM
//! for managing fixed-size metadata
//! in more complex systems.

use std::collections::BTreeMap;
use std::fs;
use std::io::{prelude::*, BufReader, BufWriter, Result};
use std::path::{Path, PathBuf};

fn hash<const K: usize, const V: usize>(k: &[u8; K], v: &[u8; V]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&*k);
    hasher.update(&*v);
    hasher.finalize()
}

fn id_format(id: u64) -> String {
    format!("{:016x}", id)
}

fn sstables(path: &Path) -> Result<BTreeMap<u64, u64>> {
    let mut sstable_map = BTreeMap::new();

    for dir_entry_res in fs::read_dir(path.join("sstables"))? {
        let dir_entry = dir_entry_res?;
        if let Ok(id) = u64::from_str_radix(&dir_entry.file_name().into_string().unwrap(), 16) {
            let metadata = dir_entry.metadata()?;

            sstable_map.insert(id, metadata.len());
        }
    }

    Ok(sstable_map)
}

fn read_sstable<const K: usize, const V: usize>(
    path: &Path,
    id: u64,
) -> Result<BTreeMap<[u8; K], [u8; V]>> {
    let mut sstable = BTreeMap::new();

    let file = fs::OpenOptions::new()
        .read(true)
        .open(path.join("sstables").join(id_format(id)))?;

    let mut reader = BufReader::new(file);

    let mut buf = vec![0; 4 + K + V];

    let len_buf = &mut [0; 8];

    reader.read_exact(len_buf)?;

    let expected_len: u64 = u64::from_le_bytes(*len_buf);

    while let Ok(()) = reader.read_exact(&mut buf) {
        let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let k: [u8; K] = buf[4..K + 4].try_into().unwrap();
        let v: [u8; V] = buf[K + 4..4 + K + V].try_into().unwrap();
        let crc_actual: u32 = hash(&k, &v);

        assert_eq!(crc_expected, crc_actual);

        sstable.insert(k, v);
    }

    assert_eq!(sstable.len() as u64, expected_len);

    Ok(sstable)
}

pub struct Lsm<const K: usize, const V: usize> {
    log: BufWriter<fs::File>,
    memtable: BTreeMap<[u8; K], [u8; V]>,
    db: BTreeMap<[u8; K], [u8; V]>,
    next_sstable_id: u64,
    path: PathBuf,
}

impl<const K: usize, const V: usize> Lsm<K, V> {
    pub fn recover<P: AsRef<Path>>(p: P) -> Result<Lsm<K, V>> {
        let path = p.as_ref();
        if !path.exists() {
            fs::create_dir(path)?;
            fs::create_dir(path.join("sstables"))?;
        }

        let log = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.join("log"))?;

        let mut sstables: Vec<u64> = sstables(path)?.into_iter().map(|(id, _sz)| id).collect();

        let mut db = BTreeMap::new();
        for sstable_id in &sstables {
            let mut sstable = read_sstable::<K, V>(path, *sstable_id)?;
            db.append(&mut sstable);
        }

        let max_sstable_id = sstables.pop();

        let mut reader = BufReader::new(log);

        let mut buf = vec![0; 4 + K + V];

        while let Ok(()) = reader.read_exact(&mut buf) {
            let crc_expected: u32 = u32::from_le_bytes(buf[0..4].try_into().unwrap());
            let k: [u8; K] = buf[4..K + 4].try_into().unwrap();
            let v: [u8; V] = buf[K + 4..4 + K + V].try_into().unwrap();
            let crc_actual: u32 = hash(&k, &v);

            assert_eq!(crc_expected, crc_actual);

            db.insert(k, v);
        }

        Ok(Lsm {
            log: BufWriter::with_capacity(8 * 1024, reader.into_inner()),
            memtable: BTreeMap::new(),
            db,
            path: path.into(),
            next_sstable_id: max_sstable_id.unwrap_or(0) + 1,
        })
    }

    pub fn insert(&mut self, k: [u8; K], v: [u8; V]) -> Result<()> {
        let crc: u32 = hash(&k, &v);
        self.log.write(&crc.to_le_bytes())?;
        self.log.write(&k)?;
        self.log.write(&v)?;

        self.memtable.insert(k, v);

        Ok(())
    }

    pub fn get(&self, k: &[u8; K]) -> Option<&[u8; V]> {
        if let Some(v) = self.memtable.get(k) {
            return Some(v);
        }

        self.db.get(k)
    }

    pub fn range<T, R>(&self, range: R) -> impl Iterator<Item = (&[u8; K], &[u8; V])>
    where
        T: std::cmp::Ord + ?Sized,
        R: std::ops::RangeBounds<T>,
        [u8; K]: std::borrow::Borrow<T> + Ord,
    {
        self.db.range(range)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;

        if self.memtable.len() > 1000 {
            self.log.get_mut().sync_all()?;
            self.write_sstable(self.next_sstable_id, &self.memtable, false)?;
            self.next_sstable_id += 1;

            self.db.append(&mut self.memtable);

            self.log.get_mut().set_len(0)?;
            self.log.get_mut().sync_all()?;

            self.sstable_maintenance()
        } else {
            Ok(())
        }
    }

    fn sstable_maintenance(&mut self) -> Result<()> {
        let sstable_map = sstables(&self.path)?;

        if sstable_map.len() < 20 {
            return Ok(());
        }

        let sorted: Vec<_> = sstable_map.into_iter().collect();

        let mut window_sums = vec![];

        for window in sorted.windows(5) {
            let sum: u64 = window.iter().map(|(_, len)| len).sum();
            window_sums.push(sum);
        }

        let min_run_size = window_sums.iter().min().unwrap();

        let min_window_index = window_sums.iter().position(|s| s == min_run_size).unwrap();

        let run_to_compact: Vec<_> = sorted
            .windows(5)
            .nth(min_window_index)
            .unwrap()
            .into_iter()
            .map(|(id, _sum)| *id)
            .collect();

        self.compact_sstable_run(&run_to_compact)
    }

    fn write_sstable(
        &self,
        id: u64,
        items: &BTreeMap<[u8; K], [u8; V]>,
        tmp_mv: bool,
    ) -> Result<()> {
        let mut path = self.path.join("sstables");
        path = if tmp_mv {
            path.join(format!("{}-tmp", id))
        } else {
            path.join(id_format(id))
        };

        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;

        let mut bw = BufWriter::new(file);

        bw.write(&(items.len() as u64).to_le_bytes())?;

        for (k, v) in items {
            let crc: u32 = hash(k, v);
            bw.write(&crc.to_le_bytes())?;
            bw.write(k)?;
            bw.write(v)?;
        }

        bw.flush()?;

        bw.get_mut().sync_all()?;

        if tmp_mv {
            let new_path = self.path.join("sstables").join(id_format(id));
            fs::rename(path, new_path)?;
        }

        Ok(())
    }

    fn compact_sstable_run(&self, sstable_ids: &[u64]) -> Result<()> {
        let mut map = BTreeMap::new();

        for sstable_id in sstable_ids {
            let mut sstable = read_sstable::<K, V>(&self.path, *sstable_id)?;
            map.append(&mut sstable);
        }

        let replacement = sstable_ids.iter().max().unwrap();

        self.write_sstable(*replacement, &map, true)?;

        for sstable_id in sstable_ids {
            if sstable_id == replacement {
                continue;
            }
            fs::remove_file(self.path.join("sstables").join(id_format(*sstable_id)))?;
        }

        Ok(())
    }
}

#[test]
fn test() {
    let mut lsm = Lsm::recover("test").unwrap();

    let before = std::time::Instant::now();
    for i in 0_u64..10_000_000 {
        lsm.insert(i.to_le_bytes(), [0; 8]).unwrap();
        lsm.flush().unwrap();
    }
    dbg!(before.elapsed());
}
