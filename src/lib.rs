use std::{
    convert::TryFrom,
    fs::File,
    io::{self, Write},
    iter::IntoIterator,
    path::Path,
    sync::Arc,
};

#[derive(Clone)]
pub struct Db {
    fst: Arc<fst::Map<&'static [u8]>>,
    mmap: Arc<memmap::Mmap>,
    file: Arc<File>,
    dict: Vec<u8>,
    fst_offset: u64,
    values_offset: u64,
}

impl Db {
    /// Build a new
    pub fn create<'a, P>(
        path: P,
        iter: impl Iterator<Item = (&'a [u8], &'a [u8])>,
    ) -> io::Result<Db>
    where
        P: AsRef<Path>,
    {
        // 1. write dictionary
        // 2. write compressed values
        // 3. write fst index
        // 4. write 16 byte footer with offset of values, index
        // 5. return read-only `Db`

        const COMPRESSION_LEVEL: i32 = 6;

        let mut file = File::create(&path)?;

        let (keys, values): (Vec<&[u8]>, Vec<&[u8]>) = iter.unzip();
        let max_dict_size =
            (values.iter().map(|v| v.len()).sum::<usize>() / 20).min(112_640);

        let dict_bytes = zstd::dict::from_samples(&values, max_dict_size)?;

        file.write_all(&dict_bytes)?;

        let mut compressor =
            zstd::block::Compressor::with_dict(dict_bytes.clone());
        let mut lengths = vec![];
        let mut total_value_length = 0_u64;
        for value in &values {
            let compressed = compressor.compress(value, COMPRESSION_LEVEL)?;
            lengths.push(compressed.len());
            total_value_length += compressed.len() as u64;
            file.write_all(&compressed)?;
        }

        let mut fst_builder = fst::MapBuilder::new(file).unwrap();

        let mut value_offset = 0_u64;
        for (key, value_len) in keys.iter().zip(lengths.into_iter()) {
            fst_builder.insert(key, value_offset).unwrap();
            value_offset += value_len as u64;
        }

        // this also finishes the fst
        let mut file = fst_builder.into_inner().unwrap();

        let values_offset: u64 = dict_bytes.len() as u64;
        let fst_offset: u64 = values_offset + total_value_length;

        file.write_all(&values_offset.to_le_bytes())?;
        file.write_all(&fst_offset.to_le_bytes())?;
        file.sync_all()?;
        drop(file);

        let file = Arc::new(File::open(path)?);
        let decompressor =
            Arc::new(zstd::block::Decompressor::with_dict(dict_bytes));
        let mmap = Arc::new(unsafe { memmap::Mmap::map(&file).unwrap() });
        let mmap_slice: &'static [u8] = mmap.as_ref();
        let fst = Arc::new(fst::Map::new(mmap_slice).unwrap());

        Ok(Db {
            fst,
            mmap,
            file,
            values_offset,
            fst_offset,
            decompressor,
        })
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 1. get index from fst
        // 2. decompress if present
        use fst::{IntoStreamer, Streamer};
        let mut range = self.fst.range().ge(key).into_stream();

        let (first_k, idx) = if let Some((first_k, idx)) = range.next() {
            (first_k, idx as usize)
        } else {
            return Ok(None);
        };

        if first_k != key {
            return Ok(None);
        }

        let next_idx = if let Some((_next_k, next_idx)) = range.next() {
            next_idx as usize
        } else {
            self.fst_offset as usize
        };

        let decompressed = self
            .decompress(&self.mmap.as_ref()[idx..next_idx], 5 * 1024 * 1024)
            .unwrap()
            .to_vec();

        todo!()
    }
}
