use std::fmt;
use std::sync::RwLock;
use std::thread::ThreadId;

use zstd_safe::DCtx;

use crate::{uninit_boxed_slice, Map, ObjectId};

pub(crate) fn zstd_error(errno: usize) -> std::io::Error {
    let name = zstd_safe::get_error_name(errno);
    std::io::Error::new(std::io::ErrorKind::Other, name.to_string())
}

#[derive(Default)]
pub(crate) struct ZstdDict {
    decompressor: Option<ThreadLocalDict>,
}

impl fmt::Debug for ZstdDict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdDict")
            .field("decompressor", &self.decompressor.is_some())
            .finish()
    }
}

impl ZstdDict {
    pub(crate) fn from_dict_bytes(dict_bytes: &[u8]) -> ZstdDict {
        let container_of_dicts = ThreadLocalDict {
            dict_bytes: dict_bytes.to_vec(),
            tls: Default::default(),
        };

        ZstdDict {
            decompressor: Some(container_of_dicts),
        }
    }

    pub(crate) fn decompress(&self, buf: Box<[u8]>) -> Box<[u8]> {
        if let Some(decompressor) = &self.decompressor {
            decompressor.decompress(&buf)
        } else {
            buf
        }
    }
}

struct DictInABox(*mut DCtx<'static>);

// this is not actually used by multiple threads concurrently
// because access is gated by ThreadId
unsafe impl Sync for DictInABox {}

// the underlying DCtx is Send
unsafe impl Send for DictInABox {}

impl DictInABox {
    fn decompress(&self, buf: &[u8]) -> Box<[u8]> {
        let exact_size = zstd_safe::find_decompressed_size(&buf).unwrap().unwrap();
        let mut out = uninit_boxed_slice(exact_size as usize);

        let dctx: &mut DCtx<'static> = unsafe { &mut *self.0 };

        dctx.decompress(&mut *out, &buf)
            .map_err(zstd_error)
            .unwrap();

        out
    }
}

pub(crate) struct ThreadLocalDict {
    dict_bytes: Vec<u8>,
    tls: RwLock<Map<ThreadId, DictInABox>>,
}

impl Drop for ThreadLocalDict {
    fn drop(&mut self) {
        let w_tls = self.tls.write().unwrap();

        for (_, diab) in &*w_tls {
            unsafe {
                let to_drop: Box<DCtx<'static>> = Box::from_raw(diab.0);
                drop(to_drop);
            }
        }
    }
}

impl ThreadLocalDict {
    fn decompress(&self, buf: &[u8]) -> Box<[u8]> {
        let tid = std::thread::current().id();
        let r_tls = self.tls.read().unwrap();

        if let Some(dict) = r_tls.get(&tid) {
            return dict.decompress(buf);
        }

        drop(r_tls);

        let mut dict = zstd_safe::DCtx::create();
        dict.load_dictionary(&self.dict_bytes)
            .map_err(zstd_error)
            .unwrap();

        let diab = DictInABox(Box::into_raw(Box::new(dict)));

        let ret = diab.decompress(buf);

        self.tls.write().unwrap().insert(tid, diab);

        ret
    }
}

pub(crate) fn from_samples<B: AsRef<[u8]>>(samples: &Map<ObjectId, Option<B>>) -> Option<Vec<u8>> {
    let mut sizes = vec![];
    let mut total_size = 0;

    let samples_iter = || {
        // filter-out deletions and empty objects before
        // constructing a zstd dictionary for this batch
        samples
            .values()
            .filter_map(|v_opt| v_opt.as_ref())
            .map(AsRef::as_ref)
            .filter(|v| !v.is_empty())
    };

    for value in samples_iter() {
        sizes.push(value.len());
        total_size += value.len();
    }

    if sizes.len() < 8 || total_size / sizes.len() <= 8 {
        // don't bother with dictionaries if there are less than 8 non-empty samples
        // don't bother with dictionaries if the average size is <= 8
        return None;
    }

    let mut buffer = Vec::with_capacity(total_size);

    for value in samples_iter() {
        buffer.extend_from_slice(value.as_ref());
    }

    assert_eq!(sizes.iter().sum::<usize>(), buffer.len());

    // set max_size to somewhere in the range [4k, 128k)
    let max_size = (total_size / 100).min(128 * 1024).max(4096);

    let mut dict_buf = Vec::with_capacity(max_size);

    match zstd_safe::train_from_buffer(&mut dict_buf, &buffer, &sizes) {
        Ok(len) => {
            assert_eq!(len, dict_buf.len());
            dict_buf.shrink_to_fit();
            Some(dict_buf)
        }
        Err(e) => {
            log::trace!(
                "failed to create dictionary for write batch: {}",
                zstd_error(e)
            );
            None
        }
    }
}
