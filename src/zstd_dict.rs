use std::fmt;

use crate::{uninit_boxed_slice, Map, ObjectId};

fn zstd_error(errno: usize) -> std::io::Error {
    let name = zstd_safe::get_error_name(errno);
    std::io::Error::new(std::io::ErrorKind::Other, name.to_string())
}

#[derive(Clone, Default)]
pub(crate) struct ZstdDict(pub(crate) Option<Vec<u8>>);

impl fmt::Debug for ZstdDict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZstdDict")
            .field("size", &self.0.as_ref().unwrap_or(&vec![]).len())
            .finish()
    }
}

impl ZstdDict {
    pub(crate) fn from_samples<B: AsRef<[u8]>>(samples: &Map<ObjectId, Option<B>>) -> ZstdDict {
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

        let mut buffer = Vec::with_capacity(total_size);

        for value in samples_iter() {
            buffer.extend_from_slice(value.as_ref());
        }

        assert_eq!(sizes.iter().sum::<usize>(), buffer.len());

        if sizes.len() < 8 || buffer.len() / sizes.len() <= 8 {
            // don't bother with dictionaries if there are less than 8 non-empty samples
            // don't bother with dictionaries if the average size is <= 8
            return ZstdDict(None);
        }

        // set max_size to somewhere in the range [4k, 128k)
        let max_size = (total_size / 100).min(128 * 1024).max(4096);

        let mut dict_buf = Vec::with_capacity(max_size);

        match zstd_safe::train_from_buffer(&mut dict_buf, &buffer, &sizes) {
            Ok(len) => {
                assert_eq!(len, dict_buf.len());
                dict_buf.shrink_to_fit();
                ZstdDict(Some(dict_buf))
            }
            Err(e) => {
                log::trace!(
                    "failed to create dictionary for write batch: {}",
                    zstd_error(e)
                );
                ZstdDict(None)
            }
        }
    }

    pub(crate) fn decompress(&self, buf: Box<[u8]>) -> Box<[u8]> {
        if let Some(dict_buffer) = &self.0 {
            let exact_size = usize::try_from(zstd_safe::find_decompressed_size(&buf)).unwrap();
            let mut out = uninit_boxed_slice(exact_size);
            let mut cx = zstd_safe::DCtx::create();
            cx.decompress_using_dict(&mut *out, &buf, &dict_buffer)
                .map_err(zstd_error)
                .unwrap();
            out
        } else {
            buf
        }
    }

    pub(crate) fn compress(&self, buf: &[u8], level: i32) -> Vec<u8> {
        if let Some(dict_buffer) = &self.0 {
            let mut out = Vec::with_capacity(zstd_safe::compress_bound(buf.len()));
            let mut cx = zstd_safe::CCtx::create();

            cx.compress_using_dict(&mut out, buf, &dict_buffer, level)
                .map_err(zstd_error)
                .unwrap();
            out
        } else {
            buf.to_vec()
        }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        if let Some(dict_buffer) = &self.0 {
            &dict_buffer
        } else {
            &[]
        }
    }
}
