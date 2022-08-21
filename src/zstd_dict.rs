use std::fmt;

use crate::{Map, ObjectId};

fn zstd_error(errno: usize) -> std::io::Error {
    let name = zstd_safe::get_error_name(errno);
    std::io::Error::new(std::io::ErrorKind::Other, name.to_string())
}

#[derive(Clone)]
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

        for value_opt in samples.values().filter(|v_opt| v_opt.is_some()) {
            let value: &[u8] = value_opt.as_ref().unwrap().as_ref();

            sizes.push(value.len());
            total_size += value.len();
        }

        let mut buffer = Vec::with_capacity(total_size);
        for value_opt in samples.values().filter(|v_opt| v_opt.is_some()) {
            let value: &[u8] = value_opt.as_ref().unwrap().as_ref();
            buffer.extend_from_slice(value.as_ref());
        }

        assert_eq!(sizes.iter().sum::<usize>(), buffer.len());

        let max_size = 128 * 1024; //(total_size / 100).min(128 * 1024).max(50000);

        let mut dict_buf = vec![0; max_size];

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

    pub(crate) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
        if let Some(dict_buffer) = &self.0 {
            let exact_size = usize::try_from(zstd_safe::find_decompressed_size(buf)).unwrap();
            let mut out = Vec::with_capacity(exact_size);
            let mut cx = zstd_safe::DCtx::create();
            cx.decompress_using_dict(&mut out, buf, &dict_buffer)
                .map_err(zstd_error)
                .unwrap();
            out
        } else {
            buf.to_vec()
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
