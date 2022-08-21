use std::fmt;

use crate::{Map, ObjectId};

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

        let max_size = (total_size / 100).min(128 * 1024).max(512);

        if let Ok(dict_buf) = zstd::dict::from_continuous(&buffer, &sizes, max_size) {
            ZstdDict(Some(dict_buf))
        } else {
            ZstdDict(None)
        }
    }

    pub(crate) fn decompress(&self, buf: &[u8]) -> Vec<u8> {
        if let Some(dict_buffer) = &self.0 {
            let mut out = vec![];
            let mut cx = zstd_safe::DCtx::create();
            cx.decompress_using_dict(&mut out, buf, &dict_buffer)
                .unwrap();
            out
        } else {
            buf.to_vec()
        }
    }

    pub(crate) fn compress(&self, buf: &[u8], level: i32) -> Vec<u8> {
        if let Some(dict_buffer) = &self.0 {
            let mut out = Vec::with_capacity(buf.len().max(128));
            let mut cx = zstd_safe::CCtx::create();

            fn map_error_code(code: usize) -> std::io::Error {
                let msg = zstd_safe::get_error_name(code);
                std::io::Error::new(std::io::ErrorKind::Other, msg.to_string())
            }

            cx.compress_using_dict(&mut out, buf, &dict_buffer, level)
                .map_err(map_error_code)
                .unwrap();
            out
        } else {
            panic!();
            buf.to_vec()
        }
    }
}
