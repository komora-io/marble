use std::io;

use backtrace::{Backtrace, BacktraceFrame};

#[derive(Debug)]
pub struct Error {
    thrower: String,
    io_error: io::Error,
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        let backtrace = Backtrace::new();
        let backtrace_frames: Vec<BacktraceFrame> = backtrace.into();
        let frame = backtrace_frames[6].symbols().first().unwrap();
        let thrower = format!(
            "{}: {}",
            frame
                .filename()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy(),
            frame.lineno().unwrap()
        );

        Error { thrower, io_error }
    }
}

impl std::ops::Deref for Error {
    type Target = io::Error;

    fn deref(&self) -> &io::Error {
        &self.io_error
    }
}
