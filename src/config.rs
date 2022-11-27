use std::io;
use std::path::PathBuf;

/// Configuration for configuring `Marble`.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage files will be kept here.
    pub path: PathBuf,
    /// The compression level to use when compressing each
    /// batch of objects. A value of `None` disables
    /// compression. This is one of the most important
    /// parameters to experiment with while finding an
    /// appropriate configuration for your system.
    pub zstd_compression_level: Option<i32>,
    /// Issue fsyncs on each new file and the containing
    /// directory when it is created. This corresponds
    /// to at least one call to fsync for each call to
    /// `write_batch`.
    pub fsync_each_batch: bool,
    /// Garbage collection will try to keep storage
    /// files around this size or smaller.
    pub target_file_size: usize,
    /// Remaining live percentage of a file before
    /// it's considered rewritabe.
    pub file_compaction_percent: u8,
    /// The ceiling on the largest allocation this system
    /// will ever attempt to perform in order to read an
    /// object off of disk.
    pub max_object_size: usize,
    /// The number of total files (of all sizes) that must
    /// exist before "small files" are squished together
    /// even if they are above the `file_compaction_percent`.
    /// A "small file" is defined as a file whose uncompressed
    /// size times `min_compaction_files` is below the
    /// `target_file_size`.
    pub small_file_cleanup_threshold: usize,
    /// A partitioning function for objects based on
    /// object ID and object size. You may override this to
    /// cause objects to be written into separate files so
    /// that garbage collection may take advantage of
    /// locality effects for your workload that are
    /// correlated to object identifiers or the size of
    /// data.
    ///
    /// Ideally, you will colocate objects that have
    /// similar expected lifespans. Doing so minimizes
    /// the costs of copying live data over time during
    /// storage file GC.
    pub partition_function: fn(object_id: u64, object_size: usize) -> u8,
    /// The minimum number of files within a generation to
    /// collect if below the live compaction percent.
    pub min_compaction_files: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: "".into(),
            target_file_size: 1 << 28, // 256mb
            file_compaction_percent: 66,
            partition_function: crate::default_partition_function,
            max_object_size: 16 * 1024 * 1024 * 1024, /* 16gb */
            small_file_cleanup_threshold: 64,
            min_compaction_files: 2,
            fsync_each_batch: false,
            zstd_compression_level: None,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.target_file_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Config's target_file_size must be non-zero",
            ));
        }

        if self.file_compaction_percent > 99 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Config's file_compaction_percent must be less than 100",
            ));
        }

        Ok(())
    }
}
