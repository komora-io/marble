# Marble

<a href="https://docs.rs/marble"><img src="https://docs.rs/marble/badge.svg"></a>

Garbage-collecting disk-based object-store. See `examples/kv.rs`
for a minimal key-value store built on top of this.

Marble is [sled](https://github.com/spacejam/sled)'s future storage engine.

Supports 4 methods:
* `read`: designed for low-latency, concurrent reads of objects
* `write_batch`: designed for bulky, high-throughput writes of large batches of objects,
  ideal for compacting write-ahead logs into a set of updates to specific objects. Does
  not block calls to `read` except for brief moments where metadata is being updated.
* `maintenance`: compacts backing storage files that have become fragmented. Blocks
  concurrent calls to `write_batch` but does not block readers any more than `write_batch`
  does. Returns the number of successfully rewritten objects.
* `stats`: returns statistics about live and total objects in the backing storage files.


Marble is a low-level object store that can be used
to build your own storage engines and databases on
top of.

At a high-level, it supports atomic batch writes and
single-object reads. Garbage collection is manual.
All operations are blocking. Objects may be
sharded upon GC by providing a custom
`Config::partition_function`. Partitioning
is not performed on the write batch when it
is initially written, because the write batch
must be stored in a single file for it to be atomic.
But when future calls to `Marble::maintenance`
defragment the storage files by rewriting objects
that are still live, it will use this function
to assign the rewritten objects into a particular
partition.

You should think of Marble as the heap that you
flush your write-ahead logs into periodically.
It will create a new file for each write batch,
and this might actually expand to more files after
garbage collection if the batch is significantly
larger than the `Config::target_file_size`.

Marble does not create any threads or call
`Marble::maintenance` automatically under any
conditions. You should probably create a background
thread that calls this periodically.

# Examples

```rust
let marble = marble::open("heap").unwrap();

// Write new data keyed by a `u64` object ID.
// Batches contain insertions and deletions
// based on whether the value is a Some or None.
marble.write_batch([
    (0_u64, Some(&[32_u8] as &[u8])),
    (4_u64, None),
]).unwrap();

// read it back
assert_eq!(marble.read(0).unwrap(), Some(vec![32].into_boxed_slice()));
assert_eq!(marble.read(4).unwrap(), None);
assert_eq!(marble.read(6).unwrap(), None);

// after a few more batches that may have caused fragmentation
// by overwriting previous objects, perform maintenance which
// will defragment the object store based on `Config` settings.
let objects_defragmented = marble.maintenance().unwrap();

// print out system statistics
dbg!(marble.stats());
```

which prints out something like
```txt,no_run
marble.stats() = Stats {
    live_objects: 1048576,
    stored_objects: 1181100,
    dead_objects: 132524,
    live_percent: 88,
    files: 11,
}
```

If you want to customize the settings passed to Marble,
you may specify your own `Config`:

```rust
let config = marble::Config {
    path: "my_path".into(),
    zstd_compression_level: Some(7),
    fsync_each_batch: true,
    target_file_size: 64 * 1024 * 1024,
    file_compaction_percent: 50,
    ..Default::default()
};

let marble = config.open().unwrap();
```

A custom GC sharding function may be provided
for partitioning objects based on the object ID
and size. This may be useful if your higher-level
system allocates certain ranges of object IDs for
certain types of objects that you would like to
group together in the hope of grouping items together
that have similar fragmentation properties (similar
expected lifespan etc...). This will only shard
objects when they are defragmented through the
`Marble::maintenance` method, because each new
write batch must be written together in one
file to retain write batch atomicity in the
face of crashes.

```rust
// This function shards objects into partitions
// similarly to a slab allocator that groups objects
// into size buckets based on powers of two.
fn shard_by_size(object_id: u64, object_size: usize) -> u8 {
    let next_po2 = object_size.next_power_of_two();
    u8::try_from(next_po2.trailing_zeros()).unwrap()
}

let config = marble::Config {
    path: "my_sharded_path".into(),
    partition_function: shard_by_size,
    ..Default::default()
};

let marble = config.open().unwrap();
```

Defragmentation is always generational, and will group rewritten
objects together. Written objects can be further sharded based on a
configured `partition_function` which allows you to shard objects
by `ObjectId` and the size of the object raw bytes.

Marble solves a pretty basic problem in database storage: storing
arbitrary bytes on-disk, getting them back, and defragmenting files.

You can think of it as a KV where keys are non-zero u64's, and values
are arbitrary blobs of raw bytes.

Writes are meant to be performed in bulk by some background process.
Each call to `Marble::write_batch` creates at least one new file
that stores the objects being written. Multiple calls to fsync occur
for each call to `write_batch`. It is blocking. Object metadata is added
to the backing wait-free pagetable incrementally, not atomically,
so if you rely on batch atomicity, you should serve the batch's objects
directly from a cache of your own until `write_batch` returns.
However, upon crash, batches are recovered atomically.

Reads can continue mostly unblocked while batch writes and maintenance are being handled.

You are responsible for:
* calling `Marble::maintenance` at appropriate intervals to defragment
  storage files.
* choosing appropriate configuration tunables for your desired space
  and write amplification.
* ensuring the `Config.partition_function` is set to a function that
  appropriately shards your objects based on their `ObjectId` and/or size.
  Ideally, objects that have expected death times will be colocated in
  a shard so that work spent copying live objects is minimized.
* allocating and managing free `ObjectId`'s.

If you want to create an industrial database on top of Marble, you will
probably also want to add:
* logging and a write cache for accumulating updates that occasionally
  get flushed to Marble via `write_batch`. Remember, each call to
  `write_batch` creates at least one new file and fsyncs multiple times,
  so you should batch calls appropriately. Once the log or write cache has
  reached an appropriate size, you can have a background thread write a
  corresponding batch of objects to its storage, and once write_batch returns, the
  corresponding log segments and write cache can be deleted, as the objects
  will be available via `Marble::read`.
* an appropriate read cache. `Marble::read` always reads directly from disk.
* for maximum SSD friendliness, your own log should be configurable to be
  written to a separate storage device, to avoid comingling writes that
  have vastly different expected death times.
* dictionary-based compression for efficiently compressing objects that may
  be smaller than 64k.

Ideas for getting great garbage collection performance:
* give certain kinds of objects a certain ObjectId range. for example,
  tree index nodes can be above 1<<63, and tree leaf nodes can be below
  that point. The `Config.partition_function` can return the shard 0 for
  leaf nodes, and 1 for index nodes, and they will always be written to
  separate files.
* WiscKey-style sharding of large items from other items, based on the
  size of the object. Assign a shard ID based on which power of 2 the
  object size is.
* Basically any sharding strategy that tends to group items together that
  exhibit some amount of locality in terms of expected mutations or
  overall lifespan.

In short, you get to focus on a bunch of the fun parts of building your own
database, without so much effort spent on boring file garbage collection.
