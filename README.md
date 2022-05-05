# marble

Garbage-collecting disk-based object-store. See `examples/kv.rs`
for a minimal key-value store built on top of this.

Supports 4 methods:
* `read`: designed for low-latency, concurrent reads of objects
* `write_batch`: designed for bulky, high-throughput writes of large batches of objects,
  ideal for compacting write-ahead logs into a set of updates to specific objects. Does
  not block calls to `read` except for brief moments where metadata is being updated.
* `maintenance`: compacts backing storage files that have become fragmented. Blocks
  concurrent calls to `write_batch` but does not block readers any more than `write_batch`
  does. Returns the number of successfully rewritten objects.
* `file_statistics`: returns statistics about live and total objects in the backing storage files.

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

In short, you get to focus on a bunch of the fun parts of building your own
database, without so much effort spent on boring file garbage collection.
