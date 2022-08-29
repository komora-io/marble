use criterion::{criterion_group, criterion_main, Criterion};

use marble::open;

pub fn measure_latency(c: &mut Criterion) {
    let marble = open("measure_latency").unwrap();
    c.bench_function("0b", |b| {
        b.iter(|| {
            let batch = [(0, Some(vec![0_u8]))];
            marble.write_batch(batch.into_iter()).unwrap();
        })
    });
}

pub fn measure_throughput(c: &mut Criterion) {
    let marble = open("measure_throughput").unwrap();
    c.bench_function("0b", |b| {
        b.iter(|| {
            let batch = [(0, Some(vec![0_u8]))];
            marble.write_batch(batch.into_iter()).unwrap();
        })
    });
}

criterion_group!(latency, measure_latency);
criterion_group!(throughput, measure_throughput);
criterion_main!(latency, throughput);
