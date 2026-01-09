use criterion::{criterion_group, criterion_main, Criterion};

fn my_function(n: u64) -> u64 {
    (0..n).sum()
}

fn bench_my_function(c: &mut Criterion) {
    c.bench_function("my_function", |b| {
        b.iter(|| my_function(1_000))
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_my_function
}
criterion_main!(benches);