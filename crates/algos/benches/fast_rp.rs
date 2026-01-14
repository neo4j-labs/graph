use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graph::prelude::{fast_rp, FastRPConfig};
use graph_builder::{CsrLayout, DirectedCsrGraph, GraphBuilder};

mod common;
use crate::common::*;

pub fn uniform_graph(n: usize, m: usize) -> DirectedCsrGraph<usize> {
    GraphBuilder::new()
        .csr_layout(CsrLayout::Unsorted)
        .edges(uniform_edge_list(n, m))
        .build()
}

fn bench_fast_rp(c: &mut Criterion) {
    let n = 1 << 18;
    let m = 4 * n;
    let graph = uniform_graph(n, m);

    c.bench_function("compute_fast_rp", |b| {
        b.iter(|| {
            black_box(fast_rp(
                &graph,
                FastRPConfig::default(),
            ))
        })
    });
}
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_fast_rp
}
criterion_main!(benches);
