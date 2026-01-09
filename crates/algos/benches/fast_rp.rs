use criterion::{black_box, criterion_group, criterion_main, Criterion};
use graph::prelude::{fast_rp, FastRPConfig};
use graph_builder::{CsrLayout, DirectedCsrGraph, GraphBuilder};

const A: u32 = (0.59 * u32::MAX as f64) as u32;
const B: u32 = (0.19 * u32::MAX as f64) as u32;
const C: u32 = (0.19 * u32::MAX as f64) as u32;
// const D: u32 = (0.03 * u32::MAX as f64) as u32;

fn edge(e: u8) -> (usize, usize) {
    let mut s = 0usize;
    let mut t = 0usize;
    for bit in 0..e { //better to set e at compile time?
        let rand = rand::random::<u32>();
        if rand < A {
        } else if rand < A + B {
            s += 1usize << bit;
        } else if rand < A + B + C {
            t += 1usize << bit;
        } else {
            s += 1usize << bit;
            t += 1usize << bit;
        }
    }
    (s,t)
}

fn edge_list(e: u8, m: usize) -> Vec<(usize, usize)> {
    (0..m).map(|_| edge(e)).collect()
}

pub fn kronecker_graph(e: u8, m: usize) -> DirectedCsrGraph<usize> {
    GraphBuilder::new()
        .csr_layout(CsrLayout::Unsorted)
        .edges(edge_list(e, m))
        .build()
}

fn bench_fast_rp(c : &mut Criterion) {

    let e = 18u8; //n = 2**16 ~ 64k
    let d = 4;
    let m = d * (2usize).pow(e as u32); //m = 2**18 ~ 256k
    let graph = kronecker_graph(e, m);


    c.bench_function("compute_fast_rp", |b| {
        b.iter(|| black_box(fast_rp(&graph, FastRPConfig::default())))
    });

}
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_fast_rp
}
criterion_main!(benches);