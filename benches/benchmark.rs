use core::fmt;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use graph::{prelude::Idx, CsrLayout, DirectedCsrGraph, Graph, GraphBuilder};

use rand::prelude::*;

fn build_from_edge_list_vec(c: &mut Criterion) {
    let mut group = c.benchmark_group("from_edge_list_vec");

    for edge_count in [10_000, 100_000, 1_000_000] {
        for csr_layout in [
            CsrLayout::Unsorted,
            CsrLayout::Sorted,
            CsrLayout::Deduplicated,
        ] {
            let config = BenchmarkConfig {
                edge_count,
                csr_layout,
            };

            let edges = gen_uniform_edge_list(edge_count);

            group.bench_with_input(
                BenchmarkId::from_parameter(&config),
                &config,
                move |b, config| {
                    b.iter_batched(
                        || edges.clone(),
                        |edges| {
                            let g: DirectedCsrGraph<usize> = GraphBuilder::new()
                                .csr_layout(config.csr_layout)
                                .edges(edges)
                                .build();
                            black_box(g.node_count());
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
}

struct BenchmarkConfig {
    edge_count: usize,
    csr_layout: CsrLayout,
}

impl fmt::Display for BenchmarkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{:?}", self.edge_count, self.csr_layout)
    }
}

fn gen_uniform_edge_list<NI>(edge_count: usize) -> Vec<(NI, NI)>
where
    NI: Idx,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..edge_count)
        .map(|_| {
            let source = NI::new(rng.gen_range(0..edge_count));
            let target = NI::new(rng.gen_range(0..edge_count));

            (source, target)
        })
        .collect::<Vec<_>>()
}

criterion_group!(benches, build_from_edge_list_vec);
criterion_main!(benches);
