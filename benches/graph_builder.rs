use core::fmt;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode,
};
use graph::{prelude::Idx, CsrLayout, DirectedCsrGraph, Graph, GraphBuilder, UndirectedCsrGraph};

use rand::prelude::*;

fn build_from_edges(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_from_edges");
    // intended for long-running benchmarks
    group.sampling_mode(SamplingMode::Flat);

    for node_count in [1_000, 10_000, 100_000] {
        for edge_count in [node_count * 1, node_count * 2, node_count * 10] {
            for orientation in [Orientation::Directed, Orientation::Undirected] {
                for csr_layout in [
                    CsrLayout::Unsorted,
                    CsrLayout::Sorted,
                    CsrLayout::Deduplicated,
                ] {
                    let edges = gen_uniform_edge_list(node_count, edge_count);

                    let config = BenchmarkConfig {
                        node_count,
                        edge_count,
                        csr_layout,
                        orientation,
                    };

                    bench(&mut group, edges, config);
                }
            }
        }
    }
}

fn bench(
    group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>,
    edges: Vec<(usize, usize)>,
    config: BenchmarkConfig,
) {
    group.bench_with_input(
        BenchmarkId::from_parameter(&config),
        &config,
        move |b, config| {
            b.iter_batched(
                || edges.clone(),
                |edges| match config.orientation {
                    Orientation::Directed => black_box(
                        GraphBuilder::new()
                            .csr_layout(config.csr_layout)
                            .edges(edges)
                            .build::<DirectedCsrGraph<usize>>()
                            .node_count(),
                    ),
                    Orientation::Undirected => black_box(
                        GraphBuilder::new()
                            .csr_layout(config.csr_layout)
                            .edges(edges)
                            .build::<UndirectedCsrGraph<usize>>()
                            .node_count(),
                    ),
                },
                BatchSize::SmallInput,
            )
        },
    );
}

struct BenchmarkConfig {
    node_count: usize,
    edge_count: usize,
    csr_layout: CsrLayout,
    orientation: Orientation,
}

impl fmt::Display for BenchmarkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{:?}_{:?}",
            self.node_count, self.edge_count, self.csr_layout, self.orientation
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum Orientation {
    Directed,
    Undirected,
}

fn gen_uniform_edge_list<NI>(node_count: usize, edge_count: usize) -> Vec<(NI, NI)>
where
    NI: Idx,
{
    let mut rng = StdRng::seed_from_u64(42);

    (0..edge_count)
        .map(|_| {
            let source = NI::new(rng.gen_range(0..node_count));
            let target = NI::new(rng.gen_range(0..node_count));

            (source, target)
        })
        .collect::<Vec<_>>()
}

criterion_group!(benches, build_from_edges);
criterion_main!(benches);
