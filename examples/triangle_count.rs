use log::info;
use std::time::Instant;

use graph::{
    graph::{CSROption, UndirectedCSRGraph},
    index::Idx,
    input::EdgeListInput,
    Graph, GraphBuilder, UndirectedGraph,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() {
    env_logger::init();

    let path = std::env::args()
        .into_iter()
        .nth(1)
        .expect("require path argument");

    let use_64_bit = std::env::args()
        .into_iter()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(true);

    info!(
        "Reading graph ({} bit) from: {}",
        if use_64_bit { "64" } else { "32" },
        path
    );

    if use_64_bit {
        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .unwrap();

        global_triangle_count(g);
    } else {
        let g: UndirectedCSRGraph<u32> = GraphBuilder::new()
            .csr_option(CSROption::Sorted)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .unwrap();

        global_triangle_count(g);
    }
}

fn global_triangle_count<Node: Idx>(graph: UndirectedCSRGraph<Node>) {
    let start = Instant::now();
    let graph = graph.relabel_by_degrees();
    info!(
        "relabel_by_degree() took {} ms",
        start.elapsed().as_millis()
    );

    let start = Instant::now();

    let tc = if std::env::var_os("TC_SCOPED").is_some() {
        use graph::index::AtomicIdx;
        use std::sync::atomic::AtomicU64;

        let next_chunk = Node::zero().atomic();
        let total_triangles = AtomicU64::new(0);

        rayon::scope(|s| {
            for _ in 0..rayon::current_num_threads() {
                s.spawn(|_| {
                    let mut triangles = 0;

                    loop {
                        let start = next_chunk.fetch_add(64, std::sync::atomic::Ordering::AcqRel);
                        if start >= graph.node_count() {
                            break;
                        }

                        let end = (start + Node::new(64)).min(graph.node_count());

                        for n in start.index()..end.index() {
                            let u = Node::new(n);

                            for &v in graph.neighbors(u) {
                                if v > u {
                                    break;
                                }
                                let mut it = graph.neighbors(u).iter();

                                for &w in graph.neighbors(v) {
                                    if w > v {
                                        break;
                                    }

                                    if let Some(x) = it.by_ref().find(|&tmp| *tmp >= w) {
                                        if *x == w {
                                            triangles += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    total_triangles.fetch_add(triangles, std::sync::atomic::Ordering::AcqRel);
                });
            }
        });

        total_triangles.load(std::sync::atomic::Ordering::SeqCst)
    } else {
        (0..graph.node_count().index())
            .into_par_iter()
            .map(Node::new)
            .map(|u| {
                let mut triangles = 0;
                for &v in graph.neighbors(u) {
                    if v > u {
                        break;
                    }
                    let mut it = graph.neighbors(u).iter();

                    for &w in graph.neighbors(v) {
                        if w > v {
                            break;
                        }

                        if let Some(x) = it.by_ref().find(|&tmp| *tmp >= w) {
                            if *x == w {
                                triangles += 1;
                            }
                        }
                    }
                }
                triangles
            })
            .sum()
    };

    info!(
        "Triangle counting finished in {:?} seconds .. global triangle count = {}",
        start.elapsed(),
        tc
    );
}
