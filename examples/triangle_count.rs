use log::info;
use std::{sync::atomic::Ordering, time::Instant};

use graph::{
    graph::{CSROption, UndirectedCSRGraph},
    index::Idx,
    input::EdgeListInput,
    Graph, GraphBuilder, UndirectedGraph,
};

use graph::index::AtomicIdx;
use std::sync::atomic::AtomicU64;

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
            .csr_option(CSROption::Deduplicated)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .unwrap();

        global_triangle_count(g);
    } else {
        let g: UndirectedCSRGraph<u32> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .file_format(EdgeListInput::default())
            .path(path)
            .build()
            .unwrap();

        global_triangle_count(g);
    }
}

fn global_triangle_count<Node: Idx>(graph: UndirectedCSRGraph<Node>) -> u64 {
    let start = Instant::now();
    let graph = graph.relabel_by_degrees();
    info!(
        "relabel_by_degree() took {} ms",
        start.elapsed().as_millis()
    );

    let start = Instant::now();

    let next_chunk = Node::zero().atomic();
    let total_triangles = AtomicU64::new(0);

    rayon::scope(|s| {
        for _ in 0..rayon::current_num_threads() {
            s.spawn(|_| {
                let mut triangles = 0;

                loop {
                    let start = next_chunk.fetch_add(64, Ordering::AcqRel);
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

                            let mut it = graph.neighbors(u);

                            for &w in graph.neighbors(v) {
                                if w > v {
                                    break;
                                }
                                while let Some(&x) = it.first() {
                                    if x >= w {
                                        if x == w {
                                            triangles += 1;
                                        }
                                        break;
                                    }
                                    it = &it[1..];
                                }
                            }
                        }
                    }
                }
                total_triangles.fetch_add(triangles, Ordering::AcqRel);
            });
        }
    });

    let tc = total_triangles.load(Ordering::SeqCst);

    info!(
        "Triangle counting finished in {:?} seconds .. global triangle count = {}",
        start.elapsed(),
        tc
    );

    tc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CSROption;
    use crate::GraphBuilder;
    use crate::UndirectedCSRGraph;

    #[test]
    fn test_tc_two_components() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (3, 4), (3, 5), (4, 5)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(g), 2);
    }

    #[test]
    fn test_tc_connected_triangles() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (0, 4), (0, 5), (4, 5)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(g), 2);
    }

    #[test]
    fn test_tc_diamond() {
        let edges = vec![(0, 1), (0, 2), (1, 2), (1, 4), (2, 4)];

        let g: UndirectedCSRGraph<usize> = GraphBuilder::new()
            .csr_option(CSROption::Deduplicated)
            .edges(edges)
            .build();

        assert_eq!(global_triangle_count(g), 2);
    }
}
