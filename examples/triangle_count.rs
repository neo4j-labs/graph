use log::info;
use std::time::Instant;

use graph::{
    graph::UndirectedCSRGraph, index::Idx, input::EdgeListInput, read_graph, Graph, UndirectedGraph,
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
        let g = load::<usize>(path);
        global_triangle_count(g);
    } else {
        let g = load::<u32>(path);
        global_triangle_count(g);
    }
}

fn load<T: Idx>(path: String) -> UndirectedCSRGraph<T> {
    let graph: UndirectedCSRGraph<T> = read_graph(path, EdgeListInput::default()).unwrap();
    info!("Node count = {:?}", graph.node_count());
    info!("Edge count = {:?}", graph.edge_count());
    graph
}

fn global_triangle_count<Node: Idx>(graph: UndirectedCSRGraph<Node>) -> usize {
    let start = Instant::now();
    let graph = graph.relabel_by_degrees();
    info!(
        "relabel_by_degree() took {} ms",
        start.elapsed().as_millis()
    );

    (0..graph.node_count().index())
        .into_par_iter()
        .map(Node::new)
        .map(|u| {
            let mut triangles = 0_usize;
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
}
