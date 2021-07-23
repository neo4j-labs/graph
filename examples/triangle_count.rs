use log::info;
use std::time::Instant;

use graph::{
    graph::{CSROption, UndirectedCSRGraph},
    index::Idx,
    input::EdgeListInput,
    read_graph, Graph, UndirectedGraph,
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
        let g: UndirectedCSRGraph<usize> =
            read_graph(path, EdgeListInput::default(), CSROption::default()).unwrap();
        global_triangle_count(g);
    } else {
        let g: UndirectedCSRGraph<u32> =
            read_graph(path, EdgeListInput::default(), CSROption::default()).unwrap();
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
    let tc: usize = (0..graph.node_count().index())
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
        .sum();

    info!(
        "Triangle counting finished in {} seconds .. global triangle count = {}",
        start.elapsed().as_secs(),
        tc
    );
}
