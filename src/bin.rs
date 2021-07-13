use std::time::Instant;

use graph::{graph::UndirectedCSRGraph, input::EdgeListInput, read_graph, Graph, UndirectedGraph};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() {
    let path = std::env::args()
        .into_iter()
        .skip(1)
        .next()
        .expect("require path argument");

    println!("opening path {}", path);
    let graph: UndirectedCSRGraph = read_graph(path, EdgeListInput).unwrap();

    println!("node count = {}", graph.node_count());
    println!("edge count = {}", graph.edge_count());

    // let start = Instant::now();
    // let global_count = tc(&graph);
    // println!(
    //     "global count = {}, took {} seconds",
    //     global_count,
    //     start.elapsed().as_secs()
    // );
}

fn tc(graph: &UndirectedCSRGraph) -> usize {
    (0..graph.node_count())
        .into_par_iter()
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

                    if let Some(x) = it.by_ref().skip_while(|&tmp| *tmp < w).next() {
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
