use std::time::Instant;

use graph::{graph::UndirectedCSRGraph, input::EdgeListInput, read_graph, Graph, UndirectedGraph};
use rayon::iter::{IntoParallelIterator, ParallelIterator};

fn main() {
    let path = std::env::args()
        .into_iter()
        .nth(1)
        .expect("require path argument");

    println!("opening path {}", path);
    let graph: UndirectedCSRGraph<usize> = read_graph(path, EdgeListInput::default()).unwrap();

    println!("node count = {}", graph.node_count());
    println!("edge count = {}", graph.edge_count());

    let start = Instant::now();
    println!("Start: relabel_by_degree()");
    let graph = graph.relabel_by_degrees();
    println!(
        "Finish: relabel_by_degree() took {} ms",
        start.elapsed().as_millis()
    );

    let start = Instant::now();
    println!("Start: global_triangle_count()");
    let global_count = global_triangle_count(&graph);
    println!(
        "Finish: global_triangle_count = {}, took {} seconds",
        global_count,
        start.elapsed().as_secs()
    );
}

fn global_triangle_count(graph: &UndirectedCSRGraph<usize>) -> usize {
    (0..graph.node_count())
        .into_par_iter()
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
