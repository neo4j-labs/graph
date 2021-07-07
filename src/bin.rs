use graph::{graph::UndirectedCSRGraph, input::EdgeListInput, read_graph, Graph};

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
}
