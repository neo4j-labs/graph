use graph::prelude::*;
use log::info;
use polars::prelude::*;

type AppResult = Result<(), Box<dyn std::error::Error>>;

fn main() -> AppResult {
    // First, we want to prepare some logging, so that we can see
    // the output of what's going on.
    env_logger::init();

    // Next we import the graph from the Graph500 format.
    // You can download the dataset from the LDBC Graphalytics site:
    // https://ldbcouncil.org/benchmarks/graphalytics/#data-sets
    //
    // We can now create a graph by loading the local file.
    // We also pass in CsrLayout::Deduplicated to tell
    // the builder to create a sorted adjacency
    // list and deduplicate parallel edges.
    let g: DirectedCsrGraph<u64> = GraphBuilder::new()
        .csr_layout(CsrLayout::Deduplicated)
        .file_format(Graph500Input::default())
        .path("examples/scale_24.graph")
        .build()?;

    // Now we can run Page Rank on the graph.
    // The result contains the actual page rank scores,
    // the number of iterations the algorithm ran until it
    // converged and the final error value.
    let (scores, iterations, _) = time(|| page_rank(&g, PageRankConfig::default()));
    info!("PageRank ran iterations: {iterations}");

    // Let's convert the scores to a polars DataFrame.
    let df = DataFrame::new(vec![Series::new("scores", scores)])?;
    // We can now calculate some statistics on the result.
    info!("size = {}", df.height());
    info!("min = {}", df.min());
    info!("max = {}", df.max());
    info!("mean = {}", df.mean());
    info!("median = {}", df.median());

    // Now we want to run Weakly Connected Components on the graph.
    // Similar to the Page Rank result, we get the component id for each node.
    let components = time(|| wcc_afforest(&g, WccConfig::default())).to_vec();

    // Calculate some statistics on the computed components.
    let df = DataFrame::new(vec![Series::new("components", components)])?;
    info!("size = {}", df.height());
    let df = df.unique(None, UniqueKeepStrategy::First)?;
    info!("component count = {}", df.height());

    // Now we want to count the total number of triangles in the graph.
    // We have to convert the graph to an undirected graph first.
    let mut ug = time(|| g.to_undirected(CsrLayout::Deduplicated));

    // If we are pressed for memory we can delete the directed graph.
    // The undirected graph is not a view, but a full copy of the graph.
    drop(g);

    // Counting triangles benefits from an adjacency list that is sorted by degree.
    // We can sort the adjacency list by calling the `make_degree_ordered` method.
    // In contrast to `to_undirected`, relabeling does not create a new graph,
    // instead it changes the adjacency list of the given graph.
    time(|| ug.make_degree_ordered());

    // Now we can count the number of global triangles in the graph.
    let tc = time(|| global_triangle_count(&ug));
    info!("TC: found {tc} triangles.");

    Ok(())
}

fn time<T, F: FnOnce() -> T>(f: F) -> T {
    let start = std::time::Instant::now();
    let res = f();
    info!("Execution took {:?}", start.elapsed());
    res
}
