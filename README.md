# graph &emsp; [![GitHub Actions workflow status]][actions] [![Latest version on crates.io]][crates.io] [![Latest version on PyPI]][pypi.org] [![License: MIT]][license]

[GitHub Actions workflow status]: https://img.shields.io/github/workflow/status/s1ck/graph/CI/main?label=CI&style=flat-square
[actions]: https://github.com/s1ck/graph/actions/workflows/rust.yml?query=branch%3Amain
[Latest version on crates.io]: https://img.shields.io/crates/v/graph?style=flat-square
[crates.io]: https://crates.io/crates/graph/
[Latest version on PyPI]: https://img.shields.io/pypi/v/graph-mate?style=flat-square
[pypi.org]: https://pypi.org/project/graph-mate/
[License: MIT]: https://img.shields.io/crates/l/graph?style=flat-square
[license]: https://choosealicense.com/licenses/mit/


A library that provides a collection of high-performant graph algorithms.
This crate builds on top of the [graph_builder](https://docs.rs/graph_builder/latest/)
crate, which can be used as a building block for custom graph algorithms.

`graph_builder` provides implementations for directed and undirected graphs.
Graphs can be created programatically or read from custom input formats in a
type-safe way. The library uses [rayon](https://github.com/rayon-rs/rayon)
to parallelize all steps during graph creation. The implementation uses a
Compressed-Sparse-Row (CSR) data structure which is tailored for fast and
 concurrent access to the graph topology.

`graph` provides graph algorithms which take graphs created using `graph_builder`
as input. The algorithm implementations are designed to run efficiently on
large-scale graphs with billions of nodes and edges.

**Note**: The development is mainly driven by
[Neo4j](https://github.com/neo4j/neo4j) developers. However, the library is
__not__ an official product of Neo4j.

## What is a graph?

A graph consists of nodes and edges where edges connect exactly two nodes. A
graph can be either directed, i.e., an edge has a source and a target node
or undirected where there is no such distinction.

In a directed graph, each node `u` has outgoing and incoming neighbors. An
outgoing neighbor of node `u` is any node `v` for which an edge `(u, v)`
exists. An incoming neighbor of node `u` is any node `v` for which an edge
`(v, u)` exists.

In an undirected graph there is no distinction between source and target
node. A neighbor of node `u` is any node `v` for which either an edge `(u,
v)` or `(v, u)` exists.

## How to use graph?

The library provides a builder that can be used to construct a graph from a
given list of edges.

For example, to create a directed graph that uses `usize` as node
identifier, one can use the builder like so:

```rust
use graph::prelude::*;

let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
    .csr_layout(CsrLayout::Sorted)
    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
    .build();

assert_eq!(graph.node_count(), 4);
assert_eq!(graph.edge_count(), 5);

assert_eq!(graph.out_degree(1), 2);
assert_eq!(graph.in_degree(1), 1);

assert_eq!(graph.out_neighbors(1).as_slice(), &[2, 3]);
assert_eq!(graph.in_neighbors(1).as_slice(), &[0]);
```

To build an undirected graph using `u32` as node identifer, we only need to
change the expected types:

```rust
use graph::prelude::*;

let graph: UndirectedCsrGraph<u32> = GraphBuilder::new()
    .csr_layout(CsrLayout::Sorted)
    .edges(vec![(0, 1), (0, 2), (1, 2), (1, 3), (2, 3)])
    .build();

assert_eq!(graph.node_count(), 4);
assert_eq!(graph.edge_count(), 5);

assert_eq!(graph.degree(1), 3);

assert_eq!(graph.neighbors(1).as_slice(), &[0, 2, 3]);
```

Check out the [graph_builder](https://docs.rs/graph_builder/latest/) crate for
for more examples on how to build graphs from various input formats.

## How to run algorithms

In the following we will demonstrate running [Page Rank](https://en.wikipedia.org/wiki/PageRank),
a graph algorithm to determine the importance of nodes in a graph based on the
number and quality of their incoming edges.

Page Rank requires a directed graph and returns the rank value for each node.

```rust
use graph::prelude::*;

// https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.svg
let graph: DirectedCsrGraph<usize> = GraphBuilder::new()
    .edges(vec![
           (1,2), // B->C
           (2,1), // C->B
           (4,0), // D->A
           (4,1), // D->B
           (5,4), // E->D
           (5,1), // E->B
           (5,6), // E->F
           (6,1), // F->B
           (6,5), // F->E
           (7,1), // G->B
           (7,5), // F->E
           (8,1), // G->B
           (8,5), // G->E
           (9,1), // H->B
           (9,5), // H->E
           (10,1), // I->B
           (10,5), // I->E
           (11,5), // J->B
           (12,5), // K->B
    ])
    .build();

let (ranks, iterations, _) = page_rank(&graph, PageRankConfig::new(10, 1E-4, 0.85));

assert_eq!(iterations, 10);

let expected = vec![
    0.024064068,
    0.3145448,
    0.27890152,
    0.01153846,
    0.029471997,
    0.06329483,
    0.029471997,
    0.01153846,
    0.01153846,
    0.01153846,
    0.01153846,
    0.01153846,
    0.01153846,
];

assert_eq!(ranks, expected);
```

License: MIT
