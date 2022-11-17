# graph_mate

Python binding for the set of `graph` crates.

`graph_mate` is a Python API that provides a collection of high-performant graph algorithms.
It provides implementations for directed and undirected graphs.

Graphs can be created programatically or read from the [`Graph500`](https://graph500.org/) input format.

The library is implemented in Rust and uses [rayon](https://github.com/rayon-rs/rayon) for running graph creation and algorithm execution in parallel without holding on to the Global Interpreter Lock or using multiprocessing.

The graph itself is implemented as a Compressed-Sparse-Row (CSR) data structure which is tailored for fast and concurrent access to the graph topology.

`graph_mate` provides a few graph algorithms.
The algorithm implementations are designed to run efficiently on large-scale graphs with billions of nodes and edges.


**Note**: The development is mainly driven by [Neo4j](https://github.com/neo4j/neo4j) developers.
However, the library is __not__ an official product of Neo4j.

## Usage

### Installation

`graph_mate` is available on [PyPI](https://pypi.org/project/graph-mate/):

```sh
pip install graph-mate
```

It is currently build for x86_64 on Windows/Mac/Linux and as universal for Apple Silicon Macs.

If you need to use `graph_mate` for a different architecture or system, please refer to the [manual installation](#developing).

### Usage

#### What is a graph?

A graph consists of nodes and edges where edges connect exactly two nodes.
A graph can be either directed, i.e., an edge has a source and a target node or undirected where there is no such distinction.

In a directed graph, each node `u` has outgoing and incoming neighbors.
An outgoing neighbor of node `u` is any node `v` for which an edge `(u, v)` exists.
An incoming neighbor of node `u` is any node `v` for which an edge `(v, u)` exists.

In an undirected graph there is no distinction between source and target node.
A neighbor of node `u` is any node `v` for which either an edge `(u,v)` or `(v, u)` exists.

#### How to create graphs

Currently there are two ways to build a graph.
You can either `load` graphs in the [`Graph500`](https://graph500.org/?page_id=12) format, for example by downloading them from the [LDBC Graphalytics site](https://ldbcouncil.org/benchmarks/graphalytics/#data-sets).
Alternatively, you can provide a numpy edge list using `from_numpy`.

```python
import graph_mate as gm
import numpy as np

# Let's load a small graph:
#    (a)-->(b)-->(c)-->(d), (a)-->(c), (b)-->(d)
# To load from an edge list, we need to create a 2d numpy array of `uint32`s.
edge_list = np.array([
    # (a)-->(b)
    [0, 1],
    # (a)-->(c)
    [0, 2],
    # (b)-->(c)
    [1, 2],
    # (b)-->(d)
    [1, 3],
    # (c)-->(d)
    [2, 3]
], dtype=np.uint32)
```

To build a directed graph, you would create a `graph_mate.DiGraph` and an undirected on with `graph_mate.Graph`.

```python
# We can load a directed graph from the edge list
directed = gm.DiGraph.from_numpy(edge_list)

# Or we can load an undirected graph
undirected = gm.Graph.from_numpy(edge_list)
```

To make assertions easier, we can create graphs with a sorted adjacency list by providing an optional second argument of type `graph_mate.Layout`.

```python
directed = gm.DiGraph.from_numpy(edge_list, gm.Layout.Sorted)

undirected = gm.Graph.from_numpy(edge_list, gm.Layout.Sorted)
```

When loading from a numpy edge list, the data is *not* shared but copied into the graph.
The numpy arrays can be deleted afterwards.

We can inspect the graph with a few methods.

```python
assert directed.node_count() == 4
assert directed.edge_count() == 5

assert directed.out_degree(1) == 2
assert directed.in_degree(1) == 1

assert np.array_equal(directed.out_neighbors(1), [2, 3])
assert np.array_equal(directed.in_neighbors(1), [0])
```

Neighbors are returned as a numpy array view into the graph, which means we cannot modify the array.

```python
neighbors = directed.out_neighbors(1)
try:
    neighbors[0] = 42
except ValueError as e:
    assert str(e) == "assignment destination is read-only"
```

In order to use the neighbors as a Python list and not a numpy array, we can use `copy_` methods.

```python
neighbors = directed.copy_out_neighbors(1)

assert neighbors == [2, 3]
assert type(neighbors) == list
```

For undirected graphs, we don't have directional methods for the degree or the neighbors.

```python
assert undirected.node_count() == 4
assert undirected.edge_count() == 5

assert undirected.degree(1) == 3

assert np.array_equal(undirected.neighbors(1), [0, 2, 3])
```

#### How to run algorithms

In the following we will demonstrate running [Page Rank](https://en.wikipedia.org/wiki/PageRank), a graph algorithm to determine the importance of nodes in a graph based on the number and quality of their incoming edges.
Page Rank requires a directed graph and returns the rank value for each node.

```python
# https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.svg

graph = gm.DiGraph.from_numpy(np.array([
    (1,2), # B->C
    (2,1), # C->B
    (4,0), # D->A
    (4,1), # D->B
    (5,4), # E->D
    (5,1), # E->B
    (5,6), # E->F
    (6,1), # F->B
    (6,5), # F->E
    (7,1), # G->B
    (7,5), # F->E
    (8,1), # G->B
    (8,5), # G->E
    (9,1), # H->B
    (9,5), # H->E
    (10,1), # I->B
    (10,5), # I->E
    (11,5), # J->B
    (12,5), # K->B
], dtype=np.uint32))

pr_result = graph.page_rank(max_iterations=10, tolerance=1e-4, damping_factor=0.85)

assert pr_result.ran_iterations == 10

expected = np.array([
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
], dtype=np.float32)

assert np.array_equal(pr_result.scores(), expected)
```

### Example Notebooks

For more examples and demos, please refer to the notebooks in the `notebooks` directory.

## Developing

The Python extension is written using [PyO3](https://pyo3.rs/v0.16.2/)
together with [maturin](https://github.com/PyO3/maturin).

### One-time setup

```
# Run the command from the extension directory, not the git root
# cd crates/mate

python -m venv .env
source .env/bin/activate
pip install -r requirements/dev.txt
```

### Once-per-new-terminal setup

Make sure that you're activating the venv in every new terminal where you want to develop.

```
source .env/bin/activate
```

### Building the extension

Build in debug mode.

```
maturin develop
```

Build in release mode.

```
maturin develop --release
```

Rebuild the extension in release mode 2 seconds after the last file change.
This is an optional step.

```
cargo watch --shell 'maturin develop --release' --delay 2
```

### Testing

Running the tests

```
pytest tests
```

### Formatting and linting

```
# Runs code formatter https://pypi.org/project/black/
black tests

# Sort imports using https://pypi.org/project/isort/
isort tests

# Verify with https://pypi.org/project/flake8/
flake8 tests

# Very types using http://mypy-lang.org
mypy .
```

License: MIT
