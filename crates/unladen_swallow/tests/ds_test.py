import numpy as np
import pandas as pd

from graph_mate import DiGraph, Graph


def test_numpy_graph():
    el = np.array([[0, 1], [2, 3], [4, 1]], dtype=np.uint32)
    g = Graph.from_numpy(el)

    assert g.node_count() == 5
    assert g.edge_count() == 3

    assert np.array_equal(g.neighbors(0), np.array([1], dtype=np.uint32))
    assert np.array_equal(g.neighbors(1), np.array([0, 4], dtype=np.uint32))
    assert np.array_equal(g.neighbors(2), np.array([3], dtype=np.uint32))
    assert np.array_equal(g.neighbors(3), np.array([2], dtype=np.uint32))
    assert np.array_equal(g.neighbors(4), np.array([1], dtype=np.uint32))


def test_pandas_graph():
    df = pd.DataFrame({"source": [0, 2, 4], "target": [1, 3, 1]})
    g = Graph.from_pandas(df)

    assert g.node_count() == 5
    assert g.edge_count() == 3

    assert np.array_equal(g.neighbors(0), np.array([1], dtype=np.uint32))
    assert np.array_equal(g.neighbors(1), np.array([0, 4], dtype=np.uint32))
    assert np.array_equal(g.neighbors(2), np.array([3], dtype=np.uint32))
    assert np.array_equal(g.neighbors(3), np.array([2], dtype=np.uint32))
    assert np.array_equal(g.neighbors(4), np.array([1], dtype=np.uint32))


def test_numpy_digraph():
    el = np.array([[0, 1], [2, 3], [4, 1]], dtype=np.uint32)
    g = DiGraph.from_numpy(el)

    assert g.node_count() == 5
    assert g.edge_count() == 3

    assert np.array_equal(g.out_neighbors(0), np.array([1], dtype=np.uint32))
    assert np.array_equal(g.out_neighbors(2), np.array([3], dtype=np.uint32))
    assert np.array_equal(g.out_neighbors(4), np.array([1], dtype=np.uint32))

    assert np.array_equal(g.in_neighbors(1), np.array([0, 4], dtype=np.uint32))
    assert np.array_equal(g.in_neighbors(3), np.array([2], dtype=np.uint32))


def test_pandas_digraph():
    df = pd.DataFrame({"source": [0, 2, 4], "target": [1, 3, 1]})
    g = DiGraph.from_pandas(df)

    assert g.node_count() == 5
    assert g.edge_count() == 3

    assert np.array_equal(g.out_neighbors(0), np.array([1], dtype=np.uint32))
    assert np.array_equal(g.out_neighbors(2), np.array([3], dtype=np.uint32))
    assert np.array_equal(g.out_neighbors(4), np.array([1], dtype=np.uint32))

    assert np.array_equal(g.in_neighbors(1), np.array([0, 4], dtype=np.uint32))
    assert np.array_equal(g.in_neighbors(3), np.array([2], dtype=np.uint32))
