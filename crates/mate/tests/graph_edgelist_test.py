import numpy as np
from graph_mate import DiGraph, Graph


def test_load_graph(el_g: DiGraph):
    assert el_g.node_count() == 5
    assert el_g.edge_count() == 6

    assert np.array_equal(el_g.out_neighbors(0), [1, 2])
    assert np.array_equal(el_g.out_neighbors(1), [2, 3])
    assert np.array_equal(el_g.out_neighbors(2), [4])
    assert np.array_equal(el_g.out_neighbors(3), [4])
    assert np.array_equal(el_g.out_neighbors(4), [])


def test_load_undirected_graph(el_ug: Graph):
    assert el_ug.node_count() == 5
    assert el_ug.edge_count() == 6

    assert np.array_equal(el_ug.neighbors(0), [1, 2])
    assert np.array_equal(el_ug.neighbors(1), [0, 2, 3])
    assert np.array_equal(el_ug.neighbors(2), [0, 1, 4])
    assert np.array_equal(el_ug.neighbors(3), [1, 4])
    assert np.array_equal(el_ug.neighbors(4), [2, 3])
