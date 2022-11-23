import numpy as np
from graph_mate import DiGraph, Graph, Layout


def arr(a):
    return np.array(a, dtype=np.uint32)


def test_load_graph(g: DiGraph):
    assert g.node_count() == 1 << 8
    assert g.edge_count() == 1 << 12


def test_to_undirected(g: DiGraph, ug: Graph):
    g = g.to_undirected()

    for n in range(g.node_count()):
        assert set(g.copy_neighbors(n)) == set(ug.copy_neighbors(n))


def test_to_undirected_with_layout():
    g = DiGraph.from_numpy(
        np.array([[0, 1], [0, 1], [0, 2], [1, 2], [2, 1], [0, 3]], dtype=np.uint32)
    )

    ug = g.to_undirected()
    assert np.array_equal(ug.neighbors(0), [1, 1, 2, 3])
    assert np.array_equal(ug.neighbors(1), [2, 0, 0, 2])
    assert np.array_equal(ug.neighbors(2), [1, 0, 1])
    assert np.array_equal(ug.neighbors(3), [0])

    ug = g.to_undirected(Layout.Unsorted)
    assert np.array_equal(ug.neighbors(0), [1, 1, 2, 3])
    assert np.array_equal(ug.neighbors(1), [2, 0, 0, 2])
    assert np.array_equal(ug.neighbors(2), [1, 0, 1])
    assert np.array_equal(ug.neighbors(3), [0])

    ug = g.to_undirected(Layout.Sorted)
    assert np.array_equal(ug.neighbors(0), [1, 1, 2, 3])
    assert np.array_equal(ug.neighbors(1), [0, 0, 2, 2])
    assert np.array_equal(ug.neighbors(2), [0, 1, 1])
    assert np.array_equal(ug.neighbors(3), [0])

    ug = g.to_undirected(Layout.Deduplicated)
    assert np.array_equal(ug.neighbors(0), [1, 2, 3])
    assert np.array_equal(ug.neighbors(1), [0, 2])
    assert np.array_equal(ug.neighbors(2), [0, 1])
    assert np.array_equal(ug.neighbors(3), [0])


def test_reorder(ug: Graph):
    sorted_degrees = sorted(
        (ug.degree(n) for n in range(ug.node_count())), reverse=True
    )

    ug.make_degree_ordered()
    degrees = [ug.degree(n) for n in range(ug.node_count())]

    assert degrees == sorted_degrees
