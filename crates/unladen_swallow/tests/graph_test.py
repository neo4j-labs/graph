from unladen_swallow import Graph, Ungraph


def test_load_graph(g: Graph):
    assert g.node_count() == 1 << 8
    assert g.edge_count() == 1 << 12


def test_to_undirected(g: Graph, ug: Ungraph):
    g = g.to_undirected()

    for n in range(g.node_count()):
        assert set(g.copy_neighbors(n)) == set(ug.copy_neighbors(n))


def test_reorder(ug: Ungraph):
    sorted_degrees = sorted(
        (ug.degree(n) for n in range(ug.node_count())), reverse=True
    )

    ug.reorder_by_degree()
    degrees = [ug.degree(n) for n in range(ug.node_count())]

    assert degrees == sorted_degrees
