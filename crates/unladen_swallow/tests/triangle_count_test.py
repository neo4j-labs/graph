import numpy as np
from graph_mate import Graph, Layout


def test_triangle_count(ug: Graph):
    tc = ug.global_triangle_count()

    assert tc.triangles == 227874
    assert tc.micros > 0


def test_tc_two_components():
    ug = Graph.from_numpy(
        np.array(
            [
                # (a)-->()-->()<--(a)
                [0, 1],
                [1, 2],
                [2, 0],
                # (b)-->()-->()<--(b)
                [3, 4],
                [4, 5],
                [5, 3],
            ],
            dtype=np.uint32,
        ),
        layout=Layout.Deduplicated,
    )

    tc = ug.global_triangle_count()

    assert tc.triangles == 2


def test_tc_connected_triangles():
    ug = Graph.from_numpy(
        np.array(
            [
                # (a)-->()-->()<--(a)
                [0, 1],
                [1, 2],
                [2, 0],
                # (1)-->()-->()<--(1)
                [0, 3],
                [3, 4],
                [4, 0],
            ],
            dtype=np.uint32,
        ),
        layout=Layout.Deduplicated,
    )

    tc = ug.global_triangle_count()

    assert tc.triangles == 2


def test_tc_diamond():
    ug = Graph.from_numpy(
        np.array(
            [
                # (a)-->(b)-->(c)<--(a)
                [0, 1],
                [1, 2],
                [2, 0],
                # (b)-->(d)<--(c)
                [1, 3],
                [3, 2],
            ],
            dtype=np.uint32,
        ),
        layout=Layout.Deduplicated,
    )

    tc = ug.global_triangle_count()

    assert tc.triangles == 2
