from pytest import fixture

from unladen_swallow import Graph, Ungraph

FILE = "../builder/resources/scale_8.graph500"


@fixture(scope="package")
def g() -> Graph:
    """A directed graph"""
    return Graph.load(FILE)


@fixture(scope="package")
def ug() -> Ungraph:
    """An undirected graph"""
    return Ungraph.load(FILE)
