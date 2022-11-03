from pytest import fixture

from graph import DiGraph, Graph

FILE = "../builder/resources/scale_8.graph500"


@fixture(scope="package")
def g() -> DiGraph:
    """A directed graph"""
    return DiGraph.load(FILE)


@fixture(scope="package")
def ug() -> Graph:
    """An undirected graph"""
    return Graph.load(FILE)
