from pytest import fixture

from graph_mate import DiGraph, Graph, FileFormat, Layout

FILE = "../builder/resources/scale_8.graph500"
EL_FILE = "../builder/resources/test.el"


@fixture(scope="package")
def g() -> DiGraph:
    """A directed graph"""
    return DiGraph.load(FILE, layout=Layout.Sorted)


@fixture(scope="package")
def ug() -> Graph:
    """An undirected graph"""
    return Graph.load(FILE, layout=Layout.Sorted)


@fixture(scope="package")
def el_g() -> DiGraph:
    """A directed graph"""
    return DiGraph.load(EL_FILE, layout=Layout.Sorted, file_format=FileFormat.EdgeList)


@fixture(scope="package")
def el_ug() -> Graph:
    """An undirected graph"""
    return Graph.load(EL_FILE, layout=Layout.Sorted, file_format=FileFormat.EdgeList)
