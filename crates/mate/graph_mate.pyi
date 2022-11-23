from typing import Optional, overload

import numpy as np
import numpy.typing as npt
import pandas as pd

class Layout:
    """
    Defines how the neighbor list of individual nodes are organized within the
    CSR target array.
    """

    """
    Neighbor lists are sorted and may contain duplicate target ids.
    """
    Sorted: Layout

    """
    Neighbor lists are not in any particular order.
    This is the default representation.
    """
    Unsorted: Layout

    """
    Neighbor lists are sorted and do not contain duplicate target ids.
    Self-loops, i.e., edges in the form of `(u, u)` are removed.
    """
    Deduplicated: Layout

class FileFormat:
    """
    Defines the file format of an input file.
    """

    """
    The input in a binary Graph500 format.
    """
    Graph500: FileFormat

    """
    The input is a text file where each line represents an edge in the form
    of `<source_id> <target_id>`.
    """
    EdgeList: FileFormat

class DiGraph:
    """
    A directed graph using 32 bits for node ids.
    """

    @staticmethod
    def load(
        path: str, layout: Layout = Layout.Unsorted, file_format=FileFormat.Graph500
    ) -> DiGraph:
        """Load a graph from the provided format."""
    @staticmethod
    def from_numpy(
        np: npt.NDArray[np.uint32], layout: Layout = Layout.Unsorted
    ) -> DiGraph:
        """Convert a numpy 2d-array into a graph."""
    @staticmethod
    def from_pandas(df: pd.DataFrame, layout: Layout = Layout.Unsorted) -> DiGraph:
        """Convert a pandas dataframe into a graph."""
    def node_count(self) -> int:
        """Returns the number of nodes in the graph."""
    def edge_count(self) -> int:
        """Returns the number of edges in the graph."""
    def out_degree(self, node: int) -> int:
        """Returns the number of edges where the given node is a source node."""
    def in_degree(self, node: int) -> int:
        """Returns the number of edges where the given node is a target node."""
    def out_neighbors(self, node: int) -> npt.NDArray[np.uint32]:
        """
        Returns all nodes which are connected in outgoing direction to the given node,
        i.e., the given node is the source node of the connecting edge.

        This functions returns a numpy array that directly references this graph without
        making a copy of the data.
        """
    def in_neighbors(self, node: int) -> npt.NDArray[np.uint32]:
        """
        Returns all nodes which are connected in incoming direction to the given node,
        i.e., the given node is the target node of the connecting edge.

        This functions returns a numpy array that directly references this graph without
        making a copy of the data.
        """
    def copy_out_neighbors(self, node: int) -> list[int]:
        """
        Returns all nodes which are connected in outgoing direction to the given node,
        i.e., the given node is the source node of the connecting edge.

        This function returns a copy of the data as a Python list.
        """
    def copy_in_neighbors(self, node: int) -> list[int]:
        """
        Returns all nodes which are connected in incoming direction to the given node,
        i.e., the given node is the target node of theconnecting edge.

        This function returns a copy of the data as a Python list.
        """
    def to_undirected(self, layout: Layout = None) -> Graph:
        """
        Convert this graph into an undirected graph.
        The new graph is unrelated to this graph and does not share any data.
        """
    def page_rank(
        self, *, max_iterations: int, tolerance: float, damping_factor: float
    ) -> PageRankResult:
        """Run Page Rank on this graph."""
    def wcc(
        self, *, chunk_size: int, neighbor_rounds: int, sampling_size: int
    ) -> WccResult:
        """Run Weakly Connected Components on this graph."""

class Graph:
    """
    An undirected graph using 32 bits for node ids.
    """

    @staticmethod
    def load(
        path: str, layout: Layout = Layout.Unsorted, file_format=FileFormat.Graph500
    ) -> Graph:
        """Load a graph from the provided format"""
    @staticmethod
    def from_numpy(
        np: npt.NDArray[np.uint32], layout: Layout = Layout.Unsorted
    ) -> Graph:
        """Convert a numpy 2d-array into a graph."""
    @staticmethod
    def from_pandas(df: pd.DataFrame, layout: Layout = Layout.Unsorted) -> Graph:
        """Convert a pandas dataframe into a graph."""
    def node_count(self) -> int:
        """Returns the number of nodes in the graph."""
    def edge_count(self) -> int:
        """Returns the number of edges in the graph."""
    def degree(self, node: int) -> int:
        """Returns the number of edges connected to the given node."""
    def neighbors(self, node: int) -> npt.NDArray[np.uint32]:
        """
        Returns all nodes connected to the given node.

        This functions returns a numpy array that directly references this graph without
        making a copy of the data.
        """
    def copy_neighbors(self, node: int) -> list[int]:
        """
        Returns all nodes connected to the given node.

        This function returns a copy of the data as a Python list.
        """
    def make_degree_ordered(self):
        """
        Converts this graph by relabeling the node ids based on their degree.

        Ids are relabaled using descending degree-order, i.e., given `n` nodes,
        the node with the largest degree will become node id `0`, the node with
        the smallest degree will become node id `n - 1`.

        This modifies the graph in-place.
        The operation can only be done when there are no `neighbors` referenced somewhere.
        """
    def global_triangle_count(self) -> TriangleCountResult:
        """Count the number of global triangles of this graph."""

class PageRankResult:
    def scores(self) -> npt.NDArray[np.float32]:
        pass
    @property
    def ran_iterations(self) -> int:
        pass
    @property
    def error(self) -> float:
        pass
    @property
    def micros(self) -> int:
        pass
    def __repr__(self) -> str:
        pass

class WccResult:
    def components(self) -> npt.NDArray[np.uint32]:
        pass
    @property
    def micros(self) -> int:
        pass
    def __repr__(self) -> str:
        pass

class TriangleCountResult:
    @property
    def triangles(self) -> int:
        pass
    @property
    def micros(self) -> int:
        pass
    def __repr__(self) -> str:
        pass
