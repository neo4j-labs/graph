use super::{Layout, PyGraph, Ungraph};
use crate::pr::PageRankResult;
use graph::prelude::DirectedCsrGraph;
use numpy::PyArray1;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList},
};
use std::path::PathBuf;

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Graph>()?;
    Ok(())
}

#[pyclass]
pub struct Graph {
    inner: PyGraph<u32, DirectedCsrGraph<u32>>,
    #[pyo3(get)]
    load_micros: u64,
}

impl Graph {
    fn new(load_micros: u64, inner: PyGraph<u32, DirectedCsrGraph<u32>>) -> Self {
        Self { inner, load_micros }
    }
}

#[pymethods]
impl Graph {
    /// Load a graph in the Graph500 format
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        let g = PyGraph::load(py, path, layout)?;
        Ok(Self::new(g.load_micros, g))
    }

    /// Returns the number of nodes in the graph.
    fn node_count(&self) -> u32 {
        self.inner.node_count()
    }

    /// Returns the number of edges in the graph.
    fn edge_count(&self) -> u32 {
        self.inner.edge_count()
    }

    /// Returns the number of edges where the given node is a source node.
    fn out_degree(&self, node: u32) -> u32 {
        self.inner.out_degree(node)
    }

    /// Returns the number of edges where the given node is a target node.
    fn in_degree(&self, node: u32) -> u32 {
        self.inner.in_degree(node)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.out_neighbors(py, node)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.in_neighbors(py, node)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_out_neighbors(py, node)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_in_neighbors(py, node)
    }

    fn __repr__(&self) -> String {
        self.inner.__repr__()
    }

    pub fn to_undirected(&self) -> Ungraph {
        let g = self.inner.to_undirected();
        Ungraph::new(g.load_micros, g)
    }

    /// Run Page Rank on this graph
    #[args(config = "**")]
    pub fn page_rank(slf: PyRef<Self>, config: Option<&PyDict>) -> PyResult<PageRankResult> {
        slf.inner.page_rank(slf.py(), config)
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
