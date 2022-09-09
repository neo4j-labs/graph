use super::{Layout, PyGraph};
use graph::prelude::UndirectedCsrGraph;
use numpy::{PyArray1, PyArray2};
use pyo3::{prelude::*, types::PyList};
use std::path::PathBuf;

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Graph>()?;
    Ok(())
}

/// An undirected graph using 32 bits for node ids.
#[pyclass]
pub struct Graph {
    inner: PyGraph<u32, UndirectedCsrGraph<u32>>,
    #[pyo3(get)]
    load_micros: u64,
}

impl Graph {
    pub(super) fn new(load_micros: u64, inner: PyGraph<u32, UndirectedCsrGraph<u32>>) -> Self {
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

    /// Convert a numpy 2d-array into a graph.
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn from_numpy(np: &PyArray2<u32>, layout: Layout) -> PyResult<Self> {
        let g = PyGraph::from_numpy(np, layout)?;
        Ok(Self::new(g.load_micros, g))
    }

    /// Convert a pandas dataframe into a graph.
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn from_pandas(py: Python<'_>, data: PyObject, layout: Layout) -> PyResult<Self> {
        let g = PyGraph::from_pandas(py, data, layout)?;
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

    /// Returns the number of edges connected to the given node.
    fn degree(&self, node: u32) -> u32 {
        self.inner.degree(node)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.neighbors(py, node)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_neighbors(py, node)
    }

    fn __repr__(&self) -> String {
        self.inner.__repr__()
    }

    /// Creates a new graph by relabeling the node ids of the given graph.
    ///
    /// Ids are relabaled using descending degree-order, i.e., given `n` nodes,
    /// the node with the largest degree will become node id `0`, the node with
    /// the smallest degree will become node id `n - 1`.
    ///
    /// Note, that this method creates a new graph with the same space
    /// requirements as the input graph.
    fn reorder_by_degree(&mut self) -> PyResult<()> {
        self.inner.reorder_by_degree()
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
