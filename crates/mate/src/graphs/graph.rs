use super::{FileFormat, Layout, PyGraph};
use crate::triangle_count::TriangleCountResult;
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
    /// Load a graph in the provided format
    #[staticmethod]
    #[args(layout = "None", file_format = "FileFormat::Graph500")]
    pub fn load(
        py: Python<'_>,
        path: PathBuf,
        layout: Option<Layout>,
        file_format: FileFormat,
    ) -> PyResult<Self> {
        let g = PyGraph::load_file(py, path, layout, file_format)?;
        Ok(Self::new(g.load_micros, g))
    }

    /// Convert a numpy 2d-array into a graph.
    #[staticmethod]
    #[args(layout = "None")]
    pub fn from_numpy(np: &PyArray2<u32>, layout: Option<Layout>) -> PyResult<Self> {
        let g = PyGraph::from_numpy(np, layout)?;
        Ok(Self::new(g.load_micros, g))
    }

    /// Convert a pandas dataframe into a graph.
    #[staticmethod]
    #[args(layout = "None")]
    pub fn from_pandas(py: Python<'_>, data: PyObject, layout: Option<Layout>) -> PyResult<Self> {
        let g = PyGraph::from_pandas(py, data, layout)?;
        Ok(Self::new(g.load_micros, g))
    }

    /// Returns the number of nodes in the graph.
    pub fn node_count(&self) -> u32 {
        self.inner.node_count()
    }

    /// Returns the number of edges in the graph.
    pub fn edge_count(&self) -> u32 {
        self.inner.edge_count()
    }

    /// Returns the number of edges connected to the given node.
    pub fn degree(&self, node: u32) -> u32 {
        self.inner.degree(node)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub fn neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.neighbors(py, node)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This function returns a copy of the data as a Python list.
    pub fn copy_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_neighbors(py, node)
    }

    pub fn __repr__(&self) -> String {
        self.inner.__repr__()
    }

    /// Converts this graph by relabeling the node ids based on their degree.
    ///
    /// Ids are relabaled using descending degree-order, i.e., given `n` nodes,
    /// the node with the largest degree will become node id `0`, the node with
    /// the smallest degree will become node id `n - 1`.
    ///
    /// This modifies the graph in-place.
    /// The operation can only be done when there are no `neighbors` referenced somewhere.
    pub fn make_degree_ordered(&mut self) -> PyResult<()> {
        self.inner.make_degree_ordered()
    }

    /// Count the number of global triangles of this graph.
    pub fn global_triangle_count(&self, py: Python<'_>) -> TriangleCountResult {
        crate::triangle_count::triangle_count(py, self.inner.g())
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
