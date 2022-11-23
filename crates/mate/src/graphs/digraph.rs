use super::{FileFormat, Graph, Layout, PyGraph};
use crate::{page_rank::PageRankResult, wcc::WccResult};
use graph::{
    page_rank::PageRankConfig,
    prelude::{CsrLayout, DirectedCsrGraph},
    wcc::WccConfig,
};
use numpy::{PyArray1, PyArray2};
use pyo3::{prelude::*, types::PyList};
use std::path::PathBuf;

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DiGraph>()?;
    Ok(())
}

#[pyclass]
pub struct DiGraph {
    inner: PyGraph<u32, DirectedCsrGraph<u32>>,
    #[pyo3(get)]
    load_micros: u64,
}

impl DiGraph {
    fn new(load_micros: u64, inner: PyGraph<u32, DirectedCsrGraph<u32>>) -> Self {
        Self { inner, load_micros }
    }
}

/// A directed graph using 32 bits for node ids.
#[pymethods]
impl DiGraph {
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

    /// Returns the number of edges where the given node is a source node.
    pub fn out_degree(&self, node: u32) -> u32 {
        self.inner.out_degree(node)
    }

    /// Returns the number of edges where the given node is a target node.
    pub fn in_degree(&self, node: u32) -> u32 {
        self.inner.in_degree(node)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub fn out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.out_neighbors(py, node)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub fn in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        self.inner.in_neighbors(py, node)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    pub fn copy_out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_out_neighbors(py, node)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    pub fn copy_in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        self.inner.copy_in_neighbors(py, node)
    }

    pub fn __repr__(&self) -> String {
        self.inner.__repr__()
    }

    #[args(layout = "None")]
    pub fn to_undirected(&self, layout: Option<Layout>) -> Graph {
        let g = self.inner.to_undirected(layout.map(CsrLayout::from));
        Graph::new(g.load_micros, g)
    }

    /// Run Page Rank on this graph.
    #[args(
        "*",
        max_iterations = "PageRankConfig::DEFAULT_MAX_ITERATIONS",
        tolerance = "PageRankConfig::DEFAULT_TOLERANCE",
        damping_factor = "PageRankConfig::DEFAULT_DAMPING_FACTOR"
    )]
    pub fn page_rank(
        &self,
        py: Python<'_>,
        max_iterations: usize,
        tolerance: f64,
        damping_factor: f32,
    ) -> PageRankResult {
        let config = PageRankConfig::new(max_iterations, tolerance, damping_factor);
        crate::page_rank::page_rank(py, self.inner.g(), config)
    }

    /// Run Weakly Connected Compontents on this graph.
    #[args(
        "*",
        chunk_size = "WccConfig::DEFAULT_CHUNK_SIZE",
        neighbor_rounds = "WccConfig::DEFAULT_NEIGHBOR_ROUNDS",
        sampling_size = "WccConfig::DEFAULT_SAMPLING_SIZE"
    )]
    pub fn wcc(
        &self,
        py: Python<'_>,
        chunk_size: usize,
        neighbor_rounds: usize,
        sampling_size: usize,
    ) -> WccResult {
        let config = WccConfig::new(chunk_size, neighbor_rounds, sampling_size);
        WccResult::new(crate::wcc::wcc(py, self.inner.g(), config))
    }
}

impl std::fmt::Debug for DiGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}
