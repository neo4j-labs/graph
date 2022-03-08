use super::{as_numpy, load_from_py, Layout, NeighborsBuffer, Ungraph};
use crate::pr::PageRankResult;
use graph::prelude::{DirectedCsrGraph, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::{path::PathBuf, sync::Arc, time::Duration};

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Graph>()?;
    Ok(())
}

#[pyclass]
pub struct Graph {
    g: Arc<DirectedCsrGraph<u32>>,
    #[pyo3(get)]
    load_micros: u64,
}

#[pymethods]
impl Graph {
    /// Load a graph in the Graph500 format
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        load_from_py(py, path, layout, |g, took| Self {
            g: Arc::new(g),
            load_micros: took,
        })
    }

    /// Returns the number of nodes in the graph.
    fn node_count(&self) -> u32 {
        self.g.node_count()
    }

    /// Returns the number of edges in the graph.
    fn edge_count(&self) -> u32 {
        self.g.edge_count()
    }

    /// Returns the number of edges where the given node is a source node.
    fn out_degree(&self, node: u32) -> u32 {
        self.g.out_degree(node)
    }

    /// Returns the number of edges where the given node is a target node.
    fn in_degree(&self, node: u32) -> u32 {
        self.g.in_degree(node)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        let buf = NeighborsBuffer::out_neighbors(&self.g, node);
        as_numpy(py, buf)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        let buf = NeighborsBuffer::in_neighbors(&self.g, node);
        as_numpy(py, buf)
    }

    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        PyList::new(py, self.g.out_neighbors(node))
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_in_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        PyList::new(py, self.g.in_neighbors(node))
    }

    fn to_undirected(&self) -> Ungraph {
        let (g, load_micros) = super::timed(self.load_micros, || self.g.to_undirected(None));
        Ungraph::new(g, load_micros)
    }

    /// Run Page Rank on this graph
    fn page_rank(slf: PyRef<Self>, py: Python<'_>) -> PageRankResult {
        crate::pr::page_rank(py, slf)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl Graph {
    pub fn g(&self) -> &DirectedCsrGraph<u32> {
        &self.g
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("node_count", &self.g.node_count())
            .field("edge_count", &self.g.edge_count())
            .field("load_took", &Duration::from_micros(self.load_micros))
            .finish()
    }
}

impl Drop for Graph {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self.g);
        if sc <= 1 {
            log::trace!("dropping graph and releasing all data");
        } else {
            log::trace!("dropping graph, but keeping data around as it is being used by {} neighbor list(s)", sc - 1);
        }
    }
}
