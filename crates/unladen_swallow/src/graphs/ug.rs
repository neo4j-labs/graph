use super::{as_numpy, load_from_py, Layout, NeighborsBuffer};
use graph::prelude::{
    Graph as GraphTrait, RelabelByDegreeOp, UndirectedCsrGraph, UndirectedDegrees,
    UndirectedNeighbors,
};
use numpy::PyArray1;
use pyo3::prelude::*;
use pyo3::{exceptions::PyValueError, types::PyList};
use std::{path::PathBuf, sync::Arc, time::Duration};

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Ungraph>()?;
    Ok(())
}

#[pyclass]
pub struct Ungraph {
    g: Arc<UndirectedCsrGraph<u32>>,
    #[pyo3(get)]
    load_micros: u64,
}

#[pymethods]
impl Ungraph {
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

    /// Returns the number of edges connected to the given node.
    fn degree(&self, node: u32) -> u32 {
        self.g.degree(node)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    fn neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        let buf = NeighborsBuffer::neighbors(&self.g, node);
        as_numpy(py, buf)
    }

    /// Returns all nodes connected to the given node.
    ///
    /// This function returns a copy of the data as a Python list.
    fn copy_neighbors<'py>(&self, py: Python<'py>, node: u32) -> &'py PyList {
        PyList::new(py, self.g.neighbors(node))
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
        let g = Arc::get_mut(&mut self.g).ok_or_else(|| {
            PyValueError::new_err(concat!(
                "Graph cannot be reordered because there ",
                "are references to this graph from neighbor lists."
            ))
        })?;

        (_, self.load_micros) = super::timed(self.load_micros, || g.to_degree_ordered());

        Ok(())
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl Ungraph {
    pub fn g(&self) -> &UndirectedCsrGraph<u32> {
        &self.g
    }

    pub(crate) fn new(g: UndirectedCsrGraph<u32>, load_micros: u64) -> Self {
        Self {
            g: Arc::new(g),
            load_micros,
        }
    }
}

impl std::fmt::Debug for Ungraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ungraph")
            .field("node_count", &self.g.node_count())
            .field("edge_count", &self.g.edge_count())
            .field("load_took", &Duration::from_micros(self.load_micros))
            .finish()
    }
}

impl Drop for Ungraph {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self.g);
        if sc <= 1 {
            log::trace!("dropping graph and releasing all data");
        } else {
            log::trace!("dropping graph, but keeping data around as it is being used by {} neighbor list(s)", sc - 1);
        }
    }
}
