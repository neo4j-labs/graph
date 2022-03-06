use super::{GResult, GraphError};
use crate::pr::PageRankResult;
use graph::prelude::{
    CsrLayout, DirectedCsrGraph, Graph as GraphTrait, Graph500Input, GraphBuilder,
};
use pyo3::prelude::*;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Layout>()?;
    m.add_class::<Graph>()?;
    Ok(())
}

/// Defines how the neighbor list of individual nodes are organized within the
/// CSR target array.
#[derive(Clone, Copy, Debug)]
#[pyclass]
pub enum Layout {
    /// Neighbor lists are sorted and may contain duplicate target ids. This is
    /// the default representation.
    Sorted,
    /// Neighbor lists are not in any particular order.
    Unsorted,
    /// Neighbor lists are sorted and do not contain duplicate target ids.
    /// Self-loops, i.e., edges in the form of `(u, u)` are removed.
    Deduplicated,
}

#[pyclass]
pub struct Graph {
    g: DirectedCsrGraph<u32>,
    #[pyo3(get)]
    load_micros: u64,
}

#[pymethods]
impl Graph {
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        let layout = match layout {
            Layout::Sorted => CsrLayout::Sorted,
            Layout::Unsorted => CsrLayout::Unsorted,
            Layout::Deduplicated => CsrLayout::Deduplicated,
        };
        let graph = py
            .allow_threads(move || Self::load_graph500(path, layout))
            .map_err(GraphError)?;
        Ok(graph)
    }

    pub fn page_rank(slf: PyRef<Self>, py: Python<'_>) -> PageRankResult {
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

    fn load_graph500(path: PathBuf, layout: CsrLayout) -> GResult<Self> {
        let start = Instant::now();
        let graph = GraphBuilder::new()
            .csr_layout(layout)
            .file_format(Graph500Input::default())
            .path(path)
            .build()?;
        let load_micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
        Ok(Self {
            g: graph,
            load_micros,
        })
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
