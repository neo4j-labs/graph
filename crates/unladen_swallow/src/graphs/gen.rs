use super::{load_from_py, Layout, NumpyType, SharedSlice};
use crate::pr::PageRankResult;
use graph::prelude::{
    CsrLayout, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait, Graph500, Idx,
    RelabelByDegreeOp, ToUndirectedOp, UndirectedDegrees, UndirectedNeighbors,
};
use numpy::PyArray1;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyList};
use std::{marker::PhantomData, path::PathBuf, sync::Arc, time::Duration};

pub(crate) struct PyGraph<NI, G> {
    g: Arc<G>,
    pub(crate) load_micros: u64,
    _ni: PhantomData<NI>,
}

impl<NI, G> PyGraph<NI, G> {
    pub(crate) fn new(load_micros: u64, g: G) -> Self {
        Self {
            g: Arc::new(g),
            load_micros,
            _ni: PhantomData,
        }
    }

    pub(crate) fn g(&self) -> &G {
        &*self.g
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
    G: TryFrom<(Graph500<NI>, CsrLayout)> + Send,
    graph::prelude::Error: From<G::Error>,
{
    /// Load a graph in the Graph500 format
    pub(crate) fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        load_from_py(py, path, layout, |g, took| Self::new(took, g))
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
    G: GraphTrait<NI>,
{
    /// Returns the number of nodes in the graph.
    pub(crate) fn node_count(&self) -> NI {
        self.g.node_count()
    }

    /// Returns the number of edges in the graph.
    pub(crate) fn edge_count(&self) -> NI {
        self.g.edge_count()
    }

    /// Returns the number of edges where the given node is a source node.
    pub(crate) fn out_degree(&self, node: NI) -> NI
    where
        G: DirectedDegrees<NI>,
    {
        self.g.out_degree(node)
    }

    /// Returns the number of edges where the given node is a target node.
    pub(crate) fn in_degree(&self, node: NI) -> NI
    where
        G: DirectedDegrees<NI>,
    {
        self.g.in_degree(node)
    }

    /// Returns the number of edges for the given node
    pub(crate) fn degree(&self, node: NI) -> NI
    where
        G: UndirectedDegrees<NI>,
    {
        self.g.degree(node)
    }

    pub(crate) fn to_undirected(&self) -> PyGraph<NI, G::Undirected>
    where
        G: ToUndirectedOp,
        G::Undirected: GraphTrait<NI>,
    {
        let (g, load_micros) = super::timed(self.load_micros, || self.g.to_undirected(None));
        PyGraph {
            g: Arc::new(g),
            load_micros,
            _ni: PhantomData,
        }
    }

    /// Creates a new graph by relabeling the node ids of the given graph.
    ///
    /// Ids are relabaled using descending degree-order, i.e., given `n` nodes,
    /// the node with the largest degree will become node id `0`, the node with
    /// the smallest degree will become node id `n - 1`.
    ///
    /// Note, that this method creates a new graph with the same space
    /// requirements as the input graph.
    pub(crate) fn reorder_by_degree<EV>(&mut self) -> PyResult<()>
    where
        G: RelabelByDegreeOp<NI, EV>,
    {
        let g = Arc::get_mut(&mut self.g).ok_or_else(|| {
            PyValueError::new_err(concat!(
                "Graph cannot be reordered because there ",
                "are references to this graph from neighbor lists."
            ))
        })?;

        (_, self.load_micros) = super::timed(self.load_micros, || g.to_degree_ordered());
        Ok(())
    }

    /// Run Page Rank on this graph
    pub(crate) fn page_rank(&self, py: Python<'_>) -> PageRankResult
    where
        G: DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    {
        crate::pr::page_rank(py, self.g())
    }

    pub(crate) fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: NumpyType,
    G: GraphTrait<NI> + Send + Sync + 'static,
{
    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub(crate) fn out_neighbors<'py>(
        &self,
        py: Python<'py>,
        node: NI,
    ) -> PyResult<&'py PyArray1<NI>>
    where
        G: DirectedNeighbors<NI>,
    {
        let buf = SharedSlice::out_neighbors(&self.g, node);
        buf.into_numpy(py)
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of the connecting edge.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub(crate) fn in_neighbors<'py>(&self, py: Python<'py>, node: NI) -> PyResult<&'py PyArray1<NI>>
    where
        G: DirectedNeighbors<NI>,
    {
        let buf = SharedSlice::in_neighbors(&self.g, node);
        buf.into_numpy(py)
    }

    /// Returns all nodes which are connected to the given node.
    ///
    /// This functions returns a numpy array that directly references this graph without
    /// making a copy of the data.
    pub(crate) fn neighbors<'py>(&self, py: Python<'py>, node: NI) -> PyResult<&'py PyArray1<NI>>
    where
        G: UndirectedNeighbors<NI>,
    {
        let buf = SharedSlice::neighbors(&self.g, node);
        buf.into_numpy(py)
    }
}

impl<NI, G> PyGraph<NI, G>
where
    NI: Idx + ToPyObject,
    G: GraphTrait<NI>,
{
    /// Returns all nodes which are connected in outgoing direction to the given node,
    /// i.e., the given node is the source node of the connecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    pub(crate) fn copy_out_neighbors<'py>(&self, py: Python<'py>, node: NI) -> &'py PyList
    where
        G: DirectedNeighbors<NI>,
    {
        PyList::new(py, self.g.out_neighbors(node))
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    pub(crate) fn copy_in_neighbors<'py>(&self, py: Python<'py>, node: NI) -> &'py PyList
    where
        G: DirectedNeighbors<NI>,
    {
        PyList::new(py, self.g.in_neighbors(node))
    }

    /// Returns all nodes which are connected in incoming direction to the given node,
    /// i.e., the given node is the target node of theconnecting edge.
    ///
    /// This function returns a copy of the data as a Python list.
    pub(crate) fn copy_neighbors<'py>(&self, py: Python<'py>, node: NI) -> &'py PyList
    where
        G: UndirectedNeighbors<NI>,
    {
        PyList::new(py, self.g.neighbors(node))
    }
}

impl<NI: Idx, G: GraphTrait<NI>> std::fmt::Debug for PyGraph<NI, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("node_count", &self.g.node_count())
            .field("edge_count", &self.g.edge_count())
            .field("load_took", &Duration::from_micros(self.load_micros))
            .finish()
    }
}

impl<NI, G> Drop for PyGraph<NI, G> {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self.g);
        if sc <= 1 {
            log::trace!("dropping graph and releasing all data");
        } else {
            log::trace!("dropping graph, but keeping data around as it is being used by {} neighbor list(s)", sc - 1);
        }
    }
}
