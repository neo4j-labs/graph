use crate::{GResult, GraphError as GraphErrorWrapper};
use ::graph::prelude::{
    CsrLayout, DirectedDegrees, DirectedNeighbors, Error as GraphError, Graph as GraphTrait,
    Graph500, Graph500Input, GraphBuilder, Idx, RelabelByDegreeOp, ToUndirectedOp,
    UndirectedDegrees, UndirectedNeighbors,
};
use numpy::PyArray1;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyList};
use std::{
    fmt::Debug,
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

mod digraph;
mod graph;
mod shared_slice;

pub(crate) use self::graph::Graph;
pub(crate) use self::shared_slice::{NumpyType, SharedSlice};

pub(crate) fn register(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Layout>()?;

    digraph::register(py, m)?;
    graph::register(py, m)?;

    Ok(())
}

/// Defines how the neighbor list of individual nodes are organized within the
/// CSR target array.
#[derive(Clone, Copy, Debug)]
#[pyclass]
pub enum Layout {
    /// Neighbor lists are sorted and may contain duplicate target ids.
    Sorted,
    /// Neighbor lists are not in any particular order.
    /// This is the default representation.
    Unsorted,
    /// Neighbor lists are sorted and do not contain duplicate target ids.
    /// Self-loops, i.e., edges in the form of `(u, u)` are removed.
    Deduplicated,
}

/// A generic implementation of a python wrapper around a graph
///
/// pyclasses cannot be generic, so for any concrete graph that we want to expose
/// we need to add a new type, that wraps this graph with concrete type arguments
struct PyGraph<NI, G> {
    g: Arc<G>,
    load_micros: u64,
    _ni: PhantomData<NI>,
}

impl<NI, G> PyGraph<NI, G> {
    fn new(load_micros: u64, g: G) -> Self {
        Self {
            g: Arc::new(g),
            load_micros,
            _ni: PhantomData,
        }
    }

    fn g(&self) -> &G {
        &*self.g
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
    G: TryFrom<(Graph500<NI>, CsrLayout)> + Send,
    GraphError: From<G::Error>,
{
    /// Load a graph in the Graph500 format
    fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        fn load_graph500<NI, G>(path: PathBuf, layout: CsrLayout) -> GResult<(G, u64)>
        where
            NI: Idx,
            G: TryFrom<(Graph500<NI>, CsrLayout)>,
            GraphError: From<G::Error>,
        {
            let (graph, load_micros) = time(move || {
                GraphBuilder::new()
                    .csr_layout(layout)
                    .file_format(Graph500Input::default())
                    .path(path)
                    .build()
            });
            let graph = graph?;

            Ok((graph, load_micros))
        }

        let layout = match layout {
            Layout::Sorted => CsrLayout::Sorted,
            Layout::Unsorted => CsrLayout::Unsorted,
            Layout::Deduplicated => CsrLayout::Deduplicated,
        };
        let (graph, took) = py
            .allow_threads(move || load_graph500(path, layout))
            .map_err(GraphErrorWrapper)?;

        Ok(Self::new(took, graph))
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
    G: GraphTrait<NI>,
{
    /// Returns the number of nodes in the graph.
    fn node_count(&self) -> NI {
        self.g.node_count()
    }

    /// Returns the number of edges in the graph.
    fn edge_count(&self) -> NI {
        self.g.edge_count()
    }

    /// Returns the number of edges where the given node is a source node.
    fn out_degree(&self, node: NI) -> NI
    where
        G: DirectedDegrees<NI>,
    {
        self.g.out_degree(node)
    }

    /// Returns the number of edges where the given node is a target node.
    fn in_degree(&self, node: NI) -> NI
    where
        G: DirectedDegrees<NI>,
    {
        self.g.in_degree(node)
    }

    /// Returns the number of edges for the given node
    fn degree(&self, node: NI) -> NI
    where
        G: UndirectedDegrees<NI>,
    {
        self.g.degree(node)
    }

    fn to_undirected(&self) -> PyGraph<NI, G::Undirected>
    where
        G: ToUndirectedOp,
        G::Undirected: GraphTrait<NI>,
    {
        let (g, load_micros) = timed(self.load_micros, || self.g.to_undirected(None));
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
    fn reorder_by_degree<EV>(&mut self) -> PyResult<()>
    where
        G: RelabelByDegreeOp<NI, EV>,
    {
        let g = Arc::get_mut(&mut self.g).ok_or_else(|| {
            PyValueError::new_err(concat!(
                "Graph cannot be reordered because there ",
                "are references to this graph from neighbor lists."
            ))
        })?;

        (_, self.load_micros) = timed(self.load_micros, || g.to_degree_ordered());
        Ok(())
    }

    fn __repr__(&self) -> String {
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
        for<'a> G: DirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>,
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
        for<'a> G: DirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>,
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
        for<'a> G: UndirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>,
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
        for<'a> G::NeighborsIterator<'a>: ExactSizeIterator,
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
        for<'a> G::NeighborsIterator<'a>: ExactSizeIterator,
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
        for<'a> G::NeighborsIterator<'a>: ExactSizeIterator,
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

fn time<R, F>(f: F) -> (R, u64)
where
    F: FnOnce() -> R,
{
    run_with_timing::<R, F, u8, _>(f, None)
}

fn timed<T, R, F>(prev: T, f: F) -> (R, u64)
where
    F: FnOnce() -> R,
    u128: From<T>,
{
    run_with_timing::<R, F, T, _>(f, Some(prev))
}

fn run_with_timing<R, F, T, U>(f: F, prev: U) -> (R, u64)
where
    F: FnOnce() -> R,
    u128: From<T>,
    U: Into<Option<T>>,
{
    let prev: Option<T> = prev.into();
    let prev = prev.map_or(0, u128::from);

    let start = Instant::now();
    let result = f();

    let micros = start.elapsed().as_micros();
    let micros = micros + prev;
    let micros = micros.min(u64::MAX as _) as u64;

    (result, micros)
}
