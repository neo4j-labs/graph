use crate::GraphError as GraphErrorWrapper;
use ::graph::prelude::{
    CsrLayout, DirectedDegrees, DirectedNeighbors, EdgeList, EdgeListInput, Edges,
    Error as GraphError, Graph as GraphTrait, Graph500, Graph500Input, GraphBuilder, Idx,
    InputCapabilities, InputPath, RelabelByDegreeOp, ToUndirectedOp, UndirectedDegrees,
    UndirectedNeighbors,
};
use numpy::{
    ndarray::{iter::AxisIter, ArrayView2, Ix1},
    Element, PyArray1, PyArray2,
};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::PyList,
};
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
    m.add_class::<FileFormat>()?;

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

/// Defines the file format of an input file.
#[derive(Clone, Copy, Debug)]
#[pyclass]
pub enum FileFormat {
    /// The input in a binary Graph500 format.
    Graph500,
    /// The input is a text file where each line represents an edge in the form
    /// of `<source_id> <target_id>`.
    EdgeList,
}

impl From<Layout> for CsrLayout {
    fn from(layout: Layout) -> Self {
        match layout {
            Layout::Sorted => CsrLayout::Sorted,
            Layout::Unsorted => CsrLayout::Unsorted,
            Layout::Deduplicated => CsrLayout::Deduplicated,
        }
    }
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
        &self.g
    }
}

impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
{
    /// Load a graph in the provided format
    fn load_file(
        py: Python<'_>,
        path: PathBuf,
        layout: Option<Layout>,
        file_format: FileFormat,
    ) -> PyResult<Self>
    where
        G: TryFrom<(EdgeList<NI, ()>, CsrLayout)> + TryFrom<(Graph500<NI>, CsrLayout)> + Send,
        GraphError: From<<EdgeList<NI, ()> as TryFrom<InputPath<PathBuf>>>::Error>,
        GraphError: From<<Graph500<NI> as TryFrom<InputPath<PathBuf>>>::Error>,
        GraphError: From<<G as TryFrom<(EdgeList<NI, ()>, CsrLayout)>>::Error>,
        GraphError: From<<G as TryFrom<(Graph500<NI>, CsrLayout)>>::Error>,
    {
        match file_format {
            FileFormat::Graph500 => {
                Self::load_file_input(py, path, layout, Graph500Input::default())
            }
            FileFormat::EdgeList => {
                Self::load_file_input(py, path, layout, EdgeListInput::default())
            }
        }
    }

    /// Load a graph in the provided format
    fn load_file_input<Format>(
        py: Python<'_>,
        path: PathBuf,
        layout: Option<Layout>,
        format: Format,
    ) -> PyResult<Self>
    where
        Format: InputCapabilities<NI> + Send,
        Format::GraphInput: TryFrom<InputPath<PathBuf>>,
        G: TryFrom<(Format::GraphInput, CsrLayout)> + Send,
        GraphError: From<<Format::GraphInput as TryFrom<InputPath<PathBuf>>>::Error>,
        GraphError: From<G::Error>,
    {
        let (graph, took) = py
            .allow_threads(move || {
                let (graph, load_micros) = time(move || {
                    let mut b = GraphBuilder::new();
                    if let Some(layout) = layout {
                        b = b.csr_layout(CsrLayout::from(layout));
                    }

                    b.file_format(format).path(path).build()
                });
                let graph = graph?;

                Ok((graph, load_micros))
            })
            .map_err(GraphErrorWrapper)?;

        Ok(Self::new(took, graph))
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: Idx,
{
    fn from_numpy(np: &PyArray2<NI>, layout: Option<Layout>) -> PyResult<Self>
    where
        NI: Element,
        for<'a> G: From<(ArrayEdgeList<'a, NI>, CsrLayout)>,
    {
        let np = np.readonly();
        let np = np.as_array();
        let el = ArrayEdgeList::new(np)?;
        Ok(Self::from_edge_list(el, layout))
    }

    fn from_pandas(py: Python<'_>, data: PyObject, layout: Option<Layout>) -> PyResult<Self>
    where
        NI: Element,
        for<'a> G: From<(ArrayEdgeList<'a, NI>, CsrLayout)>,
    {
        let to_numpy = data.getattr(py, "to_numpy")?;
        let np = to_numpy.call0(py)?;
        let np = unsafe { PyArray2::from_owned_ptr(py, np.into_ptr()) };
        Self::from_numpy(np, layout)
    }

    /// Load a graph from an edge list
    fn from_edge_list<E>(edge_list: E, layout: Option<Layout>) -> Self
    where
        E: Edges<NI = NI>,
        G: From<(E, CsrLayout)>,
    {
        let (graph, took) =
            time(move || G::from((edge_list, layout.map(CsrLayout::from).unwrap_or_default())));
        Self::new(took, graph)
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

    fn to_undirected(&self, layout: impl Into<Option<CsrLayout>>) -> PyGraph<NI, G::Undirected>
    where
        G: ToUndirectedOp,
        G::Undirected: GraphTrait<NI>,
    {
        let (g, load_micros) = timed(self.load_micros, move || self.g.to_undirected(layout));
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
    fn make_degree_ordered<EV>(&mut self) -> PyResult<()>
    where
        G: RelabelByDegreeOp<NI, EV>,
    {
        let g = Arc::get_mut(&mut self.g).ok_or_else(|| {
            PyValueError::new_err(concat!(
                "Graph cannot be reordered because there ",
                "are references to this graph from neighbor lists."
            ))
        })?;

        (_, self.load_micros) = timed(self.load_micros, || g.make_degree_ordered());
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

/// pymethods
impl<NI, G> PyGraph<NI, G>
where
    NI: NumpyType + Idx,
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
        match Arc::strong_count(&self.g) {
            0..=1 => log::trace!("dropping graph and releasing all data"),
            2 => log::trace!(
                "dropping graph, but keeping data around as it is being used by 1 neighbors array"
            ),
            count => log::trace!(
                "dropping graph, but keeping data around as it is being used by {} neighbor arrays",
                count - 1
            ),
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

struct ArrayEdgeList<'a, T> {
    array: ArrayView2<'a, T>,
    #[allow(unused)]
    edge_count: usize,
}

impl<'a, T> ArrayEdgeList<'a, T> {
    fn new(array: ArrayView2<'a, T>) -> PyResult<Self> {
        match array.shape() {
            &[edge_count, row_len] if row_len >= 2 => Ok(Self { array, edge_count }),
            _ => Err(PyTypeError::new_err(
                "Can only create a graph from a 2-dimensional array with at least 2 columns",
            )),
        }
    }
}

struct ArrayRows<'a, T>(AxisIter<'a, T, Ix1>);

impl<'a, T: Copy + Debug> Iterator for ArrayRows<'a, T> {
    type Item = (T, T, ());

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.0.next()?;
        Some((row[0], row[1], ()))
    }
}

impl<'outer, T: Idx> Edges for ArrayEdgeList<'outer, T> {
    type NI = T;

    type EV = ();

    type EdgeIter<'a> = rayon::iter::IterBridge<ArrayRows<'a, T>>
    where
        Self: 'a;

    fn edges(&self) -> Self::EdgeIter<'_> {
        use rayon::iter::ParallelBridge;
        ArrayRows(self.array.outer_iter()).par_bridge()
    }
}
