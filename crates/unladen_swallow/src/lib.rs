use std::{
    fmt::Display,
    ops::RangeBounds,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use graph::prelude::{page_rank as graph_page_rank, Error as GError, *};
use pyo3::{
    exceptions::{PyIndexError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyList, PySlice, PySliceIndices},
    PyErrArguments,
};

type GResult<T> = std::result::Result<T, GError>;

pub fn load_graph500<NI: Idx>(path: PathBuf) -> GResult<(DirectedCsrGraph<NI>, u64)> {
    let start = Instant::now();
    let graph = GraphBuilder::new()
        .csr_layout(CsrLayout::Unsorted)
        .file_format(Graph500Input::default())
        .path(path)
        .build()?;
    let graph_load_micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    Ok((graph, graph_load_micros))
}

pub fn run_page_rank<NI: Idx>(
    graph: &DirectedCsrGraph<NI>,
    graph_load_micros: u64,
) -> PageRankResult {
    let config = PageRankConfig::default();
    let start = Instant::now();
    let (scores, ran_iterations, error) = graph_page_rank(graph, config);
    let page_rank_micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    PageRankResult {
        scores: scores.into(),
        ran_iterations,
        error,
        graph_load_micros,
        page_rank_micros,
    }
}

#[derive(Clone)]
#[pyclass]
pub struct PageRankResult {
    scores: Arc<[f32]>,
    #[pyo3(get)]
    ran_iterations: usize,
    #[pyo3(get)]
    error: f64,
    #[pyo3(get)]
    graph_load_micros: u64,
    #[pyo3(get)]
    page_rank_micros: u64,
}

impl std::fmt::Debug for PageRankResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRankResult")
            .field("scores", &format!("[... {} values]", self.scores.len()))
            .field("ran_iterations", &self.ran_iterations)
            .field("error", &self.error)
            .field(
                "took_graph_load",
                &Duration::from_micros(self.graph_load_micros),
            )
            .field(
                "took_page_rank",
                &Duration::from_micros(self.page_rank_micros),
            )
            .finish()
    }
}

fn check_bounds<T, R, I, L>(range: R, index: T, original_index: I, len: L) -> PyResult<()>
where
    T: PartialOrd,
    R: RangeBounds<T>,
    I: Display,
    L: Display,
{
    if range.contains(&index) {
        Ok(())
    } else {
        Err(PyIndexError::new_err(format!(
            "Index '{original_index}' is out of range for this sequence of length '{len}'"
        )))
    }
}

impl PageRankResult {
    fn get_idx(&self, py: Python, idx: isize) -> PyResult<PyObject> {
        let len = self.scores.len() as isize;
        let index = if idx < 0 { len + idx } else { idx };

        check_bounds(0..len, index, idx, len)?;

        let score = self.scores[index as usize];
        Ok(score.to_object(py))
    }

    fn get_slice(&self, py: Python, slice: &PySlice) -> PyResult<PyObject> {
        let len = self.scores.len() as isize;

        let PySliceIndices {
            start, stop, step, ..
        } = slice.indices(len as _)?;

        check_bounds(0..len, start, start, len)?;

        let range = if step >= 0 {
            check_bounds(0..=len, stop, stop, len)?;

            let start = start.unsigned_abs();
            let stop = stop.unsigned_abs().max(start);

            start..stop
        } else {
            check_bounds(-1..len, stop, stop, len)?;

            let original_stop = stop;
            let stop = (start + 1).unsigned_abs();
            let start = (original_stop + 1).unsigned_abs().min(stop - 1);

            start..stop
        };

        let scores = &self.scores[range];

        if step == 1 {
            Ok(scores.to_object(py))
        } else if step == -1 {
            let elements = scores.iter().copied().rev();
            let list = PyList::new(py, elements);
            Ok(PyObject::from(list))
        } else if step > 1 {
            let elements = scores.iter().copied().step_by(step.unsigned_abs());
            let list = PyList::new(py, elements);
            Ok(PyObject::from(list))
        } else {
            let elements = scores.iter().copied().rev().step_by(step.unsigned_abs());
            let list = PyList::new(py, elements);
            Ok(PyObject::from(list))
        }
    }
}

#[pymethods]
impl PageRankResult {
    pub fn score(&self, node_id: usize) -> Option<f32> {
        self.scores.get(node_id).copied()
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __len__(&self) -> usize {
        self.scores.len()
    }

    fn __length_hint__(&self) -> usize {
        self.scores.len()
    }

    fn __contains__(&self, key: usize) -> bool {
        key < self.scores.len()
    }

    fn __getitem__(slf: PyRef<Self>, key: PyObject) -> PyResult<PyObject> {
        if let Ok(idx) = key.extract::<isize>(slf.py()) {
            slf.get_idx(slf.py(), idx)
        } else if let Ok(slice) = key.cast_as::<PySlice>(slf.py()) {
            slf.get_slice(slf.py(), slice)
        } else {
            let tpe = key.as_ref(slf.py()).get_type().name()?;
            Err(PyTypeError::new_err(format!(
                "Invalid type for index key '{tpe}', only int and slice is allowed"
            )))
        }
    }

    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<PageRanksIter>> {
        let iter = PageRanksIter {
            iter: slf.scores.clone(),
            next: 0,
        };
        Py::new(slf.py(), iter)
    }
}

#[pyclass]
pub struct PageRanksIter {
    iter: Arc<[f32]>,
    next: usize,
}

#[pymethods]
impl PageRanksIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<f32> {
        let current = *slf.iter.get(slf.next)?;
        slf.next += 1;
        Some(current)
    }
}

struct GraphError(GError);

impl PyErrArguments for GraphError {
    fn arguments(self, py: Python) -> PyObject {
        self.0.to_string().into_py(py)
    }
}

impl From<GraphError> for PyErr {
    fn from(e: GraphError) -> Self {
        PyValueError::new_err(e)
    }
}

/// Runs Page Rank on a Graph 500 graph
#[pyfunction]
fn page_rank(path: PathBuf) -> PyResult<Py<PageRankResult>> {
    let (graph, took) = load_graph500::<u32>(path).map_err(GraphError)?;
    let res = run_page_rank(&graph, took);
    Python::with_gil(|py| Py::new(py, res))
}

/// Python API for the graph crate
#[pymodule]
fn unladen_swallow(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PageRankResult>()?;
    m.add_function(wrap_pyfunction!(page_rank, m)?)?;
    Ok(())
}
