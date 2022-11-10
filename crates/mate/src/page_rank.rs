use crate::graphs::SharedSlice;
use graph::prelude::{
    page_rank as graph_page_rank, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait, Idx,
    PageRankConfig,
};
use numpy::PyArray1;
use pyo3::prelude::*;
use std::time::{Duration, Instant};

pub(crate) fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PageRankResult>()?;
    Ok(())
}

pub(crate) fn page_rank<NI, G, C>(py: Python<'_>, graph: &G, config: C) -> PageRankResult
where
    NI: Idx,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    C: Into<Option<PageRankConfig>> + Send,
{
    py.allow_threads(move || inner_page_rank(graph, config))
}

fn inner_page_rank<NI, G>(graph: &G, config: impl Into<Option<PageRankConfig>>) -> PageRankResult
where
    NI: Idx,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let config = config.into().unwrap_or_default();
    let start = Instant::now();
    let (scores, ran_iterations, error) = graph_page_rank(graph, config);
    let micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    let scores = SharedSlice::from_vec(scores);
    PageRankResult {
        scores,
        ran_iterations,
        error,
        micros,
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PageRankResult {
    scores: SharedSlice,
    #[pyo3(get)]
    ran_iterations: usize,
    #[pyo3(get)]
    error: f64,
    #[pyo3(get)]
    micros: u64,
}

impl std::fmt::Debug for PageRankResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageRankResult")
            .field("scores", &format!("[... {} values]", self.scores.len()))
            .field("ran_iterations", &self.ran_iterations)
            .field("error", &self.error)
            .field("took", &Duration::from_micros(self.micros))
            .finish()
    }
}

#[pymethods]
impl PageRankResult {
    pub fn scores<'py>(&self, py: Python<'py>) -> PyResult<&'py PyArray1<f32>> {
        self.scores.clone().into_numpy(py)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
