use graph::prelude::{
    fast_rp as graph_fast_rp, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait, Idx, FastRPConfig
};
use numpy::{IntoPyArray, PyArray2, ndarray::Array2};
use pyo3::prelude::*;
use std::time::{Duration, Instant};

pub(crate) fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<FastRPResult>()?;
    Ok(())
}

pub(crate) fn fast_rp<NI, G, C>(py: Python<'_>, graph: &G, config: C) -> FastRPResult
where
    NI: Idx,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    C: Into<Option<FastRPConfig>> + Send,
{
    py.allow_threads(move || inner_fast_rp(graph, config))
}

fn inner_fast_rp<NI, G>(graph: &G, config: impl Into<Option<FastRPConfig>>) -> FastRPResult
where
    NI: Idx,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let config = config.into().unwrap_or_default();
    let start = Instant::now();
    let embeddings= graph_fast_rp(graph, config);
    let micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    FastRPResult {
        embeddings,
        micros
    }
}

#[pyclass]
#[derive(Clone)]
pub struct FastRPResult {
    embeddings: Array2<f32>,
    #[pyo3(get)]
    micros: u64,
}

impl std::fmt::Debug for FastRPResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastRPResult")
            .field("embeddings", &"[[...]...]") //todo
            .field("took", &Duration::from_micros(self.micros))
            .finish()
    }
}

#[pymethods]
impl FastRPResult {
    pub fn embeddings<'py>(&self, py: Python<'py>) -> PyResult<&'py PyArray2<f32>> {
        Ok(self.embeddings.clone().into_pyarray(py)) //fixme: don't clone
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
