use graph::prelude::{global_triangle_count as tc, Graph as GraphTrait, Idx, UndirectedNeighbors};
use pyo3::prelude::*;
use std::time::{Duration, Instant};

pub(crate) fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<TriangleCountResult>()?;
    Ok(())
}

pub(crate) fn triangle_count<NI, G>(py: Python<'_>, graph: &G) -> TriangleCountResult
where
    NI: Idx,
    G: GraphTrait<NI> + UndirectedNeighbors<NI> + Sync,
{
    py.allow_threads(move || inner_triangle_count(graph))
}

fn inner_triangle_count<NI, G>(graph: &G) -> TriangleCountResult
where
    NI: Idx,
    G: GraphTrait<NI> + UndirectedNeighbors<NI> + Sync,
{
    let start = Instant::now();
    let triangles = tc(graph);
    let micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    TriangleCountResult { triangles, micros }
}

#[pyclass]
#[derive(Clone)]
pub struct TriangleCountResult {
    #[pyo3(get)]
    triangles: u64,
    #[pyo3(get)]
    micros: u64,
}

impl std::fmt::Debug for TriangleCountResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriangleCountResult")
            .field("triangles", &self.triangles)
            .field("took", &Duration::from_micros(self.micros))
            .finish()
    }
}

#[pymethods]
impl TriangleCountResult {
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
