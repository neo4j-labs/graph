use crate::graphs::{NumpyType, SharedSlice};
use graph::prelude::{
    wcc_afforest as graph_wcc, Components, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait,
    Idx, WccConfig,
};
use numpy::PyArray1;
use pyo3::prelude::*;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

pub(crate) fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<WccResult>()?;
    Ok(())
}

pub(crate) fn wcc<NI, G, C>(py: Python<'_>, graph: &G, config: C) -> WccRes<NI>
where
    NI: Idx + Hash + NumpyType,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
    C: Into<Option<WccConfig>> + Send,
{
    py.allow_threads(move || inner_wcc(graph, config))
}

fn inner_wcc<NI, G>(graph: &G, config: impl Into<Option<WccConfig>>) -> WccRes<NI>
where
    NI: Idx + Hash + NumpyType,
    G: GraphTrait<NI> + DirectedDegrees<NI> + DirectedNeighbors<NI> + Sync,
{
    let config = config.into().unwrap_or_default();
    let start = Instant::now();
    let components = graph_wcc(graph, config).to_vec();
    let micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
    let components = SharedSlice::from_vec(components);
    WccRes {
        components,
        micros,
        _phantom: PhantomData,
    }
}

pub struct WccRes<NI> {
    components: SharedSlice,
    micros: u64,
    _phantom: PhantomData<NI>,
}

#[pyclass]
#[derive(Clone)]
pub struct WccResult {
    components: SharedSlice,
    #[pyo3(get)]
    micros: u64,
}

impl WccResult {
    pub(crate) fn new(result: WccRes<u32>) -> Self {
        Self {
            components: result.components,
            micros: result.micros,
        }
    }
}

impl std::fmt::Debug for WccResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WccResult")
            .field(
                "components",
                &format!("[... {} values]", self.components.len()),
            )
            .field("took", &Duration::from_micros(self.micros))
            .finish()
    }
}

#[pymethods]
impl WccResult {
    pub fn components<'py>(&self, py: Python<'py>) -> PyResult<&'py PyArray1<u32>> {
        self.components.clone().into_numpy(py)
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
