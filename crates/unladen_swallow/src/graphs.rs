use crate::{GResult, GraphError};
use graph::prelude::{CsrLayout, Graph500, Graph500Input, GraphBuilder, Idx};
use pyo3::prelude::*;
use std::{fmt::Debug, path::PathBuf, time::Instant};

mod g;
mod gen;
mod shared_slice;
mod ug;

// pub(crate) use self::g::Graph;
pub(crate) use self::gen::PyGraph;
pub(crate) use self::shared_slice::{NumpyType, SharedSlice};
pub(crate) use self::ug::Ungraph;

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

pub(crate) fn register(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Layout>()?;

    g::register(py, m)?;
    ug::register(py, m)?;

    Ok(())
}

fn load_from_py<NI, G, T, F>(py: Python<'_>, path: PathBuf, layout: Layout, mk: F) -> PyResult<T>
where
    NI: Idx,
    G: TryFrom<(Graph500<NI>, CsrLayout)> + Send,
    graph::prelude::Error: From<G::Error>,
    F: FnOnce(G, u64) -> T,
{
    fn load_graph500<NI, G>(path: PathBuf, layout: CsrLayout) -> GResult<(G, u64)>
    where
        NI: Idx,
        G: TryFrom<(Graph500<NI>, CsrLayout)>,
        graph::prelude::Error: From<G::Error>,
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
        .map_err(GraphError)?;

    Ok(mk(graph, took))
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
