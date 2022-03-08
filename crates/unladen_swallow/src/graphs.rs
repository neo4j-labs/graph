use crate::{GResult, GraphError};
use graph::prelude::{
    CsrLayout, DirectedCsrGraph, DirectedNeighbors, Graph500, Graph500Input, GraphBuilder,
    UndirectedCsrGraph, UndirectedNeighbors,
};
use numpy::{
    npyffi::{types::NPY_TYPES, NpyTypes, NPY_ARRAY_DEFAULT, NPY_ARRAY_WRITEABLE},
    PyArray, PyArray1, PY_ARRAY_API,
};
use pyo3::{prelude::*, types::PyCapsule};
use std::{
    any::Any, ffi::CStr, fmt::Debug, os::raw::c_void, path::PathBuf, sync::Arc, time::Instant,
};

mod g;
mod ug;

pub(crate) use g::Graph;
pub(crate) use ug::Ungraph;

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

fn load_from_py<G, T, F>(py: Python<'_>, path: PathBuf, layout: Layout, mk: F) -> PyResult<T>
where
    G: TryFrom<(Graph500<u32>, CsrLayout)> + Send,
    graph::prelude::Error: From<G::Error>,
    F: FnOnce(G, u64) -> T,
{
    fn load_graph500<G>(path: PathBuf, layout: CsrLayout) -> GResult<(G, u64)>
    where
        G: TryFrom<(Graph500<u32>, CsrLayout)>,
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

fn as_numpy(py: Python<'_>, mut buf: NeighborsBuffer) -> PyResult<&PyArray1<u32>> {
    // Super class-ish of new array, this type creates a base array
    let base_type = unsafe { PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type) };
    // Type of a single element, here Uint = u32
    let element_type = unsafe { PY_ARRAY_API.PyArray_DescrFromType(py, NPY_TYPES::NPY_UINT as _) };
    // 1-D array
    let ndims = 1;
    // One dim with the len in number of elements
    let dims = std::slice::from_mut(&mut buf.len).as_mut_ptr().cast();
    // No strides required, can be NULL for 1-D arrays
    let strides = std::ptr::null_mut();
    // Owning data of the buffer (this is us)
    let data = buf.data.as_ffi();
    // Mark it as readonly
    let flags = NPY_ARRAY_DEFAULT & !NPY_ARRAY_WRITEABLE;
    // Protoype object - we don't have any so it's NULL
    let obj = std::ptr::null_mut();

    // Create the actual array
    let arr = unsafe {
        PY_ARRAY_API.PyArray_NewFromDescr(
            py,
            base_type,
            element_type,
            ndims,
            dims,
            strides,
            data,
            flags,
            obj,
        )
    };

    // In order to get numpy to run *our* destructor, we need to wrap in a capsule and
    let capsule = PyCapsule::new_with_destructor(
        py,
        buf,
        // SAFETY: byte string literal ends in a NULL byte
        unsafe { CStr::from_bytes_with_nul_unchecked(b"__graph_neighbors_buf__\0") },
        |b, _| drop(b),
    )?;

    // add the capsule as base object so that it will be freed
    unsafe {
        PY_ARRAY_API.PyArray_SetBaseObject(py, arr.cast(), capsule.into_ptr());
    }

    unsafe { Ok(PyArray::from_owned_ptr(py, arr)) }
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

#[pyclass]
pub struct NeighborsBuffer {
    data: SharedConst,
    len: usize,
    _g: Arc<dyn Any + Send + Sync>,
}

impl NeighborsBuffer {
    pub fn out_neighbors(g: &Arc<DirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.out_neighbors(node);
        Self {
            data: SharedConst(data.as_ptr()),
            len: data.len(),
            _g: g,
        }
    }

    pub fn in_neighbors(g: &Arc<DirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.in_neighbors(node);
        Self {
            data: SharedConst(data.as_ptr()),
            len: data.len(),
            _g: g,
        }
    }

    pub fn neighbors(g: &Arc<UndirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.neighbors(node);
        Self {
            data: SharedConst(data.as_ptr()),
            len: data.len(),
            _g: g,
        }
    }
}

impl Debug for NeighborsBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeighborsBuffer")
            .field("data", &self.data.0)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for NeighborsBuffer {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self._g);
        if sc <= 1 {
            log::trace!(
                "dropping last neighbors list, graph was already dropped so will release all data"
            );
        } else if sc == 2 {
            log::trace!("dropping last neighbors list, but graph is still alive");
        } else {
            log::trace!(
                "dropping neighbors list, there are still {} other neighbor list(s) around",
                sc - 2
            );
        }
    }
}

struct SharedConst(*const u32);

impl SharedConst {
    fn as_ffi(&self) -> *mut c_void {
        self.0 as _
    }
}

unsafe impl Send for SharedConst {}
