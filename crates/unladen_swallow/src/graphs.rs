use crate::{GResult, GraphError};
use graph::prelude::{
    CsrLayout, DirectedCsrGraph, DirectedNeighbors, Graph500, Graph500Input, GraphBuilder,
    UndirectedCsrGraph, UndirectedNeighbors,
};
use numpy::{npyffi::types::NPY_TYPES, PyArray, PyArray1, PY_ARRAY_API};
#[allow(deprecated)]
use pyo3::{
    class::buffer::PyBufferProtocol, exceptions::PyBufferError, ffi, prelude::*, AsPyPointer,
};
use std::{
    any::Any, ffi::CStr, fmt::Debug, os::raw::c_int, path::PathBuf, sync::Arc, time::Instant,
};

mod g;
mod ug;

pub(crate) use g::Graph;
#[allow(unused)]
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
    m.add_function(wrap_pyfunction!(show_nb, m)?)?;

    g::register(py, m)?;
    ug::register(py, m)?;

    Ok(())
}

#[pyfunction]
pub fn show_nb(py: Python<'_>, obj: PyObject) -> PyResult<String> {
    let vu: PyRef<NeighborsBuffer> = obj.extract(py)?;
    Ok(format!("very unsafe: pyobj {obj:?}, vu {vu:?}"))
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
        let start = Instant::now();
        let graph = GraphBuilder::new()
            .csr_layout(layout)
            .file_format(Graph500Input::default())
            .path(path)
            .build()?;
        let load_micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
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

fn as_numpy(py: Python<'_>, buf: NeighborsBuffer) -> PyResult<&PyArray1<u32>> {
    let buf = PyCell::new(py, buf)?;
    let buf = buf.into_ptr();

    unsafe {
        let tpe = PY_ARRAY_API.PyArray_DescrFromType(py, NPY_TYPES::NPY_UINT as _);
        let ptr = PY_ARRAY_API.PyArray_FromBuffer(py, buf, tpe, -1, 0);
        Ok(PyArray::from_owned_ptr(py, ptr))
    }
}

#[pyclass(unsendable)]
pub struct NeighborsBuffer {
    data: *const u32,
    len: usize,
    _g: Arc<dyn Any>,
}

impl NeighborsBuffer {
    pub fn out_neighbors(g: &Arc<DirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.out_neighbors(node);
        Self {
            data: data.as_ptr(),
            len: data.len(),
            _g: g,
        }
    }

    pub fn in_neighbors(g: &Arc<DirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.in_neighbors(node);
        Self {
            data: data.as_ptr(),
            len: data.len(),
            _g: g,
        }
    }

    pub fn neighbors(g: &Arc<UndirectedCsrGraph<u32>>, node: u32) -> Self {
        let g = Arc::clone(g);
        let data = g.neighbors(node);
        Self {
            data: data.as_ptr(),
            len: data.len(),
            _g: g,
        }
    }
}

#[allow(deprecated)]
#[pyproto]
impl PyBufferProtocol for NeighborsBuffer {
    fn bf_getbuffer(slf: PyRefMut<Self>, view: *mut ffi::Py_buffer, flags: c_int) -> PyResult<()> {
        // see https://docs.python.org/3/c-api/typeobj.html#c.PyBufferProcs.bf_getbuffer
        // 1. Check if the request can be met. If not, raise PyExc_BufferError, set view->obj to NULL and return -1.
        // 2. Fill in the requested fields.
        // 3. Increment an internal counter for the number of exports.
        // 4. Set view->obj to exporter and increment view->obj.
        // 5. Return 0.

        println!(
            "getbuffer *view = {view:p} flags = {flags} data = {:p}",
            slf.data
        );

        if view.is_null() {
            return Err(PyBufferError::new_err("View is null"));
        }

        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            // view is not NULL, checked above
            unsafe {
                (*view).obj = std::ptr::null_mut();
            }

            return Err(PyBufferError::new_err(
                "Cannot satisfy a write request on a read-only buffer",
            ));
        }

        // 4. Set view->obj to exporter and increment view->obj.
        // view is not NULL
        unsafe {
            (*view).obj = slf.as_ptr();
            ffi::Py_INCREF((*view).obj);
        }

        unsafe {
            // A pointer to the start of the logical structure described by the buffer fields.
            // For contiguous arrays, the value points to the beginning of the memory block.
            (*view).buf = slf.data as _;

            // product(shape) * itemsize. For contiguous arrays, this is the length of the underlying memory block.
            // This is the length in BYTES, not the number of elements in the array
            (*view).len = (slf.len * std::mem::size_of::<u32>()) as _;

            // An indicator of whether the buffer is read-only.
            (*view).readonly = true as _;

            // A NUL terminated string in struct module style syntax describing the contents of a single item.
            // https://docs.python.org/3/library/struct.html#format-characters -> I == u32
            static FORMAT: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"I\0") };
            (*view).format = FORMAT.as_ptr() as _;

            // Item size in bytes of a single element.
            (*view).itemsize = std::mem::size_of::<u32>() as _;

            // The number of dimensions the memory represents as an n-dimensional array.
            (*view).ndim = 1;

            // An array of `ndim` elements indicating the shape of the memory as an n-dimensional array
            (*view).shape = std::slice::from_ref(&slf.len).as_ptr() as _;
            // (*view).shape = std::ptr::null_mut();

            // An array of `ndim` elements giving the number of bytes to skip to get to a new element in each dimension.
            // (*view).strides = std::slice::from_ref(&0).as_ptr() as _;
            (*view).strides = std::ptr::null_mut();

            // An array `ndim` elements dictating how many bytes to add to each pointer after de-referencing.
            // A suboffset value that is negative indicates that no de-referencing should occur (striding in a contiguous memory block).
            // If all suboffsets are negative (i.e. no de-referencing is needed), then this field must be NULL.
            (*view).suboffsets = std::ptr::null_mut();

            // This is for use internally by the exporting object (this is us).
            (*view).internal = std::ptr::null_mut();
        }

        Ok(())
    }

    fn bf_releasebuffer(slf: PyRefMut<Self>, view: *mut ffi::Py_buffer) -> PyResult<()> {
        println!("releasebuffer *view = {view:p} data = {:p}", slf.data);
        Ok(())
    }
}

impl Debug for NeighborsBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeighborsBuffer")
            .field("data", &self.data)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for NeighborsBuffer {
    fn drop(&mut self) {
        let sc = Arc::strong_count(&self._g);
        let data = self.data;
        println!("buffer dropped, graph strong count: {sc}, data = {data:p}");
    }
}
