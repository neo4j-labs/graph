use super::{GResult, GraphError};
use crate::pr::PageRankResult;
use graph::prelude::{
    CsrLayout, DirectedCsrGraph, DirectedDegrees, DirectedNeighbors, Graph as GraphTrait,
    Graph500Input, GraphBuilder,
};
use numpy::{npyffi::types::NPY_TYPES, PyArray, PyArray1, PY_ARRAY_API};
use pyo3::prelude::*;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Layout>()?;
    m.add_class::<Graph>()?;
    m.add_function(wrap_pyfunction!(show_very_unsafe, m)?)?;
    Ok(())
}

#[pyfunction]
pub fn show_very_unsafe(py: Python<'_>, obj: PyObject) -> PyResult<()> {
    let vu: PyRef<very_unsafe::VeryUnsafe> = obj.extract(py)?;
    println!("very unsafe: pyobj {obj:?}, vu {vu:?}");
    Ok(())
}

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

#[pyclass]
pub struct Graph {
    g: DirectedCsrGraph<u32>,
    #[pyo3(get)]
    load_micros: u64,
}

#[pymethods]
impl Graph {
    #[staticmethod]
    #[args(layout = "Layout::Unsorted")]
    pub fn load(py: Python<'_>, path: PathBuf, layout: Layout) -> PyResult<Self> {
        let layout = match layout {
            Layout::Sorted => CsrLayout::Sorted,
            Layout::Unsorted => CsrLayout::Unsorted,
            Layout::Deduplicated => CsrLayout::Deduplicated,
        };
        let graph = py
            .allow_threads(move || Self::load_graph500(path, layout))
            .map_err(GraphError)?;
        Ok(graph)
    }

    fn node_count(&self) -> u32 {
        self.g.node_count()
    }

    fn edge_count(&self) -> u32 {
        self.g.edge_count()
    }

    fn out_degree(&self, node: u32) -> u32 {
        self.g.out_degree(node)
    }

    fn target(&self, node: u32, idx: usize) -> Option<u32> {
        self.g.out_neighbors(node).get(idx).copied()
    }

    fn out_neighbors<'py>(&self, py: Python<'py>, node: u32) -> PyResult<&'py PyArray1<u32>> {
        let nb = very_unsafe::VeryUnsafe::new(self.g.out_neighbors(node));
        let nb = Py::new(py, nb)?;
        let buf = nb.into_ptr();

        unsafe {
            let tpe = PY_ARRAY_API.PyArray_DescrFromType(py, NPY_TYPES::NPY_UINT as _);
            let ptr = PY_ARRAY_API.PyArray_FromBuffer(py, buf, tpe, -1, 0);
            Ok(PyArray::from_owned_ptr(py, ptr))
        }
    }

    fn out_neighbors2(&self, py: Python<'_>, node: u32) -> PyResult<*mut pyo3::ffi::PyObject> {
        let nb = very_unsafe::VeryUnsafe::new(self.g.out_neighbors(node));
        let nb = Py::new(py, nb)?;
        let buf = nb.into_ptr();

        unsafe {
            let tpe = PY_ARRAY_API.PyArray_DescrFromType(py, NPY_TYPES::NPY_UINT as _);
            let ptr = PY_ARRAY_API.PyArray_FromBuffer(py, buf, tpe, -1, 0);
            Ok(ptr)
        }
    }

    pub fn page_rank(slf: PyRef<Self>, py: Python<'_>) -> PageRankResult {
        crate::pr::page_rank(py, slf)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl Graph {
    pub fn g(&self) -> &DirectedCsrGraph<u32> {
        &self.g
    }

    fn load_graph500(path: PathBuf, layout: CsrLayout) -> GResult<Self> {
        let start = Instant::now();
        let graph = GraphBuilder::new()
            .csr_layout(layout)
            .file_format(Graph500Input::default())
            .path(path)
            .build()?;
        let load_micros = start.elapsed().as_micros().min(u64::MAX as _) as _;
        Ok(Self {
            g: graph,
            load_micros,
        })
    }
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("node_count", &self.g.node_count())
            .field("edge_count", &self.g.edge_count())
            .field("load_took", &Duration::from_micros(self.load_micros))
            .finish()
    }
}

impl Drop for Graph {
    fn drop(&mut self) {
        println!("graph dropped, no out neighbors should be used anymore")
    }
}

mod very_unsafe {
    use std::{ffi::CStr, os::raw::c_int};

    #[allow(deprecated)]
    use pyo3::class::buffer::PyBufferProtocol;
    use pyo3::{exceptions::PyBufferError, ffi, prelude::*, AsPyPointer};

    #[derive(Debug)]
    #[pyclass(unsendable)]
    pub struct VeryUnsafe {
        data: *const u32,
        len: usize,
    }

    impl VeryUnsafe {
        pub fn new(data: &[u32]) -> Self {
            Self {
                data: data.as_ptr(),
                len: data.len(),
            }
        }
    }

    #[allow(deprecated)]
    #[pyproto]
    impl PyBufferProtocol for VeryUnsafe {
        fn bf_getbuffer(
            slf: PyRefMut<Self>,
            view: *mut ffi::Py_buffer,
            flags: c_int,
        ) -> PyResult<()> {
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

                println!(
                    "buf={buf:p}, len={len}, ro={readonly}, f={format:?}, itemsize={itemsize}",
                    buf = (*view).buf,
                    len = (*view).len,
                    readonly = (*view).readonly,
                    format = (*view).format,
                    itemsize = (*view).itemsize,
                )
            }

            Ok(())
        }

        fn bf_releasebuffer(slf: PyRefMut<Self>, view: *mut ffi::Py_buffer) -> PyResult<()> {
            println!("releasebuffer *view = {view:p} data = {:p}", slf.data);
            Ok(())
        }
    }

    impl Drop for VeryUnsafe {
        fn drop(&mut self) {
            println!("drop data = {:p}", self.data);
        }
    }

    // impl<'a> PyBufferGetBufferProtocol<'a> for VeryUnsafe {
    //     type Result = PyResult<()>;
    // }
}
