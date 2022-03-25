use graph::prelude::{DirectedNeighbors, Idx, UndirectedNeighbors};
use numpy::{
    npyffi::{types::NPY_TYPES, NpyTypes, NPY_ARRAY_DEFAULT, NPY_ARRAY_WRITEABLE},
    PyArray, PyArray1, PY_ARRAY_API,
};
use pyo3::{prelude::*, types::PyCapsule};
use std::{ffi::CStr, fmt::Debug, os::raw::c_void, sync::Arc};

pub trait NumpyType {
    const NP_TYPE: NPY_TYPES;
}

impl NumpyType for u32 {
    const NP_TYPE: NPY_TYPES = NPY_TYPES::NPY_UINT;
}

impl NumpyType for u64 {
    const NP_TYPE: NPY_TYPES = NPY_TYPES::NPY_ULONG;
}

#[pyclass]
pub struct SharedSlice {
    data: SharedConst,
    len: usize,
    np_tpe: NPY_TYPES,
    owner: Arc<dyn Send + Sync>,
}

impl SharedSlice {
    pub fn out_neighbors<NI, G>(g: &Arc<G>, node: NI) -> Self
    where
        NI: NumpyType + Idx,
        for<'a> G: DirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>
            + Send
            + Sync
            + 'static,
    {
        let owner = Arc::clone(g);
        let data = owner.out_neighbors(node).as_slice();
        Self {
            data: SharedConst(data.as_ptr().cast()),
            len: data.len(),
            np_tpe: NI::NP_TYPE,
            owner,
        }
    }

    pub fn in_neighbors<NI, G>(g: &Arc<G>, node: NI) -> Self
    where
        NI: NumpyType + Idx,
        for<'a> G: DirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>
            + Send
            + Sync
            + 'static,
    {
        let owner = Arc::clone(g);
        let data = owner.in_neighbors(node).as_slice();
        Self {
            data: SharedConst(data.as_ptr().cast()),
            len: data.len(),
            np_tpe: NI::NP_TYPE,
            owner,
        }
    }

    pub fn neighbors<NI, G>(g: &Arc<G>, node: NI) -> Self
    where
        NI: NumpyType + Idx,
        for<'a> G: UndirectedNeighbors<NI, NeighborsIterator<'a> = std::slice::Iter<'a, NI>>
            + Send
            + Sync
            + 'static,
    {
        let owner = Arc::clone(g);
        let data = owner.neighbors(node).as_slice();
        Self {
            data: SharedConst(data.as_ptr().cast()),
            len: data.len(),
            np_tpe: NI::NP_TYPE,
            owner,
        }
    }

    pub fn into_numpy<NI: NumpyType>(mut self, py: Python<'_>) -> PyResult<&PyArray1<NI>> {
        assert_eq!(
            NI::NP_TYPE,
            self.np_tpe,
            "The shared slice is the wrong type"
        );
        // Super class-ish of new array, this type creates a base array
        let base_type = unsafe { PY_ARRAY_API.get_type_object(py, NpyTypes::PyArray_Type) };
        // Type of a single element, e.g. Uint = u32
        let element_type = unsafe { PY_ARRAY_API.PyArray_DescrFromType(py, NI::NP_TYPE as _) };
        // 1-D array
        let ndims = 1;
        // One dim with the len in number of elements
        let dims = std::slice::from_mut(&mut self.len).as_mut_ptr().cast();
        // No strides required, can be NULL for 1-D arrays
        let strides = std::ptr::null_mut();
        // Owning data of the buffer (this is us)
        let data = self.data.as_ffi();
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
            self,
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
}

impl Debug for SharedSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSlice")
            .field("data", &self.data.0)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for SharedSlice {
    fn drop(&mut self) {
        match Arc::strong_count(&self.owner) {
            0..=1 => log::trace!("dropping last shared slice, releasing all data"),
            2 => log::trace!("dropping last shared slice, there is only one owner alive"),
            3 => log::trace!("dropping shared slice, there is another shared slice alive"),
            count => log::trace!(
                "dropping shared slice, there are {} other shared slices alives",
                count - 2
            ),
        }
    }
}

struct SharedConst(*const ());

impl SharedConst {
    fn as_ffi(&self) -> *mut c_void {
        self.0 as _
    }
}

unsafe impl Send for SharedConst {}
