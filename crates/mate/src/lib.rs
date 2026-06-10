#![allow(clippy::borrow_deref_ref)]

use ::graph::prelude::Error as GError;
use pyo3::{
    exceptions::PyValueError, prelude::*, types::PyModule, Bound, IntoPyObjectExt, Py, PyAny,
    PyErrArguments,
};
use pyo3_log::{Caching, Logger};

mod graphs;
mod page_rank;
mod triangle_count;
mod wcc;

struct GraphError(GError);

impl PyErrArguments for GraphError {
    fn arguments(self, py: Python<'_>) -> Py<PyAny> {
        self.0
            .to_string()
            .into_py_any(py)
            .expect("converting a string to a Python object is infallible")
    }
}

impl From<GraphError> for PyErr {
    fn from(e: GraphError) -> Self {
        PyValueError::new_err(e)
    }
}

/// Python API for the graph crate
#[pymodule]
fn graph_mate(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    Logger::new(py, Caching::LoggersAndLevels)?
        .install()
        .unwrap();

    graphs::register(py, m)?;
    page_rank::register(py, m)?;
    wcc::register(py, m)?;
    triangle_count::register(py, m)?;

    Ok(())
}
