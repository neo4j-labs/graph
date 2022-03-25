#![feature(generic_associated_types)]

use graph::prelude::Error as GError;
use pyo3::{
    exceptions::PyValueError,
    prelude::{pymodule, IntoPy, PyErr, PyModule, PyObject, PyResult, Python},
    PyErrArguments,
};
use pyo3_log::{Caching, Logger};

mod graphs;
mod page_rank;
mod wcc;

type GResult<T> = std::result::Result<T, GError>;

struct GraphError(GError);

impl PyErrArguments for GraphError {
    fn arguments(self, py: Python) -> PyObject {
        self.0.to_string().into_py(py)
    }
}

impl From<GraphError> for PyErr {
    fn from(e: GraphError) -> Self {
        PyValueError::new_err(e)
    }
}

/// Python API for the graph crate
#[pymodule]
fn unladen_swallow(py: Python, m: &PyModule) -> PyResult<()> {
    Logger::new(py, Caching::LoggersAndLevels)?
        .install()
        .unwrap();

    graphs::register(py, m)?;
    page_rank::register(py, m)?;
    wcc::register(py, m)?;

    Ok(())
}
