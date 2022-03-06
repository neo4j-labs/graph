use graph::prelude::Error as GError;
use pyo3::{
    exceptions::PyValueError,
    prelude::{pymodule, IntoPy, PyErr, PyModule, PyObject, PyResult, Python},
    PyErrArguments,
};
use pyo3_log::{Caching, Logger};

mod g;
mod pr;

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

    g::register(py, m)?;
    pr::register(py, m)?;

    Ok(())
}
