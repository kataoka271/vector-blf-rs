pub mod blf;

#[cfg(feature = "python")]
mod python;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
fn vector_blf(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(m)
}
