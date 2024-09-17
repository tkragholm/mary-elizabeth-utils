use pyo3::prelude::*;
use std::path::PathBuf;

#[pyfunction]
fn get_readstat_path() -> PyResult<PathBuf> {
    let path = std::env::var("READSTAT_BINARY")
        .map(PathBuf::from)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to get READSTAT_BINARY: {}",
                e
            ))
        })?;
    Ok(path)
}

/// A Python module implemented in Rust.
#[pymodule]
fn _mary_elizabeth_utils(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_readstat_path, m)?)?;
    Ok(())
}
