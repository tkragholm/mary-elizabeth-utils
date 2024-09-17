use pyo3::prelude::*;
use std::path::PathBuf;

#[pyfunction]
fn get_readstat_path() -> PyResult<PathBuf> {
    let path = PathBuf::from(env!("READSTAT_BINARY"));
    if path.exists() {
        Ok(path)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "ReadStat binary not found at the expected location",
        ))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn _mary_elizabeth_utils(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_readstat_path, m)?)?;
    Ok(())
}
