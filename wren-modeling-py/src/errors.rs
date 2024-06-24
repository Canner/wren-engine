use base64::DecodeError;
use pyo3::exceptions::PyException;
use pyo3::PyErr;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("{message}")]
pub struct CoreError {
    message: String,
}

impl CoreError {
    pub fn new(msg: &str) -> CoreError {
        CoreError {
            message: msg.to_string(),
        }
    }
}

impl From<CoreError> for PyErr {
    fn from(err: CoreError) -> Self {
        PyException::new_err(err.to_string())
    }
}

impl From<DecodeError> for CoreError {
    fn from(err: DecodeError) -> Self {
        CoreError::new(&err.to_string())
    }
}

impl From<FromUtf8Error> for CoreError {
    fn from(err: FromUtf8Error) -> Self {
        CoreError::new(&err.to_string())
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(err: serde_json::Error) -> Self {
        CoreError::new(&err.to_string())
    }
}
