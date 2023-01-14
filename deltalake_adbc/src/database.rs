// TODO: remove this
#[allow(unused_variables)]
use std::convert::From;
use std::ffi::{c_char, c_void, CString};

use crate::ffi::{AdbcDatabase, AdbcDriver, AdbcStatusCode};
use crate::AdbcError;
use std::rc::Rc;
use std::sync::Arc;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeltalakeAdbcError {
    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl From<&DeltalakeAdbcError> for AdbcError {
    fn from(error: &DeltalakeAdbcError) -> Self {
        AdbcError {
            message: CString::new(error.to_string()).unwrap().into_raw(),
            vendor_code: -1,
            sqlstate: ['\0' as c_char; 5],
            release: Some(drop_adbc_error),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn drop_adbc_error(error: *mut AdbcError) -> () {
    if !error.is_null() {
        // Retake pointer so it will drop when it goes out of scope.
        let _1 = CString::from_raw((*error).message);
        // Delete the data
        error.drop_in_place()
    }
}

impl From<&DeltalakeAdbcError> for AdbcStatusCode {
    fn from(err: &DeltalakeAdbcError) -> Self {
        match err {
            DeltalakeAdbcError::NotImplemented(_) => AdbcStatusCode::NotImplemented,
            // TODO: fill in other errors
            _ => AdbcStatusCode::Unknown,
        }
    }
}

impl From<&Result<(), DeltalakeAdbcError>> for AdbcStatusCode {
    fn from(res: &Result<(), DeltalakeAdbcError>) -> Self {
        match res {
            Ok(()) => AdbcStatusCode::Ok,
            Err(err) => AdbcStatusCode::from(err),
        }
    }
}

/// Represents a particular session connecting to delta tables. Shared between threads.
pub struct DeltalakeAdbcDatabase {}

impl DeltalakeAdbcDatabase {
    pub fn new() -> Self {
        DeltalakeAdbcDatabase {}
    }
}

impl From<Arc<DeltalakeAdbcDatabase>> for AdbcDatabase {
    fn from(internal: Arc<DeltalakeAdbcDatabase>) -> Self {
        let driver = AdbcDriver::new();
        Self {
            // Place the Arc into private data
            private_data: Arc::into_raw(internal) as *mut c_void,
            private_driver: Box::into_raw(Box::new(driver)),
        }
    }
}

// A particular connection. Each thread will have it's own.
struct DeltalakeAdbcConnection {}

impl From<&Rc<DeltalakeAdbcConnection>> for AdbcDatabase {
    fn from(error: &Rc<DeltalakeAdbcConnection>) -> Self {
        // put a copy of the Rc in private data
        todo!()
    }
}
