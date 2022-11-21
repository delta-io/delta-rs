use std::convert::From;

enum DeltaLakeAdbcError {

}

impl From<DeltalakeAdbcError> for AdbcError {
    fn from(error: DeltalakeAdbcError) -> Self {

        AdbcError {
            message: "message".to_c_str(),
            vendor_code: -1,
            sqlstate: b"\0\0\0\0\0",
            release: Some(delete_adbc_error)
        }
    }
}


/// Represents a particular session connecting to delta tables. Shared between threads.
struct DeltalakeAdbcDatabase {

}

impl From<&Arc<DeltalakeAdbcDatabase>> for AdbcDatabase {
    fn from(error: &Arc<DeltalakeAdbcDatabase>) -> Self {
        // put a copy of the Arc in private data
        todo!()
    }
}


// A particular connection. Each thread will have it's own.
struct DeltalakeAdbcConnection {

}

impl From<&Rc<DeltalakeAdbcConnection>> for AdbcDatabase {
    fn from(error: &Rc<DeltalakeAdbcConnection>) -> Self {
        // put a copy of the Rc in private data
        todo!()
    }
}
