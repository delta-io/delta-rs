// TODO: remove this
#[allow(unused_variables)]

use std::ptr::null_mut;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

// pub type AdbcStatusCode = u8;

#[repr(u8)]
pub enum AdbcStatusCode {
    /// No error.
    Ok = 0,
    /// An unknown error occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Unknown = 1,
    /// The operation is not implemented or supported.
    ///
    /// May indicate a driver-side or database-side error.
    NotImplemented = 2,
    /// A requested resource was not found.
    ///
    /// May indicate a driver-side or database-side error.
    NotFound = 3,
    /// A requested resource already exists.
    ///
    /// May indicate a driver-side or database-side error.
    AlreadyExists = 4,
    /// The arguments are invalid, likely a programming error.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidArguments = 5,
    /// The preconditions for the operation are not met, likely a
    ///   programming error.
    ///
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidState = 6,
    /// Invalid data was processed (not a programming error).
    ///
    /// For instance, a division by zero may have occurred during query
    /// execution.
    ///
    /// May indicate a database-side error only.
    InvalidData = 7,
    /// The database's integrity was affected.
    ///
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    ///
    /// May indicate a database-side error only.
    Integrity = 8,
    /// An error internal to the driver or database occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Internal = 9,
    /// An I/O error occurred.
    ///
    /// For instance, a remote service may be unavailable.
    ///
    /// May indicate a driver-side or database-side error.
    IO = 10,
    /// The operation was cancelled, not due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Cancelled = 11,
    /// The operation was cancelled due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Timeout = 12,
    /// Authentication failed.
    ///
    /// May indicate a database-side error only.
    Unauthenticated = 13,
    /// The client is not authorized to perform the given operation.
    ///
    /// May indicate a database-side error only.
    Unauthorized = 14,
}

/// A detailed error message for an operation.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcError {
    /// The error message.
    pub message: *mut ::std::os::raw::c_char,
    /// A vendor-specific error code, if applicable.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the
    /// SQL:2003 standard.  If not set, it should be set to
    /// "\\0\\0\\0\\0\\0".
    pub sqlstate: [::std::os::raw::c_char; 5usize],
    /// Release the contained error.
    ///
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    pub release: ::std::option::Option<unsafe extern "C" fn(error: *mut AdbcError)>,
}

/// An instance of a database.
///
/// Must be kept alive as long as any connections exist.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcDatabase {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    ///   track state).
    pub private_driver: *mut AdbcDriver,
}

/// An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a connection.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcConnection {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    ///   track state).
    pub private_driver: *mut AdbcDriver,
}

///  A container for all state needed to execute a database
/// query, such as the query itself, parameters for prepared
/// statements, driver parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used
/// concurrently (whether from a single thread or multiple threads).
///
/// Statements are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a statement.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcStatement {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    /// track state).
    pub private_driver: *mut AdbcDriver,
}

/// The partitions of a distributed/partitioned result set.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcPartitions {
    /// The number of partitions.
    pub num_partitions: usize,
    /// The partitions of the result set, where each entry (up to
    /// num_partitions entries) is an opaque identifier that can be
    /// passed to AdbcConnectionReadPartition.
    pub partitions: *mut *const u8,
    /// The length of each corresponding entry in partitions.
    pub partition_lengths: *const usize,
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// Release the contained partitions.
    ///
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    pub release: ::std::option::Option<unsafe extern "C" fn(partitions: *mut AdbcPartitions)>,
}

/// An instance of an initialized database driver.
///
/// This provides a common interface for vendor-specific driver
/// initialization routines. Drivers should populate this struct, and
/// applications can call ADBC functions through this struct, without
/// worrying about multiple definitions of the same symbol.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct AdbcDriver {
    /// Opaque driver-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub private_data: *mut ::std::os::raw::c_void,
    /// Opaque driver manager-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub private_manager: *mut ::std::os::raw::c_void,
    ///  Release the driver and perform any cleanup.
    ///
    /// This is an embedded callback to make it easier for the driver
    /// manager and driver to cooperate.
    pub release: ::std::option::Option<
        unsafe extern "C" fn(driver: *mut AdbcDriver, error: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub DatabaseInit: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcDatabase, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub DatabaseNew: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcDatabase, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub DatabaseSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcDatabase,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub DatabaseRelease: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcDatabase, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub ConnectionCommit: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcConnection, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub ConnectionGetInfo: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *mut u32,
            arg3: usize,
            arg4: *mut FFI_ArrowArrayStream,
            arg5: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetObjects: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: ::std::os::raw::c_int,
            arg3: *const ::std::os::raw::c_char,
            arg4: *const ::std::os::raw::c_char,
            arg5: *const ::std::os::raw::c_char,
            arg6: *mut *const ::std::os::raw::c_char,
            arg7: *const ::std::os::raw::c_char,
            arg8: *mut FFI_ArrowArrayStream,
            arg9: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *const ::std::os::raw::c_char,
            arg5: *mut FFI_ArrowSchema,
            arg6: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableTypes: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionInit: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *mut AdbcDatabase,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionNew: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcConnection, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub ConnectionSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionReadPartition: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *const u8,
            arg3: usize,
            arg4: *mut FFI_ArrowArrayStream,
            arg5: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionRelease: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcConnection, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub ConnectionRollback: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcConnection, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub StatementBind: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut FFI_ArrowArray,
            arg3: *mut FFI_ArrowSchema,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementBindStream: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecuteQuery: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut i64,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecutePartitions: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut FFI_ArrowSchema,
            arg3: *mut AdbcPartitions,
            arg4: *mut i64,
            arg5: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementGetParameterSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut FFI_ArrowSchema,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementNew: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *mut AdbcStatement,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementPrepare: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcStatement, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub StatementRelease: ::std::option::Option<
        unsafe extern "C" fn(arg1: *mut AdbcStatement, arg2: *mut AdbcError) -> AdbcStatusCode,
    >,
    pub StatementSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementSetSqlQuery: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *const ::std::os::raw::c_char,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementSetSubstraitPlan: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *const u8,
            arg3: usize,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
}

impl AdbcDriver {
    pub fn new() -> Self {
        Self {
            private_data: null_mut(),
            private_manager: null_mut(),
            release: Some(release_adbc_driver),
            DatabaseInit: Some(super::AdbcDatabaseInit),
            DatabaseNew: Some(super::AdbcDatabaseNew),
            DatabaseRelease: Some(super::AdbcDatabaseRelease),
            DatabaseSetOption: Some(super::AdbcDatabaseSetOption),
            ConnectionCommit: Some(super::AdbcConnectionCommit),
            ConnectionGetInfo: Some(super::AdbcConnectionGetInfo),
            ConnectionInit: Some(super::AdbcConnectionInit),
            ConnectionRelease: Some(super::AdbcConnectionRelease),
            ConnectionGetObjects: Some(super::AdbcConnectionGetObjects),
            ConnectionGetTableSchema: Some(super::AdbcConnectionGetTableSchema),
            ConnectionGetTableTypes: Some(super::AdbcConnectionGetTableTypes),
            ConnectionNew: Some(super::AdbcConnectionNew),
            ConnectionReadPartition: Some(super::AdbcConnectionReadPartition),
            ConnectionRollback: Some(super::AdbcConnectionRollback),
            ConnectionSetOption: Some(super::AdbcConnectionSetOption),
            StatementNew: Some(super::AdbcStatementNew),
            StatementBind: Some(super::AdbcStatementBind),
            StatementBindStream: Some(super::AdbcStatementBindStream),
            StatementExecutePartitions: Some(super::AdbcStatementExecutePartitions),
            StatementExecuteQuery: Some(super::AdbcStatementExecuteQuery),
            StatementGetParameterSchema: Some(super::AdbcStatementGetParameterSchema),
            StatementPrepare: Some(super::AdbcStatementPrepare),
            StatementRelease: Some(super::AdbcStatementRelease),
            StatementSetOption: Some(super::AdbcStatementSetOption),
            StatementSetSqlQuery: Some(super::AdbcStatementSetSqlQuery),
            StatementSetSubstraitPlan: Some(super::AdbcStatementSetSubstraitPlan),
        }
    }
}

pub unsafe extern "C" fn release_adbc_driver(
    driver: *mut AdbcDriver,
    error: *mut AdbcError,
) -> AdbcStatusCode {
    todo!()
}
