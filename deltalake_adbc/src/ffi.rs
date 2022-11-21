use deltalake::arrow::ffi;

pub type AdbcStatusCode = u8;

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

unsafe {
    extern fn "C" delete_adbc_error(error: *mut AdbcError) -> () {
        todo!();
    }
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
            arg4: *mut ArrowArrayStream,
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
            arg8: *mut ArrowArrayStream,
            arg9: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *const ::std::os::raw::c_char,
            arg5: *mut ArrowSchema,
            arg6: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableTypes: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcConnection,
            arg2: *mut ArrowArrayStream,
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
            arg4: *mut ArrowArrayStream,
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
            arg2: *mut ArrowArray,
            arg3: *mut ArrowSchema,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementBindStream: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut ArrowArrayStream,
            arg3: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecuteQuery: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut ArrowArrayStream,
            arg3: *mut i64,
            arg4: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecutePartitions: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut ArrowSchema,
            arg3: *mut AdbcPartitions,
            arg4: *mut i64,
            arg5: *mut AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementGetParameterSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut AdbcStatement,
            arg2: *mut ArrowSchema,
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