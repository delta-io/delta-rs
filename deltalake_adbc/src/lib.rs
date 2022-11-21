mod ffi;
mod impl;

extern "C" {
    /// Allocate a new (but uninitialized) database.
    pub fn AdbcDatabaseNew(database: *mut AdbcDatabase, error: *mut AdbcError) -> AdbcStatusCode;
}
extern "C" {
    /// Set a char* option.
    ///
    /// Options may be set before AdbcDatabaseInit.  Some drivers may
    /// support setting options after initialization as well.
    ///
    /// \\return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
    pub fn AdbcDatabaseSetOption(
        database: *mut AdbcDatabase,
        key: *const ::std::os::raw::c_char,
        value: *const ::std::os::raw::c_char,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Finish setting options and initialize the database.
    ///
    /// Some drivers may support setting options after initialization
    /// as well.
    pub fn AdbcDatabaseInit(database: *mut AdbcDatabase, error: *mut AdbcError) -> AdbcStatusCode;
}
extern "C" {
    /// Destroy this database. No connections may exist.
    /// \\param[in] database The database to release.
    /// \\param[out] error An optional location to return an error
    ///   message if necessary.
    pub fn AdbcDatabaseRelease(
        database: *mut AdbcDatabase,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Allocate a new (but uninitialized) connection.
    pub fn AdbcConnectionNew(
        connection: *mut AdbcConnection,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Set a char* option.
    ///
    /// Options may be set before AdbcConnectionInit.  Some drivers may
    /// support setting options after initialization as well.
    ///
    /// \\return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
    pub fn AdbcConnectionSetOption(
        connection: *mut AdbcConnection,
        key: *const ::std::os::raw::c_char,
        value: *const ::std::os::raw::c_char,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Finish setting options and initialize the connection.
    ///
    /// Some drivers may support setting options after initialization
    /// as well.
    pub fn AdbcConnectionInit(
        connection: *mut AdbcConnection,
        database: *mut AdbcDatabase,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Destroy this connection.
    ///
    /// \\param[in] connection The connection to release.
    /// \\param[out] error An optional location to return an error
    ///   message if necessary.
    pub fn AdbcConnectionRelease(
        connection: *mut AdbcConnection,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Get metadata about the database/driver.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name                  | Field Type
    /// ----------------------------|------------------------
    /// info_name                   | uint32 not null
    /// info_value                  | INFO_SCHEMA
    ///
    /// INFO_SCHEMA is a dense union with members:
    ///
    /// Field Name (Type Code)      | Field Type
    /// ----------------------------|------------------------
    /// string_value (0)            | utf8
    /// bool_value (1)              | bool
    /// int64_value (2)             | int64
    /// int32_bitmask (3)           | int32
    /// string_list (4)             | list<utf8>
    /// int32_to_int32_list_map (5) | map<int32, list<int32>>
    ///
    /// Each metadatum is identified by an integer code.  The recognized
    /// codes are defined as constants.  Codes [0, 10_000) are reserved
    /// for ADBC usage.  Drivers/vendors will ignore requests for
    /// unrecognized codes (the row will be omitted from the result).
    ///
    /// \\param[in] connection The connection to query.
    /// \\param[in] info_codes A list of metadata codes to fetch, or NULL
    ///   to fetch all.
    /// \\param[in] info_codes_length The length of the info_codes
    ///   parameter.  Ignored if info_codes is NULL.
    /// \\param[out] out The result set.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcConnectionGetInfo(
        connection: *mut AdbcConnection,
        info_codes: *mut u32,
        info_codes_length: usize,
        out: *mut ArrowArrayStream,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Get a hierarchical view of all catalogs, database schemas,
    ///   tables, and columns.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | catalog_name             | utf8                    |
    /// | catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>  |
    ///
    /// DB_SCHEMA_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | db_schema_name           | utf8                    |
    /// | db_schema_tables         | list<TABLE_SCHEMA>      |
    ///
    /// TABLE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | table_name               | utf8 not null           |
    /// | table_type               | utf8 not null           |
    /// | table_columns            | list<COLUMN_SCHEMA>     |
    /// | table_constraints        | list<CONSTRAINT_SCHEMA> |
    ///
    /// COLUMN_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | column_name              | utf8 not null           |          |
    /// | ordinal_position         | int32                   | (1)      |
    /// | remarks                  | utf8                    | (2)      |
    /// | xdbc_data_type           | int16                   | (3)      |
    /// | xdbc_type_name           | utf8                    | (3)      |
    /// | xdbc_column_size         | int32                   | (3)      |
    /// | xdbc_decimal_digits      | int16                   | (3)      |
    /// | xdbc_num_prec_radix      | int16                   | (3)      |
    /// | xdbc_nullable            | int16                   | (3)      |
    /// | xdbc_column_def          | utf8                    | (3)      |
    /// | xdbc_sql_data_type       | int16                   | (3)      |
    /// | xdbc_datetime_sub        | int16                   | (3)      |
    /// | xdbc_char_octet_length   | int32                   | (3)      |
    /// | xdbc_is_nullable         | utf8                    | (3)      |
    /// | xdbc_scope_catalog       | utf8                    | (3)      |
    /// | xdbc_scope_schema        | utf8                    | (3)      |
    /// | xdbc_scope_table         | utf8                    | (3)      |
    /// | xdbc_is_autoincrement    | bool                    | (3)      |
    /// | xdbc_is_generatedcolumn  | bool                    | (3)      |
    ///
    /// 1. The column's ordinal position in the table (starting from 1).
    /// 2. Database-specific description of the column.
    /// 3. Optional value.  Should be null if not supported by the driver.
    ///    xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
    ///    in an agnostic manner.
    ///
    /// CONSTRAINT_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | constraint_name          | utf8                    |          |
    /// | constraint_type          | utf8 not null           | (1)      |
    /// | constraint_column_names  | list<utf8> not null     | (2)      |
    /// | constraint_column_usage  | list<USAGE_SCHEMA>      | (3)      |
    ///
    /// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
    /// 2. The columns on the current table that are constrained, in
    ///    order.
    /// 3. For FOREIGN KEY only, the referenced table and columns.
    ///
    /// USAGE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | fk_catalog               | utf8                    |
    /// | fk_db_schema             | utf8                    |
    /// | fk_table                 | utf8 not null           |
    /// | fk_column_name           | utf8 not null           |
    ///
    /// \\param[in] connection The database connection.
    /// \\param[in] depth The level of nesting to display. If 0, display
    ///   all levels. If 1, display only catalogs (i.e.  catalog_schemas
    ///   will be null). If 2, display only catalogs and schemas
    ///   (i.e. db_schema_tables will be null), and so on.
    /// \\param[in] catalog Only show tables in the given catalog. If NULL,
    ///   do not filter by catalog. If an empty string, only show tables
    ///   without a catalog.  May be a search pattern (see section
    ///   documentation).
    /// \\param[in] db_schema Only show tables in the given database schema. If
    ///   NULL, do not filter by database schema. If an empty string, only show
    ///   tables without a database schema. May be a search pattern (see section
    ///   documentation).
    /// \\param[in] table_name Only show tables with the given name. If NULL, do not
    ///   filter by name. May be a search pattern (see section documentation).
    /// \\param[in] table_type Only show tables matching one of the given table
    ///   types. If NULL, show tables of any type. Valid table types can be fetched
    ///   from GetTableTypes.  Terminate the list with a NULL entry.
    /// \\param[in] column_name Only show columns with the given name. If
    ///   NULL, do not filter by name.  May be a search pattern (see
    ///   section documentation).
    /// \\param[out] out The result set.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcConnectionGetObjects(
        connection: *mut AdbcConnection,
        depth: ::std::os::raw::c_int,
        catalog: *const ::std::os::raw::c_char,
        db_schema: *const ::std::os::raw::c_char,
        table_name: *const ::std::os::raw::c_char,
        table_type: *mut *const ::std::os::raw::c_char,
        column_name: *const ::std::os::raw::c_char,
        out: *mut ArrowArrayStream,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Get the Arrow schema of a table.
    ///
    /// \\param[in] connection The database connection.
    /// \\param[in] catalog The catalog (or nullptr if not applicable).
    /// \\param[in] db_schema The database schema (or nullptr if not applicable).
    /// \\param[in] table_name The table name.
    /// \\param[out] schema The table schema.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcConnectionGetTableSchema(
        connection: *mut AdbcConnection,
        catalog: *const ::std::os::raw::c_char,
        db_schema: *const ::std::os::raw::c_char,
        table_name: *const ::std::os::raw::c_char,
        schema: *mut ArrowSchema,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Get a list of table types in the database.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name     | Field Type
    /// ---------------|--------------
    /// table_type     | utf8 not null
    ///
    /// \\param[in] connection The database connection.
    /// \\param[out] out The result set.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcConnectionGetTableTypes(
        connection: *mut AdbcConnection,
        out: *mut ArrowArrayStream,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Construct a statement for a partition of a query. The
    ///   results can then be read independently.
    ///
    /// A partition can be retrieved from AdbcPartitions.
    ///
    /// \\param[in] connection The connection to use.  This does not have
    ///   to be the same connection that the partition was created on.
    /// \\param[in] serialized_partition The partition descriptor.
    /// \\param[in] serialized_length The partition descriptor length.
    /// \\param[out] out The result set.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcConnectionReadPartition(
        connection: *mut AdbcConnection,
        serialized_partition: *const u8,
        serialized_length: usize,
        out: *mut ArrowArrayStream,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Commit any pending transactions. Only used if autocommit is
    ///   disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction
    /// statements.
    pub fn AdbcConnectionCommit(
        connection: *mut AdbcConnection,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Roll back any pending transactions. Only used if autocommit
    ///   is disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction
    /// statements.
    pub fn AdbcConnectionRollback(
        connection: *mut AdbcConnection,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Create a new statement for a given connection.
    ///
    /// Set options on the statement, then call AdbcStatementExecuteQuery
    /// or AdbcStatementPrepare.
    pub fn AdbcStatementNew(
        connection: *mut AdbcConnection,
        statement: *mut AdbcStatement,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Destroy a statement.
    /// \\param[in] statement The statement to release.
    /// \\param[out] error An optional location to return an error
    ///   message if necessary.
    pub fn AdbcStatementRelease(
        statement: *mut AdbcStatement,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Execute a statement and get the results.
    ///
    /// This invalidates any prior result sets.
    ///
    /// \\param[in] statement The statement to execute.
    /// \\param[out] out The results. Pass NULL if the client does not
    ///   expect a result set.
    /// \\param[out] rows_affected The number of rows affected if known,
    ///   else -1. Pass NULL if the client does not want this information.
    /// \\param[out] error An optional location to return an error
    ///   message if necessary.
    pub fn AdbcStatementExecuteQuery(
        statement: *mut AdbcStatement,
        out: *mut ArrowArrayStream,
        rows_affected: *mut i64,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Turn this statement into a prepared statement to be
    ///   executed multiple times.
    ///
    /// This invalidates any prior result sets.
    pub fn AdbcStatementPrepare(
        statement: *mut AdbcStatement,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Set the SQL query to execute.
    ///
    /// The query can then be executed with AdbcStatementExecute.  For
    /// queries expected to be executed repeatedly, AdbcStatementPrepare
    /// the statement first.
    ///
    /// \\param[in] statement The statement.
    /// \\param[in] query The query to execute.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcStatementSetSqlQuery(
        statement: *mut AdbcStatement,
        query: *const ::std::os::raw::c_char,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Set the Substrait plan to execute.
    ///
    /// The query can then be executed with AdbcStatementExecute.  For
    /// queries expected to be executed repeatedly, AdbcStatementPrepare
    /// the statement first.
    ///
    /// \\param[in] statement The statement.
    /// \\param[in] plan The serialized substrait.Plan to execute.
    /// \\param[in] length The length of the serialized plan.
    /// \\param[out] error Error details, if an error occurs.
    pub fn AdbcStatementSetSubstraitPlan(
        statement: *mut AdbcStatement,
        plan: *const u8,
        length: usize,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Bind Arrow data. This can be used for bulk inserts or
    ///   prepared statements.
    ///
    /// \\param[in] statement The statement to bind to.
    /// \\param[in] values The values to bind. The driver will call the
    ///   release callback itself, although it may not do this until the
    ///   statement is released.
    /// \\param[in] schema The schema of the values to bind.
    /// \\param[out] error An optional location to return an error message
    ///   if necessary.
    pub fn AdbcStatementBind(
        statement: *mut AdbcStatement,
        values: *mut ArrowArray,
        schema: *mut ArrowSchema,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Bind Arrow data. This can be used for bulk inserts or
    ///   prepared statements.
    /// \\param[in] statement The statement to bind to.
    /// \\param[in] stream The values to bind. The driver will call the
    ///   release callback itself, although it may not do this until the
    ///   statement is released.
    /// \\param[out] error An optional location to return an error message
    ///   if necessary.
    pub fn AdbcStatementBindStream(
        statement: *mut AdbcStatement,
        stream: *mut ArrowArrayStream,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Get the schema for bound parameters.
    ///
    /// This retrieves an Arrow schema describing the number, names, and
    /// types of the parameters in a parameterized statement.  The fields
    /// of the schema should be in order of the ordinal position of the
    /// parameters; named parameters should appear only once.
    ///
    /// If the parameter does not have a name, or the name cannot be
    /// determined, the name of the corresponding field in the schema will
    /// be an empty string.  If the type cannot be determined, the type of
    /// the corresponding field will be NA (NullType).
    ///
    /// This should be called after AdbcStatementPrepare.
    ///
    /// \\return ADBC_STATUS_NOT_IMPLEMENTED if the schema cannot be determined.
    pub fn AdbcStatementGetParameterSchema(
        statement: *mut AdbcStatement,
        schema: *mut ArrowSchema,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Set a string option on a statement.
    pub fn AdbcStatementSetOption(
        statement: *mut AdbcStatement,
        key: *const ::std::os::raw::c_char,
        value: *const ::std::os::raw::c_char,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
extern "C" {
    /// Execute a statement and get the results as a partitioned
    ///   result set.
    ///
    /// \\param[in] statement The statement to execute.
    /// \\param[out] schema The schema of the result set.
    /// \\param[out] partitions The result partitions.
    /// \\param[out] rows_affected The number of rows affected if known,
    ///   else -1. Pass NULL if the client does not want this information.
    /// \\param[out] error An optional location to return an error
    ///   message if necessary.
    /// \\return ADBC_STATUS_NOT_IMPLEMENTED if the driver does not support
    ///   partitioned results
    pub fn AdbcStatementExecutePartitions(
        statement: *mut AdbcStatement,
        schema: *mut ArrowSchema,
        partitions: *mut AdbcPartitions,
        rows_affected: *mut i64,
        error: *mut AdbcError,
    ) -> AdbcStatusCode;
}
/// Common entry point for drivers via the driver manager
///   (which uses dlopen(3)/LoadLibrary). The driver manager is told
///   to load a library and call a function of this type to load the
///   driver.
///
/// Although drivers may choose any name for this function, the
/// recommended name is \"AdbcDriverInit\".
///
/// \\param[in] version The ADBC revision to attempt to initialize (see
///   ADBC_VERSION_1_0_0).
/// \\param[out] driver The table of function pointers to
///   initialize. Should be a pointer to the appropriate struct for
///   the given version (see the documentation for the version).
/// \\param[out] error An optional location to return an error message
///   if necessary.
/// \\return ADBC_STATUS_OK if the driver was initialized, or
///   ADBC_STATUS_NOT_IMPLEMENTED if the version is not supported.  In
///   that case, clients may retry with a different version.
pub type AdbcDriverInitFunc = ::std::option::Option<
    unsafe extern "C" fn(
        version: ::std::os::raw::c_int,
        driver: *mut ::std::os::raw::c_void,
        error: *mut AdbcError,
    ) -> AdbcStatusCode,
>;



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
