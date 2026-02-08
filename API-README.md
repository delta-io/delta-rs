# delta-rs API: How to Get Parquet File Names

This document explains how clients retrieve parquet file paths in delta-rs and analyzes the laziness characteristics of the API.

---

## 1. High-Level API (Recommended for most users)

```rust
use deltalake::DeltaTable;
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the table
    let table = DeltaTable::try_from_url("s3://bucket/my_table").await?;
    
    // Get the log store (storage abstraction)
    let log_store = table.log_store();
    
    // Get file views - a stream of LogicalFileView objects
    let files: Vec<_> = table
        .snapshot()?
        .file_views(log_store.as_ref(), None)  // None = no predicate filter
        .try_collect()
        .await?;
    
    // Each LogicalFileView gives you the file path
    for file in files {
        println!("Parquet file: {}", file.path());
        println!("  Size: {} bytes", file.size());
        println!("  Modified: {:?}", file.modification_datetime()?);
        println!("  Partition values: {:?}", file.partition_values());
        println!("  Num records: {:?}", file.num_records());
    }
    
    Ok(())
}
```

---

## 2. The `LogicalFileView` Struct

This is the core abstraction for accessing file metadata. Defined in `crates/core/src/kernel/snapshot/iterators.rs`:

```rust
pub struct LogicalFileView {
    files: RecordBatch,  // Arrow RecordBatch containing file data
    index: usize,        // Index into the batch
}
```

### Key Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `path()` | `Cow<'_, str>` | File path (URL decoded, relative to table root) |
| `size()` | `i64` | File size in bytes |
| `modification_time()` | `i64` | Modification timestamp (epoch ms) |
| `modification_datetime()` | `DeltaResult<DateTime<Utc>>` | Parsed modification time |
| `partition_values()` | `Option<StructData>` | Partition column values |
| `num_records()` | `Option<usize>` | Number of rows in file |
| `null_counts()` | `Option<Scalar>` | Null counts per column |
| `min_values()` | `Option<Scalar>` | Min values per column (for data skipping) |
| `max_values()` | `Option<Scalar>` | Max values per column (for data skipping) |
| `stats()` | `Option<String>` | Raw JSON stats string |
| `deletion_vector_descriptor()` | `Option<DeletionVectorDescriptor>` | DV info if present |
| `object_store_path()` | `Path` | Object store path for reading |

---

## 3. With Predicate Pushdown (File Skipping)

To get only files that *might* match your query:

```rust
use deltalake::kernel::Expression;
use std::sync::Arc;

// Create a predicate (e.g., date = '2026-02-05')
let predicate = Arc::new(Expression::eq(
    Expression::column("date"),
    Expression::literal("2026-02-05")
));

// Get only files that pass the predicate (based on min/max stats)
let matching_files: Vec<_> = table
    .snapshot()?
    .file_views(log_store.as_ref(), Some(predicate))
    .try_collect()
    .await?;
```

---

## 4. Using `EagerSnapshot` (Loads all files eagerly)

```rust
use deltalake::kernel::EagerSnapshot;

// Create an eager snapshot (loads all file metadata into memory)
let eager_snapshot = EagerSnapshot::try_new(
    log_store.as_ref(),
    Default::default(),
    None  // version
).await?;

// Use LogDataHandler for synchronous iteration
for file_view in eager_snapshot.log_data() {
    println!("File: {}", file_view.path());
}
```

---

## 5. Using DataFusion for Query Execution

For query execution, DataFusion handles file iteration internally:

```rust
use deltalake::DeltaTable;

let table = DeltaTable::try_from_url("s3://bucket/my_table").await?;

// Get a TableProvider for DataFusion
let provider = table.table_provider().await?;

// Register with DataFusion session
session.register_table("my_table", Arc::new(provider))?;

// Query - DataFusion handles file iteration internally
let df = session.sql("SELECT * FROM my_table WHERE date = '2026-02-05'").await?;
let batches = df.collect().await?;
```

---

## 6. Low-Level: Raw RecordBatch Stream

```rust
// Get raw RecordBatches of file metadata
let file_batches: Vec<RecordBatch> = table
    .snapshot()?
    .files(log_store.as_ref(), None)
    .try_collect()
    .await?;

// Each batch has columns: path, size, modificationTime, stats_parsed, etc.
for batch in file_batches {
    let path_col = batch.column_by_name("path").unwrap();
    // ... extract data from Arrow arrays
}
```

---

## 7. Full Path Construction

```rust
let table_root = log_store.table_root_url();
let full_path = table_root.join(file_view.path().as_ref())?;
println!("Full URI: {}", full_path);
```

---

# Laziness Analysis

## Is the API Lazy?

### `Snapshot::file_views()` - **Partially Lazy (Stream)**

```rust
pub fn file_views(
    &self,
    log_store: &dyn LogStore,
    predicate: Option<PredicateRef>,
) -> BoxStream<'_, DeltaResult<LogicalFileView>>
```

**What's lazy:**
- Returns a `Stream` (async iterator), so you can process files one-by-one as they arrive
- You can stop early with `.take(n)` or other stream combinators
- Processing happens batch-by-batch via `ScanRowOutStream`

**What's NOT lazy (the problem):**
- The underlying log replay still reads **all checkpoint + commit data** before the stream can start yielding
- The kernel's `scan_metadata()` method does a full log replay upfront

### `EagerSnapshot` - **Not Lazy at All**

```rust
pub struct EagerSnapshot {
    snapshot: Arc<Snapshot>,
    files: Vec<RecordBatch>,  // ⚠️ ALL file metadata loaded here
}
```

When you create an `EagerSnapshot`, **all file metadata is loaded into memory immediately**. The `log_data()` iterator is just iterating over this pre-loaded `Vec`.

---

## Where the "Laziness" Breaks Down

Looking at the code flow in `snapshot/scan.rs`:

```rust
pub fn scan_metadata(&self, engine: Arc<dyn Engine>) -> SendableScanMetadataStream {
    let mut builder = ReceiverStreamBuilder::<ScanMetadata>::new(100);
    let tx = builder.tx();

    let inner = self.inner.clone();
    let blocking_iter = move || {
        // ⚠️ This iterates through ALL log data from checkpoint + commits
        for res in inner.scan_metadata(engine.as_ref())? {
            if tx.blocking_send(Ok(res?)).is_err() {
                break;
            }
        }
        Ok(())
    };

    builder.spawn_blocking(blocking_iter);
    builder.build()  // Returns a Stream, but the work is already running
}
```

The stream is created, but a background task is spawned that:
1. Reads the entire checkpoint Parquet file (potentially GBs for millions of files)
2. Reads all JSON commits since checkpoint
3. Performs log replay to compute the active file set
4. Only then starts yielding to the stream

---

## The Real Problem for Huge Tables

| Step | Lazy? | Memory Impact |
|------|-------|---------------|
| Open table | Yes | Minimal |
| Read checkpoint | **No** | O(total files) - could be GBs |
| Log replay | **No** | O(removes + adds) |
| Stream to client | Yes | O(batch size) |

**You pay the full cost upfront** before getting any files. For a table with 10M files:
- Checkpoint Parquet: ~10-50GB read
- Memory: ~10GB+ to hold file metadata
- Time to first file: Minutes (waiting for full replay)

---

## The Reverse Reader Alternative

The reverse-first reader design (see `REVERSE-READ.md`) provides **true laziness**:

| Step | Current delta-rs | Reverse Reader |
|------|------------------|----------------|
| Open table | Read checkpoint (slow) | Read only `_last_checkpoint` (tiny JSON) |
| First file available | After full replay | After reading newest commit |
| Memory usage | O(total files) | O(removes seen in scanned window) |
| Time to first file | Minutes for huge tables | Milliseconds |

### Practical Example

```rust
// Current delta-rs (NOT truly lazy):
let files = snapshot.file_views(log_store, Some(predicate));

// Even though this is a Stream, internally:
// 1. Full checkpoint is read
// 2. All commits are replayed
// 3. THEN the stream starts yielding

// With reverse reader:
let files = reverse_planner.plan_reverse_add_remove(&predicates, Some(100));

// Truly lazy:
// 1. Only newest commits are read
// 2. Files are emitted as discovered
// 3. Early termination after finding 100 files
```

---

## Summary

| API | Returns | True Laziness? | Memory for 10M files |
|-----|---------|----------------|----------------------|
| `snapshot.file_views()` | `Stream<LogicalFileView>` | Partial (stream yes, replay no) | ~10GB+ |
| `eager_snapshot.log_data()` | `Iterator<LogicalFileView>` | No | ~10GB+ |
| `table.scan_table()` | `RecordBatchStream` | Partial | ~10GB+ |
| Reverse Reader (proposed) | `Stream<PlanFile>` | **Yes** | O(removes) ~10-50MB |

**The API surfaces look lazy (streams/iterators), but the underlying implementation is eager for the expensive parts (checkpoint reading and log replay).**

---

# Calling from C# Managed Code

There are several approaches to use delta-rs from C#/.NET:

## Option 1: Python Interop via Python.NET (Easiest)

Use the `deltalake` Python package with Python.NET:

```csharp
using Python.Runtime;

public class DeltaTableReader
{
    public static void ReadDeltaTable(string tablePath)
    {
        // Initialize Python runtime
        Runtime.PythonDLL = @"C:\Python311\python311.dll";
        PythonEngine.Initialize();
        
        using (Py.GIL())
        {
            dynamic deltalake = Py.Import("deltalake");
            
            // Open table
            dynamic dt = deltalake.DeltaTable(tablePath);
            
            // Get file URIs
            dynamic fileUris = dt.file_uris();
            foreach (var uri in fileUris)
            {
                Console.WriteLine($"Parquet file: {uri}");
            }
            
            // Or convert to PyArrow and read
            dynamic pyarrow = Py.Import("pyarrow");
            dynamic table = dt.to_pyarrow_table();
            
            // Get as Pandas DataFrame
            dynamic df = table.to_pandas();
        }
    }
}
```

**Pros:** Full delta-rs functionality, maintained by the community
**Cons:** Requires Python runtime, marshalling overhead

---

## Option 2: Native FFI via P/Invoke (Best Performance)

Build a C-compatible library from Rust and call via P/Invoke:

### Rust Side (create a cdylib)

```rust
// lib.rs
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

#[repr(C)]
pub struct FileInfo {
    pub path: *mut c_char,
    pub size: i64,
}

#[no_mangle]
pub extern "C" fn delta_get_files(
    table_path: *const c_char,
    out_files: *mut *mut FileInfo,
    out_count: *mut usize,
) -> i32 {
    let path = unsafe { CStr::from_ptr(table_path).to_str().unwrap() };
    
    // Use tokio runtime for async delta-rs calls
    let rt = tokio::runtime::Runtime::new().unwrap();
    let result = rt.block_on(async {
        let table = deltalake::open_table(path).await?;
        let log_store = table.log_store();
        
        let files: Vec<_> = table
            .snapshot()?
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        
        Ok::<_, Box<dyn std::error::Error>>(files)
    });
    
    match result {
        Ok(files) => {
            let mut file_infos: Vec<FileInfo> = files
                .iter()
                .map(|f| FileInfo {
                    path: CString::new(f.path().to_string()).unwrap().into_raw(),
                    size: f.size(),
                })
                .collect();
            
            unsafe {
                *out_count = file_infos.len();
                *out_files = file_infos.as_mut_ptr();
            }
            std::mem::forget(file_infos);
            0 // Success
        }
        Err(_) => -1 // Error
    }
}

#[no_mangle]
pub extern "C" fn delta_free_files(files: *mut FileInfo, count: usize) {
    unsafe {
        let files = Vec::from_raw_parts(files, count, count);
        for f in files {
            let _ = CString::from_raw(f.path);
        }
    }
}
```

### C# Side

```csharp
using System;
using System.Runtime.InteropServices;

public class DeltaInterop
{
    [StructLayout(LayoutKind.Sequential)]
    public struct FileInfo
    {
        public IntPtr Path;
        public long Size;
    }

    [DllImport("delta_ffi", CallingConvention = CallingConvention.Cdecl)]
    private static extern int delta_get_files(
        [MarshalAs(UnmanagedType.LPStr)] string tablePath,
        out IntPtr files,
        out UIntPtr count);

    [DllImport("delta_ffi", CallingConvention = CallingConvention.Cdecl)]
    private static extern void delta_free_files(IntPtr files, UIntPtr count);

    public static List<(string Path, long Size)> GetDeltaFiles(string tablePath)
    {
        var result = new List<(string, long)>();
        
        int status = delta_get_files(tablePath, out IntPtr filesPtr, out UIntPtr count);
        if (status != 0)
            throw new Exception("Failed to read delta table");

        try
        {
            int fileCount = (int)count;
            int structSize = Marshal.SizeOf<FileInfo>();
            
            for (int i = 0; i < fileCount; i++)
            {
                IntPtr current = IntPtr.Add(filesPtr, i * structSize);
                FileInfo info = Marshal.PtrToStructure<FileInfo>(current);
                string path = Marshal.PtrToStringAnsi(info.Path);
                result.Add((path, info.Size));
            }
        }
        finally
        {
            delta_free_files(filesPtr, count);
        }

        return result;
    }
}

// Usage
var files = DeltaInterop.GetDeltaFiles("abfss://container@storage.dfs.core.windows.net/delta_table");
foreach (var (path, size) in files)
{
    Console.WriteLine($"{path}: {size} bytes");
}
```

**Pros:** Best performance, no Python dependency
**Cons:** More complex setup, need to build Rust library

---

## Option 3: delta-dotnet (Community Package)

There's a community .NET wrapper: [delta-dotnet](https://github.com/delta-incubator/delta-dotnet)

```csharp
// NuGet: Install-Package DeltaLake.Net

using DeltaLake;

var table = await DeltaTable.LoadAsync("s3://bucket/my_table");
var files = table.GetFiles();

foreach (var file in files)
{
    Console.WriteLine(file.Path);
}
```

**Note:** Check the project's current status and feature completeness.

---

## Option 4: Direct Parquet Reading (After Getting File List)

If you only need the reverse reader (your custom implementation), expose it via FFI and then read Parquet files directly in C#:

### Get File List from Rust FFI

```csharp
var files = DeltaInterop.GetDeltaFiles("path/to/table");
```

### Read Parquet in C# with Parquet.Net

```csharp
// NuGet: Install-Package Parquet.Net

using Parquet;
using Parquet.Data;

foreach (var (path, _) in files)
{
    using var stream = await OpenFromAzureBlob(path);
    using var reader = await ParquetReader.CreateAsync(stream);
    
    for (int i = 0; i < reader.RowGroupCount; i++)
    {
        using var rowGroup = reader.OpenRowGroupReader(i);
        var columns = await rowGroup.ReadAllColumnsAsync();
        // Process data...
    }
}
```

### Or with Apache Arrow C#

```csharp
// NuGet: Install-Package Apache.Arrow

using Apache.Arrow;
using Apache.Arrow.Ipc;

// Read Arrow IPC or use Arrow Flight for streaming
```

---

## Option 5: Arrow Flight (Best for Streaming Large Data Over Network)

Arrow Flight is a high-performance RPC framework built on gRPC and Arrow, designed for efficient transfer of large datasets.

> **⚠️ Important Clarification on "Zero-Copy":** Arrow Flight does NOT provide true zero-copy when data crosses a network boundary. Data must be serialized, transmitted, and deserialized. However, it provides:
> - **Minimal serialization overhead** - Arrow IPC format is nearly identical to in-memory format
> - **Zero-copy within each process** - No additional copies after receiving data
> - **Efficient columnar transfer** - Cache-friendly processing on both ends

**Benefits for your use case:**
- **Streaming** - Results stream as they're discovered (perfect for reverse reader)
- **Backpressure** - Client controls the rate of data consumption
- **Language-agnostic** - Works with any language that has Arrow Flight bindings
- **Low serialization cost** - Arrow IPC is very efficient compared to JSON/Protobuf

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        C# Application                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Arrow Flight Client (gRPC)                  │   │
│  │  - GetFlightInfo(table_path, predicate)                 │   │
│  │  - DoGet(ticket) → Stream<RecordBatch>                  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │ gRPC (HTTP/2)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Rust Arrow Flight Server                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              FlightService Implementation                │   │
│  │  - get_flight_info() → endpoints, schema                │   │
│  │  - do_get() → Stream RecordBatches from Delta table     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Reverse Reader / delta-rs                   │   │
│  │  - Lazy file iteration                                  │   │
│  │  - Predicate pushdown                                   │   │
│  │  - Parquet reading                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    Azure Blob Storage
                    (Delta Table files)
```

### Rust Flight Server Implementation

Add dependencies to `Cargo.toml`:

```toml
[package]
name = "delta-flight-server"
version = "0.1.0"
edition = "2021"

[dependencies]
# Arrow Flight
arrow = "54"
arrow-flight = "54"
arrow-schema = "54"
arrow-array = "54"
arrow-ipc = "54"

# Async runtime
tokio = { version = "1", features = ["full"] }
tokio-stream = "0.1"
futures = "0.3"

# gRPC
tonic = "0.12"
prost = "0.13"

# Delta Lake
deltalake = { version = "0.23", features = ["datafusion", "azure"] }

# Azure
azure_identity = "0.21"
azure_storage_blobs = "0.21"

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Error handling
anyhow = "1"
thiserror = "1"
```

Complete Flight server implementation:

```rust
// src/main.rs
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult,
    Ticket, encode::FlightDataEncoderBuilder,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema;
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming, transport::Server};

use deltalake::DeltaTable;

/// Request to scan a delta table
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct DeltaScanRequest {
    /// Table path (e.g., "abfss://container@account.dfs.core.windows.net/path/to/table")
    pub table_path: String,
    /// Optional predicate for partition/stats filtering (JSON format)
    pub predicate: Option<String>,
    /// Optional limit on number of rows
    pub limit: Option<usize>,
    /// Optional projection (column names)
    pub columns: Option<Vec<String>>,
}

pub struct DeltaFlightService {
    // Could hold connection pool, credentials, etc.
}

impl DeltaFlightService {
    pub fn new() -> Self {
        Self {}
    }

    async fn scan_delta_table(
        &self,
        request: DeltaScanRequest,
    ) -> Result<impl Stream<Item = Result<RecordBatch, Status>>, Status> {
        // Open the delta table
        let table = DeltaTable::try_from_url(&request.table_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to open table: {}", e)))?;

        let log_store = table.log_store();
        
        // Get file views as a stream
        let snapshot = table
            .snapshot()
            .map_err(|e| Status::internal(format!("Failed to get snapshot: {}", e)))?;

        // TODO: Parse predicate from JSON and apply
        let predicate = None; // For now, no predicate

        // Create a stream that reads parquet files
        let files_stream = snapshot.file_views(log_store.as_ref(), predicate);

        // For each file, read and stream the data
        // This is a simplified version - in practice you'd want to parallelize
        let table_clone = table.clone();
        let record_batch_stream = files_stream
            .map_err(|e| Status::internal(format!("File iteration error: {}", e)))
            .and_then(move |file_view| {
                let table = table_clone.clone();
                async move {
                    // Read the parquet file
                    let path = file_view.path().to_string();
                    let full_path = table.log_store().table_root_url().join(&path)
                        .map_err(|e| Status::internal(format!("Path error: {}", e)))?;
                    
                    // Use object_store to read the file
                    // This is simplified - you'd use ParquetRecordBatchStream
                    // For now, return the file metadata as a placeholder
                    Err(Status::unimplemented("Full parquet reading not shown"))
                }
            });

        Ok(record_batch_stream)
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for DeltaFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    /// Get metadata about a delta table (schema, endpoints)
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        
        // Parse the descriptor to get table path and options
        let scan_request: DeltaScanRequest = match &descriptor.cmd {
            cmd if !cmd.is_empty() => {
                serde_json::from_slice(cmd)
                    .map_err(|e| Status::invalid_argument(format!("Invalid request: {}", e)))?
            }
            _ => {
                // Use path as table location
                let path = descriptor.path.join("/");
                DeltaScanRequest {
                    table_path: path,
                    predicate: None,
                    limit: None,
                    columns: None,
                }
            }
        };

        // Open table to get schema
        let table = DeltaTable::try_from_url(&scan_request.table_path)
            .await
            .map_err(|e| Status::not_found(format!("Table not found: {}", e)))?;

        let snapshot = table.snapshot()
            .map_err(|e| Status::internal(format!("Snapshot error: {}", e)))?;
        
        let arrow_schema: Schema = snapshot.schema()
            .as_ref()
            .try_into()
            .map_err(|e| Status::internal(format!("Schema conversion error: {}", e)))?;

        // Create a ticket that encodes the full request
        let ticket = Ticket {
            ticket: serde_json::to_vec(&scan_request)
                .map_err(|e| Status::internal(format!("Serialization error: {}", e)))?
                .into(),
        };

        // Single endpoint (this server)
        let endpoint = arrow_flight::FlightEndpoint {
            ticket: Some(ticket),
            location: vec![], // Empty means "this server"
            expiration_time: None,
            app_metadata: vec![].into(),
        };

        let flight_info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor);

        Ok(Response::new(flight_info))
    }

    /// Stream data from the delta table
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        
        // Parse the scan request from the ticket
        let scan_request: DeltaScanRequest = serde_json::from_slice(&ticket.ticket)
            .map_err(|e| Status::invalid_argument(format!("Invalid ticket: {}", e)))?;

        // Open the delta table
        let table = DeltaTable::try_from_url(&scan_request.table_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to open table: {}", e)))?;

        // Use DataFusion for actual query execution (simpler approach)
        let (table, stream) = table.scan_table()
            .await
            .map_err(|e| Status::internal(format!("Scan error: {}", e)))?;

        // Get schema for encoding
        let schema = stream.schema();

        // Convert RecordBatch stream to FlightData stream
        let options = IpcWriteOptions::default();
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .with_options(options)
            .build(stream.map_err(|e| arrow_flight::error::FlightError::Arrow(e.into())))
            .map_err(|e| Status::internal(format!("Encoding error: {}", e)));

        Ok(Response::new(Box::pin(flight_data_stream)))
    }

    // Required but not used for our case
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let info = self.get_flight_info(request).await?.into_inner();
        let schema = info.try_decode_schema()
            .map_err(|e| Status::internal(format!("Schema decode error: {}", e)))?;
        
        Ok(Response::new(SchemaResult {
            schema: SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
                .try_into()
                .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?,
        }))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;
    let service = DeltaFlightService::new();

    println!("Delta Flight Server listening on {}", addr);

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
```

### Integration with Your Reverse Reader

Replace the DataFusion scan with your reverse reader:

```rust
async fn do_get_with_reverse_reader(
    &self,
    request: Request<Ticket>,
) -> Result<Response<Self::DoGetStream>, Status> {
    let ticket = request.into_inner();
    let scan_request: DeltaScanRequest = serde_json::from_slice(&ticket.ticket)?;

    // Use your reverse reader instead of DataFusion
    let client = AzureDeltaLogClient::new_with_default_credential(
        &account, &container, &table_path
    ).await?;

    let planner = ReversePlanner::new(&client, |_| true);
    let predicates = parse_predicates(&scan_request.predicate)?;
    
    // Get file list lazily
    let plan_files = planner.plan_reverse_add_remove(&predicates, scan_request.limit).await?;

    // Create a stream that reads each parquet file
    let parquet_stream = futures::stream::iter(plan_files)
        .map(|plan_file| async move {
            // Read parquet file from Azure
            read_parquet_from_azure(&plan_file.path).await
        })
        .buffered(4)  // Parallelize up to 4 files
        .flatten();

    // Encode as Flight data
    let schema = get_schema_from_first_batch(&parquet_stream)?;
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(parquet_stream);

    Ok(Response::new(Box::pin(flight_stream)))
}
```

### C# Flight Client (Complete Implementation)

Add NuGet packages:

```xml
<PackageReference Include="Apache.Arrow" Version="17.0.0" />
<PackageReference Include="Apache.Arrow.Flight" Version="17.0.0" />
<PackageReference Include="Grpc.Net.Client" Version="2.62.0" />
```

Complete C# client:

```csharp
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Flight;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Types;
using Grpc.Net.Client;

namespace DeltaFlightClient
{
    /// <summary>
    /// Request parameters for scanning a Delta table
    /// </summary>
    public class DeltaScanRequest
    {
        public string TablePath { get; set; } = "";
        public string? Predicate { get; set; }
        public int? Limit { get; set; }
        public List<string>? Columns { get; set; }
    }

    /// <summary>
    /// Client for reading Delta tables via Arrow Flight
    /// </summary>
    public class DeltaFlightClient : IDisposable
    {
        private readonly FlightClient _client;
        private readonly GrpcChannel _channel;

        public DeltaFlightClient(string serverAddress = "http://localhost:50051")
        {
            _channel = GrpcChannel.ForAddress(serverAddress, new GrpcChannelOptions
            {
                MaxReceiveMessageSize = 100 * 1024 * 1024, // 100MB
                MaxSendMessageSize = 100 * 1024 * 1024,
            });
            _client = new FlightClient(_channel);
        }

        /// <summary>
        /// Get schema and flight info for a Delta table
        /// </summary>
        public async Task<(Schema Schema, FlightInfo Info)> GetTableInfoAsync(DeltaScanRequest request)
        {
            var cmd = JsonSerializer.SerializeToUtf8Bytes(new
            {
                table_path = request.TablePath,
                predicate = request.Predicate,
                limit = request.Limit,
                columns = request.Columns
            });

            var descriptor = FlightDescriptor.CreateCommandDescriptor(cmd);
            var info = await _client.GetFlightInfo(descriptor);
            var schema = info.Schema;

            return (schema, info);
        }

        /// <summary>
        /// Stream all data from a Delta table
        /// </summary>
        public async IAsyncEnumerable<RecordBatch> ReadTableAsync(DeltaScanRequest request)
        {
            var (schema, info) = await GetTableInfoAsync(request);

            foreach (var endpoint in info.Endpoints)
            {
                var ticket = endpoint.Ticket;
                
                await foreach (var batch in _client.GetStream(ticket))
                {
                    yield return batch;
                }
            }
        }

        /// <summary>
        /// Read table and process with callback (for memory efficiency)
        /// </summary>
        public async Task ReadTableWithCallbackAsync(
            DeltaScanRequest request,
            Func<RecordBatch, Task<bool>> processCallback)
        {
            await foreach (var batch in ReadTableAsync(request))
            {
                bool continueReading = await processCallback(batch);
                if (!continueReading)
                    break;
            }
        }

        /// <summary>
        /// Read table with row-level processing
        /// </summary>
        public async Task ProcessRowsAsync<T>(
            DeltaScanRequest request,
            Func<RecordBatch, int, T> rowMapper,
            Action<T> rowHandler)
        {
            await foreach (var batch in ReadTableAsync(request))
            {
                for (int i = 0; i < batch.Length; i++)
                {
                    var item = rowMapper(batch, i);
                    rowHandler(item);
                }
            }
        }

        public void Dispose()
        {
            _channel.Dispose();
        }
    }

    /// <summary>
    /// Helper extensions for working with Arrow RecordBatches
    /// </summary>
    public static class RecordBatchExtensions
    {
        public static T? GetValue<T>(this RecordBatch batch, string columnName, int rowIndex)
        {
            var column = batch.Column(columnName);
            return GetArrayValue<T>(column, rowIndex);
        }

        private static T? GetArrayValue<T>(IArrowArray array, int index)
        {
            return array switch
            {
                StringArray sa => (T)(object)sa.GetString(index),
                Int32Array i32 => (T)(object)i32.GetValue(index),
                Int64Array i64 => (T)(object)i64.GetValue(index),
                DoubleArray da => (T)(object)da.GetValue(index),
                BooleanArray ba => (T)(object)ba.GetValue(index),
                TimestampArray ta => (T)(object)ta.GetTimestamp(index),
                _ => default
            };
        }
    }

    // Usage example
    public class Program
    {
        public static async Task Main(string[] args)
        {
            using var client = new DeltaFlightClient("http://localhost:50051");

            var request = new DeltaScanRequest
            {
                TablePath = "abfss://container@account.dfs.core.windows.net/tables/sales",
                Predicate = "{\"date\": \"2026-02-05\"}",
                Limit = 1000,
                Columns = new List<string> { "id", "name", "amount", "date" }
            };

            // Option 1: Stream all batches
            Console.WriteLine("Streaming data from Delta table...");
            long totalRows = 0;
            
            await foreach (var batch in client.ReadTableAsync(request))
            {
                totalRows += batch.Length;
                Console.WriteLine($"Received batch with {batch.Length} rows");
                
                // Process batch...
                for (int i = 0; i < Math.Min(5, batch.Length); i++)
                {
                    var id = batch.GetValue<long?>("id", i);
                    var name = batch.GetValue<string>("name", i);
                    Console.WriteLine($"  Row {i}: id={id}, name={name}");
                }
            }
            Console.WriteLine($"Total rows: {totalRows}");

            // Option 2: With callback (for early termination)
            Console.WriteLine("\nWith callback (stop after 100 rows):");
            long rowCount = 0;
            await client.ReadTableWithCallbackAsync(request, async batch =>
            {
                rowCount += batch.Length;
                Console.WriteLine($"Processing batch: {batch.Length} rows");
                
                // Return false to stop streaming
                return rowCount < 100;
            });

            // Option 3: Row-level processing
            Console.WriteLine("\nRow-level processing:");
            await client.ProcessRowsAsync(
                request,
                rowMapper: (batch, idx) => new
                {
                    Id = batch.GetValue<long?>("id", idx),
                    Name = batch.GetValue<string>("name", idx),
                    Amount = batch.GetValue<double?>("amount", idx)
                },
                rowHandler: row =>
                {
                    Console.WriteLine($"Row: {row.Id}, {row.Name}, {row.Amount}");
                });
        }
    }
}
```

### Docker Deployment

```dockerfile
# Dockerfile for the Flight Server
FROM rust:1.75 as builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/delta-flight-server /usr/local/bin/
EXPOSE 50051
CMD ["delta-flight-server"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  delta-flight:
    build: .
    ports:
      - "50051:50051"
    environment:
      - AZURE_CLIENT_ID=${AZURE_CLIENT_ID}
      - AZURE_CLIENT_SECRET=${AZURE_CLIENT_SECRET}
      - AZURE_TENANT_ID=${AZURE_TENANT_ID}
    volumes:
      - ~/.azure:/root/.azure:ro  # For DefaultAzureCredential
```

### Performance Considerations

| Aspect | Recommendation |
|--------|----------------|
| **Batch size** | 64K-256K rows per batch (Arrow default is good) |
| **Parallelism** | Buffer 2-4 parquet file reads concurrently |
| **Compression** | Use LZ4 for Flight data (fast decompression) |
| **Memory** | Streaming means O(batch_size) memory, not O(total_rows) |
| **Network** | HTTP/2 multiplexing handles many concurrent streams |

**Pros:** 
- Efficient streaming, native Arrow format, language-agnostic
- Zero-copy possible between Rust and C# via shared memory
- Backpressure built-in
- Works with your reverse reader for truly lazy evaluation

**Cons:** 
- Requires running a server (can be sidecar or separate service)
- Additional network hop (mitigated by high throughput)
- **NOT true zero-copy** - data is serialized over network

---

## Option 6: True Zero-Copy (Same Machine Only)

If Rust and C# run on the **same machine**, you can achieve true zero-copy using shared memory or in-process FFI with Arrow buffer passing.

### Option 6a: Memory-Mapped Files (Inter-Process)

Rust writes Arrow IPC to a memory-mapped file, C# reads directly from the same mapped region.

**Rust Side:**

```rust
use arrow_ipc::writer::FileWriter;
use memmap2::MmapMut;
use std::fs::OpenOptions;

pub fn write_to_shared_memory(
    batches: &[RecordBatch],
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create/open the file
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    
    // Estimate size and set file length
    let estimated_size = estimate_arrow_ipc_size(batches);
    file.set_len(estimated_size as u64)?;
    
    // Memory map
    let mut mmap = unsafe { MmapMut::map_mut(&file)? };
    
    // Write Arrow IPC format
    let schema = batches[0].schema();
    let mut writer = FileWriter::try_new(&mut mmap[..], &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    
    // Flush to ensure visibility
    mmap.flush()?;
    
    Ok(())
}

fn estimate_arrow_ipc_size(batches: &[RecordBatch]) -> usize {
    // Rough estimate: sum of all buffer sizes + metadata overhead
    batches.iter()
        .flat_map(|b| b.columns().iter())
        .map(|c| c.get_buffer_memory_size())
        .sum::<usize>() + 1024 * batches.len()
}
```

**C# Side:**

```csharp
using System.IO.MemoryMappedFiles;
using Apache.Arrow;
using Apache.Arrow.Ipc;

public class SharedMemoryReader : IDisposable
{
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _accessor;

    public SharedMemoryReader(string path)
    {
        _mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open);
        _accessor = _mmf.CreateViewAccessor();
    }

    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync()
    {
        // Create a stream over the memory-mapped region
        using var stream = _mmf.CreateViewStream();
        using var reader = new ArrowFileReader(stream);
        
        // Read schema
        var schema = reader.Schema;
        
        // Read all batches - these point directly to mapped memory!
        for (int i = 0; i < reader.RecordBatchCount; i++)
        {
            var batch = await reader.ReadRecordBatchAsync(i);
            if (batch != null)
            {
                yield return batch;  // Zero-copy: points to mmap'd memory
            }
        }
    }

    public void Dispose()
    {
        _accessor.Dispose();
        _mmf.Dispose();
    }
}

// Usage
using var reader = new SharedMemoryReader("/tmp/delta_data.arrow");
await foreach (var batch in reader.ReadBatchesAsync())
{
    // batch.Column("id") points directly to shared memory
    // No serialization, no copy!
    ProcessBatch(batch);
}
```

### Option 6b: In-Process FFI with Arrow C Data Interface

Load Rust as a DLL in the C# process and pass Arrow arrays via the Arrow C Data Interface (zero-copy pointer passing).

**Rust Side:**

```rust
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use std::ffi::c_void;

/// Export a RecordBatch via FFI using Arrow C Data Interface
#[no_mangle]
pub extern "C" fn get_next_batch(
    context: *mut c_void,
    out_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
) -> i32 {
    let ctx = unsafe { &mut *(context as *mut ScanContext) };
    
    match ctx.next_batch() {
        Some(batch) => {
            // Convert to FFI format - this is zero-copy!
            let struct_array = arrow::array::StructArray::from(batch);
            
            unsafe {
                // Export schema (only needed once, but shown here for completeness)
                let schema = batch.schema();
                arrow::ffi::export_field_to_c(
                    &arrow::datatypes::Field::new("", schema.as_ref().into(), false),
                    out_schema
                );
                
                // Export array data - just passes pointers!
                arrow::ffi::export_array_to_c(
                    Arc::new(struct_array),
                    out_array
                );
            }
            
            0 // Success, more data available
        }
        None => 1 // No more data
    }
}

#[no_mangle]
pub extern "C" fn release_batch(
    schema: *mut FFI_ArrowSchema,
    array: *mut FFI_ArrowArray,
) {
    unsafe {
        if !schema.is_null() {
            let _ = arrow::ffi::import_field_from_c(schema);
        }
        if !array.is_null() {
            let _ = arrow::ffi::import_array_from_c(array, todo!());
        }
    }
}

struct ScanContext {
    // Your reverse reader state
    planner: ReversePlanner,
    current_file_stream: Option<ParquetRecordBatchStream>,
}

impl ScanContext {
    fn next_batch(&mut self) -> Option<RecordBatch> {
        // Get next batch from current file or advance to next file
        todo!()
    }
}
```

**C# Side (using Arrow C# FFI):**

```csharp
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

public class ZeroCopyDeltaReader : IDisposable
{
    private IntPtr _context;

    [DllImport("delta_ffi")]
    private static extern IntPtr create_scan_context(
        [MarshalAs(UnmanagedType.LPStr)] string tablePath,
        [MarshalAs(UnmanagedType.LPStr)] string predicateJson);

    [DllImport("delta_ffi")]
    private static extern int get_next_batch(
        IntPtr context,
        out CArrowSchema schema,
        out CArrowArray array);

    [DllImport("delta_ffi")]
    private static extern void release_batch(
        ref CArrowSchema schema,
        ref CArrowArray array);

    [DllImport("delta_ffi")]
    private static extern void destroy_scan_context(IntPtr context);

    public ZeroCopyDeltaReader(string tablePath, string? predicate = null)
    {
        _context = create_scan_context(tablePath, predicate ?? "");
        if (_context == IntPtr.Zero)
            throw new Exception("Failed to create scan context");
    }

    public IEnumerable<RecordBatch> ReadBatches()
    {
        while (true)
        {
            int result = get_next_batch(_context, out var schema, out var array);
            
            if (result != 0)
                yield break;  // No more data

            try
            {
                // Import from C Data Interface - zero-copy!
                // The Arrow C# library wraps the Rust-owned buffers
                var importedSchema = CArrowSchemaImporter.ImportSchema(&schema);
                var importedArray = CArrowArrayImporter.ImportArray(&array, importedSchema);
                
                // Create RecordBatch from the struct array
                var batch = new RecordBatch(importedSchema, importedArray);
                
                yield return batch;
            }
            finally
            {
                // Release when done
                release_batch(ref schema, ref array);
            }
        }
    }

    public void Dispose()
    {
        if (_context != IntPtr.Zero)
        {
            destroy_scan_context(_context);
            _context = IntPtr.Zero;
        }
    }
}

// Usage
using var reader = new ZeroCopyDeltaReader(
    "abfss://container@account.dfs.core.windows.net/tables/sales",
    "{\"date\": \"2026-02-05\"}");

foreach (var batch in reader.ReadBatches())
{
    // batch columns point directly to Rust-owned memory
    // TRUE zero-copy!
    var ids = batch.Column("id") as Int64Array;
    for (int i = 0; i < ids.Length; i++)
    {
        Console.WriteLine(ids.GetValue(i));
    }
}
```

### Comparison: True Zero-Copy Options

| Approach | Zero-Copy? | Complexity | Use Case |
|----------|------------|------------|----------|
| Memory-mapped file | ✅ Yes | Medium | Batch processing, file-based exchange |
| Arrow C Data Interface | ✅ Yes | High | Streaming, in-process integration |
| Arrow Flight (network) | ❌ No | Medium | Distributed, multi-client |
| P/Invoke strings | ❌ No | Low | Simple file list retrieval |

### When to Use Each

| Scenario | Recommended Approach |
|----------|---------------------|
| Same machine, streaming query | Arrow C Data Interface (Option 6b) |
| Same machine, batch export | Memory-mapped files (Option 6a) |
| Different machines | Arrow Flight (Option 5) |
| Just need file paths | P/Invoke callback (Option 2) |

---

## Recommended Approach for Your Use Case

Given your requirements (huge tables, query execution, Azure):

1. **For file list only (reverse reader):**
   - Build a Rust cdylib with your reverse reader
   - Call via P/Invoke to get file paths
   - Read Parquet files directly in C# using Parquet.Net or Azure SDK

2. **For full query execution:**
   - Use Arrow Flight server in Rust
   - Stream results to C# client
   - Best performance for large data

3. **For quick prototyping:**
   - Python.NET with deltalake package
   - Fastest to implement

---

## Example: Minimal FFI for Reverse Reader

```rust
// Cargo.toml
[lib]
crate-type = ["cdylib"]

[dependencies]
# your reverse reader deps
```

```rust
// src/lib.rs
#[no_mangle]
pub extern "C" fn reverse_get_files(
    storage_account: *const c_char,
    container: *const c_char,
    table_path: *const c_char,
    partition_filter_json: *const c_char,
    limit: usize,
    callback: extern "C" fn(*const c_char, i64) -> bool,
) -> i32 {
    // Your reverse reader implementation
    // Calls callback for each file found
    // Returns 0 on success
}
```

```csharp
public delegate bool FileCallback(
    [MarshalAs(UnmanagedType.LPStr)] string path,
    long size);

[DllImport("delta_reverse")]
private static extern int reverse_get_files(
    string account, string container, string table,
    string filterJson, UIntPtr limit, FileCallback callback);

// Usage with streaming callback
var files = new List<string>();
reverse_get_files(
    "mystorageaccount", "mycontainer", "tables/sales",
    "{\"date\": \"2026-02-05\"}", (UIntPtr)100,
    (path, size) => { files.Add(path); return true; });
```

This gives you true streaming from Rust to C# without buffering all files in memory.
