use std::collections::{HashMap, HashSet};
use std::fs;
use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{protocol::SaveMode, DeltaOps};

use std::sync::Arc;

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("int"),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            String::from("string"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("timestamp"),
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
    let ts_values = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(int_values),
            Arc::new(str_values),
            Arc::new(ts_values),
        ],
    )
    .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized location.
    let path = "/home/cjoy/src/delta-rs/crates/deltalake/examples/test_v";
    let joined = String::from("file://") + path;
    let table_uri = joined.as_str();

    let _ = fs::remove_dir_all(path);
    fs::create_dir(path)?;
    
    let ops = DeltaOps::try_from_uri(table_uri).await?
    ;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        // .with_partition_columns(["timestamp"])
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), Some(0));

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let batch = get_table_batches();
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), Some(1));

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .with_writer_properties(writer_properties.clone())
        .await?;

    assert_eq!(table.version(), Some(2));
    
    // To append data, you can use the same `write` method again:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), Some(3));
    
    // TODO show history
    let mut history = table
        .history(None)
        .await?;
    
    history.reverse();
    
    println!("###############################################################################");
    println!("history:");
    for (version, action) in history.iter().enumerate() {
        println!("version:{}\n, {:#?}", version, action);
    }
    
    let tombstones = table.snapshot()
        .unwrap()
        .all_tombstones(&table.log_store())
        .await
        .unwrap()
        .collect::<Vec<_>>();

    println!("###############################################################################");
    println!("tombstones:");
    println!("{:#?}", tombstones);
    
    let tombstone_files = table
        .snapshot()
        .unwrap()
        .all_tombstones(&table.log_store())
        .await
        .unwrap()
        .map(|r| r.path.clone())
        .collect::<Vec<_>>();
;
    println!("###############################################################################");
    println!("tombstone_files:");
    println!("{:#?}", tombstone_files);
    
    let max_version = table
        .snapshot()
        .unwrap()
        .version();

    println!("###############################################################################");
    println!("all active files used by version:");
    let mut files_by_version: HashMap<i64, Vec<String>> = HashMap::new();
    let mut tv = table.clone();
    for i in 0..=max_version {
        tv.load_version(i).await?;
        let files: Vec<String> = tv.get_files_iter()?.map(|path| path.to_string()).collect();
        println!("version:{}\n, {:#?}", i, files);
        files_by_version.insert(i, files);
    }
    
    let versions_to_keep = vec![1, 2];
    let mut keep_files = HashSet::new();
    for version in versions_to_keep.iter() {
        if let Some(files) = files_by_version.get(&version) {
            for file in files {
                keep_files.insert(file.clone());
            }
        }
    }

    println!("###############################################################################");
    println!("keep file list:");
    println!("versions_to_keep:{:#?}", versions_to_keep);
    println!("keep_files:{:#?}", keep_files);


    //let (_table, stream) = DeltaOps(table).load().await?;
    //let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    //println!("{data:?}");

    Ok(())
}
