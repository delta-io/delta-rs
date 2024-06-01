/*
 * This file is a self-contained example of writing to a Delta table using the Arrow
 * `RecordBatch` API rather than pushing the data through a JSON intermediary
 *
 *
 * This example was originally posted by @rtyler in:
 *      <https://github.com/buoyant-data/demo-recordbatch-writer>
 */
use chrono::prelude::*;
use deltalake::arrow::array::*;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::Path;
use deltalake::*;
use std::sync::Arc;
use tracing::*;

/*
 * The main function gets everything started, but does not contain any meaningful
 * example code for writing to Delta tables
 */
#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    info!("Logger initialized");

    let table_uri = std::env::var("TABLE_URI").map_err(|e| DeltaTableError::GenericError {
        source: Box::new(e),
    })?;
    info!("Using the location of: {:?}", table_uri);

    let table_path = Path::parse(&table_uri)?;

    let maybe_table = deltalake::open_table(&table_path).await;
    let mut table = match maybe_table {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            info!("It doesn't look like our delta table has been created");
            create_initialized_table(&table_path).await
        }
        Err(err) => panic!("{:?}", err),
    };

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut writer = RecordBatchWriter::for_table(&table)
        .expect("Failed to make RecordBatchWriter")
        .with_writer_properties(writer_properties);

    let records = fetch_readings();
    let batch = convert_to_batch(&table, &records);

    writer.write(batch).await?;

    let adds = writer
        .flush_and_commit(&mut table)
        .await
        .expect("Failed to flush write");
    info!("{} adds written", adds);

    Ok(())
}

// Creating a simple type alias for improved readability
type Fahrenheit = i32;

/*
 * WeatherRecord is just a simple example structure to represent a row in the
 * delta table. Imagine a time-series of weather data which is being recorded
 * by a small sensor.
 */
struct WeatherRecord {
    timestamp: DateTime<Utc>,
    temp: Fahrenheit,
    lat: f64,
    long: f64,
}

impl WeatherRecord {
    fn columns() -> Vec<StructField> {
        vec![
            StructField::new(
                "timestamp".to_string(),
                DataType::Primitive(PrimitiveType::TimestampNtz),
                true,
            ),
            StructField::new(
                "temp".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "lat".to_string(),
                DataType::Primitive(PrimitiveType::Double),
                true,
            ),
            StructField::new(
                "long".to_string(),
                DataType::Primitive(PrimitiveType::Double),
                true,
            ),
        ]
    }
}

impl Default for WeatherRecord {
    fn default() -> Self {
        Self {
            timestamp: Utc::now(),
            temp: 72,
            lat: 39.61940984546992,
            long: -119.22916208856955,
        }
    }
}

/*
 * This function just generates a series of 5 temperature readings to be written
 * to the table
 */
fn fetch_readings() -> Vec<WeatherRecord> {
    let mut readings = vec![];

    for i in 1..=5 {
        let mut wx = WeatherRecord::default();
        wx.temp -= i;
        readings.push(wx);
    }
    readings
}

/*
 * The convert to batch function does some of the heavy lifting for writing a
 * `RecordBatch` to a delta table. In essence, the Vec of WeatherRecord needs to
 * turned into a columnar format in order to be written correctly.
 *
 * That is to say that the following example rows:
 *  | ts | temp | lat | long |
 *  | 0  | 72   | 0.0 | 0.0  |
 *  | 1  | 71   | 0.0 | 0.0  |
 *  | 2  | 78   | 0.0 | 0.0  |
 *
 *  Must be converted into a data structure where all timestamps are together,
 *  ```
 *  let ts = vec![0, 1, 2];
 *  let temp = vec![72, 71, 78];
 *  ```
 *
 *  The Arrow Rust array primitives are _very_ fickle and so creating a direct
 *  transformation is quite tricky in Rust, whereas in Python or another loosely
 *  typed language it might be simpler.
 */
fn convert_to_batch(table: &DeltaTable, records: &Vec<WeatherRecord>) -> RecordBatch {
    let metadata = table
        .metadata()
        .expect("Failed to get metadata for the table");
    let arrow_schema = <deltalake::arrow::datatypes::Schema as TryFrom<&StructType>>::try_from(
        &metadata.schema().expect("failed to get schema"),
    )
    .expect("Failed to convert to arrow schema");
    let arrow_schema_ref = Arc::new(arrow_schema);

    let mut ts = vec![];
    let mut temp = vec![];
    let mut lat = vec![];
    let mut long = vec![];

    for record in records {
        ts.push(record.timestamp.timestamp_micros());
        temp.push(record.temp);
        lat.push(record.lat);
        long.push(record.long);
    }

    let arrow_array: Vec<Arc<dyn Array>> = vec![
        Arc::new(TimestampMicrosecondArray::from(ts)),
        Arc::new(Int32Array::from(temp)),
        Arc::new(Float64Array::from(lat)),
        Arc::new(Float64Array::from(long)),
    ];

    RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")
}

/*
 * Pilfered from writer/test_utils.rs in delta-rs. This code will basically create a new Delta
 * Table in an existing directory that doesn't currently contain a Delta table
 */
async fn create_initialized_table(table_path: &Path) -> DeltaTable {
    DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .create()
        .with_columns(WeatherRecord::columns())
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_readings() {
        let readings = fetch_readings();
        assert_eq!(
            5,
            readings.len(),
            "fetch_readings() should return 5 readings"
        );
    }

    #[test]
    fn test_schema() {
        let schema: Schema = WeatherRecord::schema();
        assert_eq!(schema.get_fields().len(), 4, "schema should have 4 fields");
    }
}
