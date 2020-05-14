use std::fs::File;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use crossbeam::channel::{unbounded, Receiver, SendError, Sender};
use datafusion;
use datafusion::datasource::{ScanResult, TableProvider};
use datafusion::error::ExecutionError;
use datafusion::execution::physical_plan::{BatchIterator, Partition};
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;

use crate::delta;
use crate::schema;

struct ParquetPartition {
    iterator: Arc<Mutex<dyn BatchIterator>>,
}

impl ParquetPartition {
    /// Create a new Parquet partition
    pub fn new(
        filename: &str,
        projection: Vec<usize>,
        schema: Arc<Schema>,
        batch_size: usize,
    ) -> Self {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = unbounded();
        let (response_tx, response_rx): (
            Sender<datafusion::error::Result<Option<RecordBatch>>>,
            Receiver<datafusion::error::Result<Option<RecordBatch>>>,
        ) = unbounded();

        let filename = filename.to_string();

        thread::spawn(move || {
            //TODO error handling, remove unwraps

            // open file
            let file = File::open(&filename).unwrap();
            match SerializedFileReader::new(file) {
                Ok(file_reader) => {
                    if file_reader.metadata().num_row_groups() == 0 {
                        // skip empty parquet files
                        response_tx.send(Ok(None)).unwrap();
                    } else {
                        let file_reader = Rc::new(file_reader);
                        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);

                        match arrow_reader.get_record_reader_by_columns(projection, batch_size) {
                            Ok(mut batch_reader) => {
                                while let Ok(_) = request_rx.recv() {
                                    match batch_reader.next_batch() {
                                        Ok(Some(batch)) => {
                                            response_tx.send(Ok(Some(batch))).unwrap();
                                        }
                                        Ok(None) => {
                                            response_tx.send(Ok(None)).unwrap();
                                            break;
                                        }
                                        Err(e) => {
                                            response_tx
                                                .send(Err(ExecutionError::General(format!(
                                                    "{:?}",
                                                    e
                                                ))))
                                                .unwrap();
                                            break;
                                        }
                                    }
                                }
                            }

                            Err(e) => {
                                response_tx
                                    .send(Err(ExecutionError::General(format!(
                                        "record batch: {:#?}",
                                        e
                                    ))))
                                    .unwrap();
                            }
                        }
                    }
                }

                Err(e) => {
                    response_tx
                        .send(Err(ExecutionError::General(format!(
                            "new file error: {:#?}",
                            e
                        ))))
                        .unwrap();
                }
            }
        });
        let iterator = Arc::new(Mutex::new(ParquetIterator {
            schema,
            request_tx,
            response_rx,
        }));

        Self { iterator }
    }
}

impl Partition for ParquetPartition {
    fn execute(&self) -> datafusion::error::Result<Arc<Mutex<dyn BatchIterator>>> {
        Ok(self.iterator.clone())
    }
}

struct ParquetIterator {
    schema: Arc<Schema>,
    request_tx: Sender<()>,
    response_rx: Receiver<datafusion::error::Result<Option<RecordBatch>>>,
}

impl BatchIterator for ParquetIterator {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn next(&mut self) -> datafusion::error::Result<Option<RecordBatch>> {
        match self.request_tx.send(()) {
            Ok(_) => match self.response_rx.recv() {
                Ok(batch) => batch,
                Err(e) => Err(ExecutionError::General(format!(
                    "Error receiving batch: {:?}",
                    e
                ))),
            },
            // SendError means receiver exited and disconnected
            Err(SendError(())) => Ok(None),
        }
    }
}

impl TableProvider for delta::DeltaTable {
    fn schema(&self) -> Arc<Schema> {
        Arc::new(<Schema as From<&schema::Schema>>::from(
            delta::DeltaTable::schema(&self).unwrap(),
        ))
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> datafusion::error::Result<Vec<ScanResult>> {
        let schema = TableProvider::schema(self);

        let projection = match projection.clone() {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let partitions = self
            .get_file_paths()
            .iter()
            .map(|fpath| {
                Arc::new(ParquetPartition::new(
                    &fpath,
                    projection.clone(),
                    schema.clone(),
                    batch_size,
                )) as Arc<dyn Partition>
            })
            .collect::<Vec<_>>();

        let iterators = partitions
            .iter()
            .map(|p| p.execute())
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        Ok(iterators)
    }
}

impl From<&schema::Schema> for Schema {
    fn from(s: &schema::Schema) -> Self {
        let fields = s
            .get_fields()
            .iter()
            .map(|field| <Field as From<&schema::SchemaField>>::from(field))
            .collect();

        Schema::new(fields)
    }
}

impl From<&schema::SchemaField> for Field {
    fn from(f: &schema::SchemaField) -> Self {
        Field::new(f.get_name(), DataType::from(f.get_type()), f.is_nullable())
    }
}

impl From<&schema::SchemaDataType> for DataType {
    fn from(t: &schema::SchemaDataType) -> Self {
        match t {
            schema::SchemaDataType::primitive(p) => {
                match p.as_str() {
                    "string" => DataType::Utf8,
                    "long" => DataType::Int64, // undocumented type
                    "integer" => DataType::Int32,
                    "short" => DataType::Int16,
                    "byte" => DataType::Int8,
                    "float" => DataType::Float32,
                    "double" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    "binary" => DataType::Binary,
                    "date" => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone.
                        panic!("date is not supported in arrow");
                    }
                    "timestamp" => {
                        // Microsecond precision timestamp without a timezone.
                        DataType::Time64(TimeUnit::Microsecond)
                    }
                    s @ _ => {
                        panic!("unexpected delta schema type: {}", s);
                    }
                }
            }
            schema::SchemaDataType::r#struct(s) => DataType::Struct(
                s.get_fields()
                    .iter()
                    .map(|f| <Field as From<&schema::SchemaField>>::from(f))
                    .collect(),
            ),
            schema::SchemaDataType::array(a) => {
                DataType::List(Box::new(<DataType as From<&schema::SchemaDataType>>::from(
                    a.get_element_type(),
                )))
            }
            schema::SchemaDataType::map(m) => DataType::Dictionary(
                Box::new(<DataType as From<&schema::SchemaDataType>>::from(
                    m.get_key_type(),
                )),
                Box::new(<DataType as From<&schema::SchemaDataType>>::from(
                    m.get_value_type(),
                )),
            ),
        }
    }
}
