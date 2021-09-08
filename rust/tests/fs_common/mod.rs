use deltalake::action::Protocol;
use deltalake::{storage, DeltaTable, DeltaTableMetaData, Schema};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub fn cleanup_dir_except<P: AsRef<Path>>(path: P, ignore_files: Vec<String>) {
    for p in fs::read_dir(path).unwrap() {
        if let Ok(d) = p {
            let path = d.path();
            let name = d.path().file_name().unwrap().to_str().unwrap().to_string();

            if !ignore_files.contains(&name) && !name.starts_with(".") {
                fs::remove_file(&path).unwrap();
            }
        }
    }
}

pub async fn create_test_table(
    path: &str,
    schema: Schema,
    config: HashMap<String, Option<String>>,
) -> DeltaTable {
    let backend = storage::get_backend_for_uri(path).unwrap();
    let mut table = DeltaTable::new(path, backend).unwrap();
    let md = DeltaTableMetaData::new(None, None, None, schema, Vec::new(), config);
    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };
    table.create(md, protocol, None).await.unwrap();
    table
}
