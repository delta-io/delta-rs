#![recursion_limit="1024"]

#[macro_use]
extern crate helix;
extern crate deltalake;

use deltalake::DeltaTable;
use std::sync::Arc;


ruby! {
    class Table {
        struct {
            table_path: String,
            actual: Arc<DeltaTable>,
        }

        def initialize(helix, table_path: String) {
            println!("initializing with {}", table_path);

            let table = deltalake::open_table(&table_path).unwrap();
            let actual = Arc::new(table);

            Table {
                helix,
                table_path,
                actual,
            }
        }

        def table_path(&self) -> String {
            self.table_path.clone()
        }

        def version(&self) -> i64 {
            self.actual.version
        }

        def files(&self) -> Vec<String> {
            self.actual.get_files().to_vec()
        }
    }
}
