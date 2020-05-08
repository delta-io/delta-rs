#![recursion_limit="1024"]

#[macro_use]
extern crate helix;
extern crate delta;

use delta::DeltaTable;
use std::sync::Arc;


ruby! {
    class Table {
        struct {
            table_path: String,
            // TODO
            // actual: Arc<DeltaTable>,
        }

        def initialize(helix, table_path: String) {
            println!("initializing with {}", table_path);

            let table = delta::open_table(&table_path).unwrap();
            let actual = Arc::new(table);

            Table {
                helix,
                table_path,
                // TODO
                // actual,
            }
        }
    }
}
