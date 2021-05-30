#![recursion_limit = "1024"]

extern crate deltalake;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rutie;

use deltalake::DeltaTable;
use rutie::{AnyObject, Array, Class, Integer, Object, RString};
use std::sync::Arc;

pub struct TableData {
    table_uri: String,
    actual: Arc<DeltaTable>,
}

impl TableData {
    fn new(table_uri: String) -> Self {
        println!("initializing with {}", table_uri);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let table = rt.block_on(deltalake::open_table(&table_uri)).unwrap();
        let actual = Arc::new(table);

        Self { table_uri, actual }
    }

    fn table_uri(&self) -> &str {
        &self.table_uri
    }

    fn version(&self) -> i64 {
        self.actual.version
    }

    fn files(&self) -> Vec<String> {
        self.actual
            .get_files_iter()
            .map(|f| f.to_string())
            .collect()
    }
}

wrappable_struct!(TableData, TableDataWrapper, TABLE_DATA_WRAPPER);

class!(Table);

methods!(
    Table,
    rtself,
    fn ruby_table_new(table_uri: RString) -> AnyObject {
        let table_data = TableData::new(table_uri.unwrap().to_string());

        Class::from_existing("Table").wrap_data(table_data, &*TABLE_DATA_WRAPPER)
    },
    fn ruby_table_uri() -> RString {
        let table_uri = rtself.get_data(&*TABLE_DATA_WRAPPER).table_uri();

        RString::new_utf8(table_uri)
    },
    fn ruby_version() -> Integer {
        let version = rtself.get_data(&*TABLE_DATA_WRAPPER).version();

        Integer::new(version)
    },
    fn ruby_files() -> Array {
        let files = rtself.get_data(&*TABLE_DATA_WRAPPER).files();

        let mut array = Array::with_capacity(files.len());

        for file in files {
            array.push(RString::new_utf8(&file));
        }

        array
    }
);

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_table() {
    let data_class = Class::from_existing("Object");

    Class::new("Table", Some(&data_class)).define(|klass| {
        klass.def_self("new", ruby_table_new);

        klass.def("table_uri", ruby_table_uri);
        klass.def("version", ruby_version);
        klass.def("files", ruby_files);
    });
}
