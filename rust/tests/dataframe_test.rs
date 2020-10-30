extern crate arrow;
extern crate deltalake;
#[cfg(feature = "rust-dataframe-ext")]
extern crate rust_dataframe;

#[cfg(feature = "rust-dataframe-ext")]
use self::arrow::array::UInt64Array;
#[cfg(feature = "rust-dataframe-ext")]
use self::deltalake::DeltaDataframe;
#[cfg(feature = "rust-dataframe-ext")]
use self::rust_dataframe::dataframe::DataFrame;

#[test]
#[cfg(feature = "rust-dataframe-ext")]
fn dataframe_from_delta_table() {
    let df = DataFrame::from_delta_table("./tests/data/simple_table").unwrap();
    assert_eq!(1, df.num_columns());
    assert_eq!(3, df.num_rows());

    assert_eq!(
        df.column_by_name("id")
            .data()
            .chunks()
            .iter()
            .map(|chunk| UInt64Array::from(chunk.data()).value_slice(0, 1)[0])
            .collect::<Vec<u64>>(),
        vec![5u64, 7u64, 9u64],
    );
}

#[test]
#[cfg(feature = "rust-dataframe-ext")]
fn dataframe_from_delta_table_with_time_travel() {
    // start with 0..5
    let mut df = DataFrame::from_delta_table_with_version("./tests/data/simple_table", 0).unwrap();
    assert_eq!(1, df.num_columns());
    assert_eq!(5, df.num_rows());
    assert_eq!(
        df.column_by_name("id")
            .data()
            .chunks()
            .iter()
            .map(|chunk| UInt64Array::from(chunk.data()).value_slice(0, 1)[0])
            .collect::<Vec<u64>>(),
        (0u64..5u64).collect::<Vec<u64>>(),
    );

    // upsert with 0..20
    df = DataFrame::from_delta_table_with_version("./tests/data/simple_table", 1).unwrap();
    assert_eq!(1, df.num_columns());
    assert_eq!(20, df.num_rows());
    let mut expected = df
        .column_by_name("id")
        .data()
        .chunks()
        .iter()
        .map(|chunk| UInt64Array::from(chunk.data()).value_slice(0, 1)[0])
        .collect::<Vec<u64>>();
    expected.sort();
    assert_eq!(expected, (0u64..20u64).collect::<Vec<u64>>());

    // overwrite with 5..10
    df = DataFrame::from_delta_table_with_version("./tests/data/simple_table", 2).unwrap();
    assert_eq!(1, df.num_columns());
    assert_eq!(5, df.num_rows());
    assert_eq!(
        df.column_by_name("id")
            .data()
            .chunks()
            .iter()
            .map(|chunk| UInt64Array::from(chunk.data()).value_slice(0, 1)[0])
            .collect::<Vec<u64>>(),
        (5u64..10u64).collect::<Vec<u64>>(),
    );

    // add 100 to even rows
    df = DataFrame::from_delta_table_with_version("./tests/data/simple_table", 3).unwrap();
    assert_eq!(1, df.num_columns());
    assert_eq!(5, df.num_rows());
    assert_eq!(
        df.column_by_name("id")
            .data()
            .chunks()
            .iter()
            .map(|chunk| UInt64Array::from(chunk.data()).value_slice(0, 1)[0])
            .collect::<Vec<u64>>(),
        vec![5u64, 7u64, 9u64, 106u64, 108u64],
    );
}
