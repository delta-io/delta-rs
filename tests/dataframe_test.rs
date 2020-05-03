use arrow::array::UInt64Array;
use delta::DeltaDataframe;
use rust_dataframe::dataframe::DataFrame;

#[test]
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
