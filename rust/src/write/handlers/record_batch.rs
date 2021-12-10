//! Handle RecordBatch messages when writing to delta tables
#[cfg(test)]
mod tests {
    use arrow::{
        array::{StringArray, UInt32Array},
        compute::{lexicographical_partition_ranges, lexsort_to_indices, take, SortColumn},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn test_lex_sort_single_column() {
        let batches = get_record_batch();

        for batch in batches {
            // println!("{:?}", batch);
            let sort_cols = batch
                .columns()
                .iter()
                .map(|arr| SortColumn {
                    values: arr.clone(),
                    options: None,
                })
                .collect::<Vec<_>>();

            // let sorted = lexsort(sort_cols.as_slice(), None).unwrap();

            let indices = lexsort_to_indices(sort_cols.as_slice(), None).unwrap();
            let sorted_values = sort_cols
                .iter()
                .map(|c| SortColumn {
                    values: take(c.values.as_ref(), &indices, None).unwrap(),
                    options: None,
                })
                .collect::<Vec<_>>();

            // let sort_sorted = sorted
            //     .iter()
            //     .map(|arr| SortColumn {
            //         values: arr.clone(),
            //         options: None,
            //     })
            //     .collect::<Vec<_>>();

            // println!("{:?}", sorted);
            println!("{:?}", indices);

            let ranges = lexicographical_partition_ranges(sorted_values.as_slice())
                .unwrap()
                .map(|range| {
                    let idx: UInt32Array = (range.start..range.end)
                        .map(|i| Some(indices.value(i)))
                        .into_iter()
                        .collect();
                    batch
                        .columns()
                        .iter()
                        .map(move |col| take(col.as_ref(), &idx, None).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            println!("{:?}", ranges);
        }
    }

    fn get_record_batch() -> Vec<RecordBatch> {
        // let int_values = Int32Array::from(vec![42, 44, 46, 48, 50, 52, 54, 56, 148, 150, 152]);
        let id_values =
            StringArray::from(vec!["A", "C", "A", "B", "B", "A", "A", "B", "A", "A", "B"]);
        let modified_values = StringArray::from(vec![
            "2021-02-02",
            "2021-02-02",
            "2021-02-02",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
            "2021-02-01",
        ]);

        // expected results from parsing json payload
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("modified", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(modified_values), Arc::new(id_values)],
        )
        .unwrap();

        vec![batch]
    }

    // fn test_lex_sort_arrays(
    //     input: Vec<SortColumn>,
    //     expected_output: Vec<ArrayRef>,
    //     limit: Option<usize>,
    // ) {
    //     let sorted = lexsort(&input, limit).unwrap();   //
    //     for (result, expected) in sorted.iter().zip(expected_output.iter()) {
    //         assert_eq!(result, expected);
    //     }
    // }
}
