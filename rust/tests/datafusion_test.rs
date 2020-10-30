extern crate arrow;
#[cfg(feature = "datafusion-ext")]
extern crate datafusion;
extern crate deltalake;

#[cfg(feature = "datafusion-ext")]
use self::arrow::array::UInt64Array;
#[cfg(feature = "datafusion-ext")]
use self::datafusion::execution::context::ExecutionContext;

#[test]
#[cfg(feature = "datafusion-ext")]
fn test_datafusion_simple_query() {
    let mut ctx = ExecutionContext::new();
    let table = deltalake::open_table("./tests/data/simple_table").unwrap();
    ctx.register_table("demo", Box::new(table));

    let sql = "SELECT id FROM demo WHERE id > 5";
    let plan = ctx.create_logical_plan(&sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan, 1024 * 1024).unwrap();

    let results = ctx.collect(plan.as_ref()).unwrap();

    let results = results
        .iter()
        .filter(|batch| batch.num_rows() > 0)
        .flat_map(|batch| {
            UInt64Array::from(batch.column(0).data())
                .value_slice(0, 1)
                .iter()
                .map(|v| *v)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<u64>>();
    assert_eq!(results, vec![7u64, 9u64])
}
