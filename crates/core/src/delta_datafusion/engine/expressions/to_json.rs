use std::sync::Arc;
use std::{any::Any, sync::LazyLock};

use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, scalar_doc_sections::DOC_SECTION_STRUCT,
};
use delta_kernel::engine::arrow_expression::evaluate_expression::to_json as to_json_kernel;

pub fn to_json() -> Arc<ScalarUDF> {
    static INSTANCE: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(ToJson::new())));
    Arc::clone(&INSTANCE)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToJson {
    signature: Signature,
}

impl ToJson {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Stable),
        }
    }
}

static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_STRUCT,
        "Serialize data as a JSON string.",
        "to_json(<data>)",
    )
    .with_argument("data", "The data to be converted to JSON format.")
    .build()
});

fn get_doc() -> &'static Documentation {
    &DOCUMENTATION
}

impl ScalarUDFImpl for ToJson {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let Some(data) = args.first().map(|c| c.to_array(number_rows)).transpose()? else {
            return Err(DataFusionError::Internal(
                "to_json requires one argument".to_string(),
            ));
        };
        Ok(ColumnarValue::Array(to_json_kernel(&data)?))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_doc())
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The function preserves the order of its argument.
        Ok(input[0].sort_properties)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Float64Array, Int8Array, RecordBatch};
    use arrow_array::StructArray;
    use datafusion::{
        assert_batches_eq,
        prelude::{SessionContext, col},
    };

    use super::*;

    #[tokio::test]
    async fn test_to_json() -> Result<(), Box<dyn std::error::Error>> {
        let long: ArrayRef = Arc::new(Float64Array::from(vec![100.0, -122.4783, -122.4783]));
        let lat: ArrayRef = Arc::new(Float64Array::from(vec![45.0, 37.8199, 37.8199]));
        let res: ArrayRef = Arc::new(Int8Array::from(vec![6, 13, 16]));
        let struct_array: ArrayRef =
            Arc::new(StructArray::from(RecordBatch::try_from_iter(vec![
                ("long", long),
                ("lat", lat),
                ("res", res),
            ])?));
        let batch = RecordBatch::try_from_iter(vec![("geometry", struct_array)])?;

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;

        let df = ctx.table("t").await?;
        let df = df.select(vec![to_json().call(vec![col("geometry")]).alias("json")])?;

        let results = df.collect().await?;
        let expected = vec![
            r#"+-------------------------------------------+"#,
            r#"| json                                      |"#,
            r#"+-------------------------------------------+"#,
            r#"| {"long":100.0,"lat":45.0,"res":6}         |"#,
            r#"| {"long":-122.4783,"lat":37.8199,"res":13} |"#,
            r#"| {"long":-122.4783,"lat":37.8199,"res":16} |"#,
            r#"+-------------------------------------------+"#,
        ];
        assert_batches_eq!(expected, &results);

        Ok(())
    }
}
