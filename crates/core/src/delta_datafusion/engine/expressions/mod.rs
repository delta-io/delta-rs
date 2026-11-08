pub(crate) use self::to_kernel::*;
pub(crate) use deltalake_kernel_executor::expressions::*;

mod to_kernel;

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use datafusion::logical_expr::{Expr, col, lit};
    use delta_kernel::schema::{DataType, StructField, StructType};

    use super::*;

    /// Columns `a`..`d` the round-trip tests resolve against.
    fn test_schema() -> StructType {
        StructType::try_new(
            ["a", "b", "c", "d"].map(|name| StructField::nullable(name, DataType::INTEGER)),
        )
        .unwrap()
    }

    fn roundtrip(df_expr: &Expr) -> Expr {
        let delta_expr = to_delta_expression(df_expr).unwrap();
        to_datafusion_expr(&delta_expr, &test_schema(), None).unwrap()
    }

    #[test]
    fn test_roundtrip_simple_and() {
        let df_expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_nested_and() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .and(col("c").eq(lit(3)))
            .and(col("d").eq(lit(4)));
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_mixed_and_or() {
        let df_expr = col("a")
            .eq(lit(1))
            .and(col("b").eq(lit(2)))
            .or(col("c").eq(lit(3)).and(col("d").eq(lit(4))));
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_unary() {
        let df_expr = !col("a").eq(lit(1));
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_is_null() {
        let df_expr = col("a").is_null();
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_binary_ops() {
        let df_expr = col("a") + col("b") * col("c");
        assert_eq!(df_expr, roundtrip(&df_expr));
    }

    #[test]
    fn test_roundtrip_comparison_ops() {
        let df_expr = col("a").gt(col("b")).and(col("c").gt(col("d")).not());
        assert_eq!(df_expr, roundtrip(&df_expr));
    }
}
