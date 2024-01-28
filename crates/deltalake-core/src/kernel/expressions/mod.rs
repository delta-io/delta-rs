//! expressions.

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema as ArrowSchema;
use itertools::Itertools;

use self::eval::evaluate_expression;
use super::{DataType, DeltaResult, SchemaRef};

pub use self::scalars::*;

mod eval;
mod scalars;

/// Interface for implementing an Expression evaluator.
///
/// It contains one Expression which can be evaluated on multiple ColumnarBatches.
/// Connectors can implement this interface to optimize the evaluation using the
/// connector specific capabilities.
pub trait ExpressionEvaluator {
    /// Evaluate the expression on given ColumnarBatch data.
    ///
    /// Contains one value for each row of the input.
    /// The data type of the output is same as the type output of the expression this evaluator is using.
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<ArrayRef>;
}

/// Provides expression evaluation capability to Delta Kernel.
///
/// Delta Kernel can use this client to evaluate predicate on partition filters,
/// fill up partition column values and any computation on data using Expressions.
pub trait ExpressionHandler {
    /// Create an [`ExpressionEvaluator`] that can evaluate the given [`Expression`]
    /// on columnar batches with the given [`Schema`] to produce data of [`DataType`].
    ///
    /// # Parameters
    ///
    /// - `schema`: Schema of the input data.
    /// - `expression`: Expression to evaluate.
    /// - `output_type`: Expected result data type.
    ///
    /// [`Schema`]: crate::schema::StructType
    /// [`DataType`]: crate::schema::DataType
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator>;
}

/// Default implementation of [`ExpressionHandler`] that uses [`evaluate_expression`]
#[derive(Debug)]
pub struct ArrowExpressionHandler {}

impl ExpressionHandler for ArrowExpressionHandler {
    fn get_evaluator(
        &self,
        schema: SchemaRef,
        expression: Expression,
        output_type: DataType,
    ) -> Arc<dyn ExpressionEvaluator> {
        Arc::new(DefaultExpressionEvaluator {
            input_schema: schema,
            expression: Box::new(expression),
            output_type,
        })
    }
}

/// Default implementation of [`ExpressionEvaluator`] that uses [`evaluate_expression`]
#[derive(Debug)]
pub struct DefaultExpressionEvaluator {
    input_schema: SchemaRef,
    expression: Box<Expression>,
    output_type: DataType,
}

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate(&self, batch: &RecordBatch) -> DeltaResult<ArrayRef> {
        let _input_schema: ArrowSchema = self.input_schema.as_ref().try_into()?;
        // TODO: make sure we have matching schemas for validation
        // if batch.schema().as_ref() != &input_schema {
        //     return Err(Error::Generic(format!(
        //         "input schema does not match batch schema: {:?} != {:?}",
        //         input_schema,
        //         batch.schema()
        //     )));
        // };
        evaluate_expression(&self.expression, batch, Some(&self.output_type))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A binary operator.
pub enum BinaryOperator {
    /// Arithmetic Plus
    Plus,
    /// Arithmetic Minus
    Minus,
    /// Arithmetic Multiply
    Multiply,
    /// Arithmetic Divide
    Divide,
    /// Comparison Less Than
    LessThan,
    /// Comparison Less Than Or Equal
    LessThanOrEqual,
    /// Comparison Greater Than
    GreaterThan,
    /// Comparison Greater Than Or Equal
    GreaterThanOrEqual,
    /// Comparison Equal
    Equal,
    /// Comparison Not Equal
    NotEqual,
}

/// Variadic operators
#[derive(Debug, Clone, PartialEq)]
pub enum VariadicOperator {
    /// AND
    And,
    /// OR
    Or,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            // Self::And => write!(f, "AND"),
            // Self::Or => write!(f, "OR"),
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::LessThan => write!(f, "<"),
            Self::LessThanOrEqual => write!(f, "<="),
            Self::GreaterThan => write!(f, ">"),
            Self::GreaterThanOrEqual => write!(f, ">="),
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A unary operator.
pub enum UnaryOperator {
    /// Unary Not
    Not,
    /// Unary Is Null
    IsNull,
}

/// A SQL expression.
///
/// These expressions do not track or validate data types, other than the type
/// of literals. It is up to the expression evaluator to validate the
/// expression against a schema and add appropriate casts as required.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A literal value.
    Literal(Scalar),
    /// A column reference by name.
    Column(String),
    ///
    Struct(Vec<Expression>),
    /// A binary operation.
    BinaryOperation {
        /// The operator.
        op: BinaryOperator,
        /// The left-hand side of the operation.
        left: Box<Expression>,
        /// The right-hand side of the operation.
        right: Box<Expression>,
    },
    /// A unary operation.
    UnaryOperation {
        /// The operator.
        op: UnaryOperator,
        /// The expression.
        expr: Box<Expression>,
    },
    /// A variadic operation.
    VariadicOperation {
        /// The operator.
        op: VariadicOperator,
        /// The expressions.
        exprs: Vec<Expression>,
    },
    /// A NULLIF expression.
    NullIf {
        /// The expression to evaluate.
        expr: Box<Expression>,
        /// The expression to compare against.
        if_expr: Box<Expression>,
    },
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

impl<T: Into<Scalar>> From<T> for Expression {
    fn from(value: T) -> Self {
        Self::literal(value)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column(name) => write!(f, "Column({})", name),
            Self::Struct(exprs) => write!(
                f,
                "Struct({})",
                &exprs.iter().map(|e| format!("{e}")).join(", ")
            ),
            Self::BinaryOperation { op, left, right } => write!(f, "{} {} {}", left, op, right),
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {}", expr),
                UnaryOperator::IsNull => write!(f, "{} IS NULL", expr),
            },
            Self::VariadicOperation { op, exprs } => match op {
                VariadicOperator::And => {
                    write!(
                        f,
                        "AND({})",
                        &exprs.iter().map(|e| format!("{e}")).join(", ")
                    )
                }
                VariadicOperator::Or => {
                    write!(
                        f,
                        "OR({})",
                        &exprs.iter().map(|e| format!("{e}")).join(", ")
                    )
                }
            },
            Self::NullIf { expr, if_expr } => write!(f, "NULLIF({}, {})", expr, if_expr),
        }
    }
}

impl Expression {
    /// Returns a set of columns referenced by this expression.
    pub fn references(&self) -> HashSet<&str> {
        let mut set = HashSet::new();

        for expr in self.walk() {
            if let Self::Column(name) = expr {
                set.insert(name.as_str());
            }
        }

        set
    }

    /// Create an new expression for a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Create a new expression for a literal value
    pub fn literal(value: impl Into<Scalar>) -> Self {
        Self::Literal(value.into())
    }

    /// Create a new expression for a struct
    pub fn struct_expr(exprs: impl IntoIterator<Item = Self>) -> Self {
        Self::Struct(exprs.into_iter().collect())
    }

    /// Create a new expression for a unary operation
    pub fn unary(op: UnaryOperator, expr: impl Into<Expression>) -> Self {
        Self::UnaryOperation {
            op,
            expr: Box::new(expr.into()),
        }
    }

    /// Create a new expression for a binary operation
    pub fn binary(
        op: BinaryOperator,
        lhs: impl Into<Expression>,
        rhs: impl Into<Expression>,
    ) -> Self {
        Self::BinaryOperation {
            op,
            left: Box::new(lhs.into()),
            right: Box::new(rhs.into()),
        }
    }

    /// Create a new expression for a variadic operation
    pub fn variadic(op: VariadicOperator, other: impl IntoIterator<Item = Self>) -> Self {
        let mut exprs = other.into_iter().collect::<Vec<_>>();
        if exprs.is_empty() {
            // TODO this might break if we introduce new variadic operators?
            return Self::literal(matches!(op, VariadicOperator::And));
        }
        if exprs.len() == 1 {
            return exprs.pop().unwrap();
        }
        Self::VariadicOperation { op, exprs }
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::Equal, self, other)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: Self) -> Self {
        Self::binary(BinaryOperator::NotEqual, self, other)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: Self) -> Self {
        Self::binary(BinaryOperator::LessThan, self, other)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: Self) -> Self {
        Self::binary(BinaryOperator::GreaterThan, self, other)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::GreaterThanOrEqual, self, other)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: Self) -> Self {
        Self::binary(BinaryOperator::LessThanOrEqual, self, other)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: Self) -> Self {
        self.and_many([other])
    }

    /// Create a new expression `self AND others`
    pub fn and_many(self, other: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::And, std::iter::once(self).chain(other))
    }

    /// Create a new expression `self AND other`
    pub fn or(self, other: Self) -> Self {
        self.or_many([other])
    }

    /// Create a new expression `self OR other`
    pub fn or_many(self, other: impl IntoIterator<Item = Self>) -> Self {
        Self::variadic(VariadicOperator::Or, std::iter::once(self).chain(other))
    }

    /// Create a new expression `self IS NULL`
    pub fn is_null(self) -> Self {
        Self::unary(UnaryOperator::IsNull, self)
    }

    /// Create a new expression `NULLIF(self, other)`
    pub fn null_if(self, other: Self) -> Self {
        Self::NullIf {
            expr: Box::new(self),
            if_expr: Box::new(other),
        }
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::Struct(exprs) => {
                    stack.extend(exprs.iter());
                }
                Self::BinaryOperation { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::UnaryOperation { expr, .. } => {
                    stack.push(expr);
                }
                Self::VariadicOperation { op, exprs } => match op {
                    VariadicOperator::And | VariadicOperator::Or => {
                        stack.extend(exprs.iter());
                    }
                },
                Self::NullIf { expr, if_expr } => {
                    stack.push(expr);
                    stack.push(if_expr);
                }
            }
            Some(expr)
        })
    }
}

impl std::ops::Add<Expression> for Expression {
    type Output = Self;

    fn add(self, rhs: Expression) -> Self::Output {
        Self::binary(BinaryOperator::Plus, self, rhs)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = Self;

    fn sub(self, rhs: Expression) -> Self::Output {
        Self::binary(BinaryOperator::Minus, self, rhs)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = Self;

    fn mul(self, rhs: Expression) -> Self::Output {
        Self::binary(BinaryOperator::Multiply, self, rhs)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = Self;

    fn div(self, rhs: Expression) -> Self::Output {
        Self::binary(BinaryOperator::Divide, self, rhs)
    }
}

#[cfg(test)]
mod tests {
    use super::Expression as Expr;

    #[test]
    fn test_expression_format() {
        let col_ref = Expr::column("x");
        let cases = [
            (col_ref.clone(), "Column(x)"),
            (col_ref.clone().eq(Expr::literal(2)), "Column(x) = 2"),
            (
                col_ref
                    .clone()
                    .gt_eq(Expr::literal(2))
                    .and(col_ref.clone().lt_eq(Expr::literal(10))),
                "AND(Column(x) >= 2, Column(x) <= 10)",
            ),
            (
                col_ref
                    .clone()
                    .gt(Expr::literal(2))
                    .or(col_ref.clone().lt(Expr::literal(10))),
                "OR(Column(x) > 2, Column(x) < 10)",
            ),
            (
                (col_ref.clone() - Expr::literal(4)).lt(Expr::literal(10)),
                "Column(x) - 4 < 10",
            ),
            (
                (col_ref.clone() + Expr::literal(4)) / Expr::literal(10) * Expr::literal(42),
                "Column(x) + 4 / 10 * 42",
            ),
            (col_ref.eq(Expr::literal("foo")), "Column(x) = 'foo'"),
        ];

        for (expr, expected) in cases {
            let result = format!("{}", expr);
            assert_eq!(result, expected);
        }
    }
}
