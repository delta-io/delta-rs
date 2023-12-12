//! expressions.

use std::{
    collections::HashSet,
    fmt::{Display, Formatter},
};

use self::scalars::Scalar;

pub mod scalars;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A binary operator.
pub enum BinaryOperator {
    /// Logical And
    And,
    /// Logical Or
    Or,
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

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
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
    // TODO: support more expressions, such as IS IN, LIKE, etc.
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(l) => write!(f, "{}", l),
            Self::Column(name) => write!(f, "Column({})", name),
            Self::BinaryOperation { op, left, right } => {
                match op {
                    // OR requires parentheses
                    BinaryOperator::Or => write!(f, "({} OR {})", left, right),
                    _ => write!(f, "{} {} {}", left, op, right),
                }
            }
            Self::UnaryOperation { op, expr } => match op {
                UnaryOperator::Not => write!(f, "NOT {}", expr),
                UnaryOperator::IsNull => write!(f, "{} IS NULL", expr),
            },
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

    fn binary_op_impl(self, other: Self, op: BinaryOperator) -> Self {
        Self::BinaryOperation {
            op,
            left: Box::new(self),
            right: Box::new(other),
        }
    }

    /// Create a new expression `self == other`
    pub fn eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Equal)
    }

    /// Create a new expression `self != other`
    pub fn ne(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::NotEqual)
    }

    /// Create a new expression `self < other`
    pub fn lt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThan)
    }

    /// Create a new expression `self > other`
    pub fn gt(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThan)
    }

    /// Create a new expression `self >= other`
    pub fn gt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::GreaterThanOrEqual)
    }

    /// Create a new expression `self <= other`
    pub fn lt_eq(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::LessThanOrEqual)
    }

    /// Create a new expression `self AND other`
    pub fn and(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::And)
    }

    /// Create a new expression `self OR other`
    pub fn or(self, other: Self) -> Self {
        self.binary_op_impl(other, BinaryOperator::Or)
    }

    fn walk(&self) -> impl Iterator<Item = &Self> + '_ {
        let mut stack = vec![self];
        std::iter::from_fn(move || {
            let expr = stack.pop()?;
            match expr {
                Self::Literal(_) => {}
                Self::Column { .. } => {}
                Self::BinaryOperation { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                Self::UnaryOperation { expr, .. } => {
                    stack.push(expr);
                }
            }
            Some(expr)
        })
    }
}

impl std::ops::Add<Expression> for Expression {
    type Output = Self;

    fn add(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Plus)
    }
}

impl std::ops::Sub<Expression> for Expression {
    type Output = Self;

    fn sub(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Minus)
    }
}

impl std::ops::Mul<Expression> for Expression {
    type Output = Self;

    fn mul(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Multiply)
    }
}

impl std::ops::Div<Expression> for Expression {
    type Output = Self;

    fn div(self, rhs: Expression) -> Self::Output {
        self.binary_op_impl(rhs, BinaryOperator::Divide)
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
                "Column(x) >= 2 AND Column(x) <= 10",
            ),
            (
                col_ref
                    .clone()
                    .gt(Expr::literal(2))
                    .or(col_ref.clone().lt(Expr::literal(10))),
                "(Column(x) > 2 OR Column(x) < 10)",
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
