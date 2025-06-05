use datafusion::common::DataFusionError;
use std::collections::VecDeque;
use std::fmt;

use datafusion::sql::parser::{DFParserBuilder, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{ObjectName, Value};
use datafusion::sql::sqlparser::dialect::{keywords::Keyword, Dialect, GenericDialect};
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DescribeOperation {
    Detail,
    History,
    Files,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStatement {
    pub table: ObjectName,
    pub operation: DescribeOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VacuumStatement {
    pub table: ObjectName,
    pub retention_hours: Option<i32>,
    pub dry_run: bool,
}

/// Delta Lake Statement representations.
///
/// Tokens parsed by [`DeltaParser`] are converted into these values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// Datafusion AST node (from datafusion-sql)
    Datafusion(DFStatement),
    /// Extension: `DESCRIBE [HISTORY | DETAIL] table_name`
    Describe(DescribeStatement),
    /// Extension: `VACUUM table_name [RETAIN num HOURS] [DRY RUN]`
    Vacuum(VacuumStatement),
}

impl From<DFStatement> for Statement {
    fn from(value: DFStatement) -> Self {
        Self::Datafusion(value)
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Datafusion(stmt) => write!(f, "{stmt}"),
            Statement::Describe(_) => write!(f, "DESCRIBE TABLE ..."),
            Statement::Vacuum(_) => write!(f, "VACUUM TABLE ..."),
        }
    }
}

/// Delta Lake SQL Parser based on [`sqlparser`](https://crates.io/crates/sqlparser)
///
/// This parser handles Delta Lake specific statements, delegating to
/// [`DFParser`]for other SQL statements.
pub struct DeltaParser<'a> {
    sql: &'a str,
    parser: Parser<'a>,
}

impl<'a> DeltaParser<'a> {
    /// Create a new parser for the specified tokens using the [`GenericDialect`].
    pub fn new(sql: &'a str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        DeltaParser::new_with_dialect(sql, dialect)
    }

    /// Create a new parser for the specified tokens with the
    /// specified dialect.
    pub fn new_with_dialect(sql: &'a str, dialect: &'a dyn Dialect) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(Self {
            sql,
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse a sql string into one or [`Statement`]s using the
    /// [`GenericDialect`].
    pub fn parse_sql(sql: impl AsRef<str>) -> Result<VecDeque<Statement>, ParserError> {
        let dialect: &GenericDialect = &GenericDialect {};
        DeltaParser::parse_sql_with_dialect(sql.as_ref(), dialect)
    }

    /// Parse a SQL string and produce one or more [`Statement`]s with
    /// with the specified dialect.
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<Statement>, ParserError> {
        let mut parser = DeltaParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }

        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(&self, expected: &str, found: TokenWithSpan) -> Result<T, ParserError> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::VACUUM => {
                    self.parser.next_token();
                    self.parse_vacuum()
                }
                _ => self.parse_with_native(),
            },
            _ => self.parse_with_native(),
        }
    }

    /// Use the native parser to parse the statement
    // TODO fix for multiple statememnts and keeping parsers in sync
    fn parse_with_native(&mut self) -> Result<Statement, ParserError> {
        // DataFusion's parser returns DataFusionError as it may also add
        // additional context to the error, so find the underlying ParserError
        // if it exists.
        fn unwrap_err(df_err: DataFusionError) -> DataFusionError {
            if let DataFusionError::Context(_message, inner_error) = df_err {
                unwrap_err(*inner_error)
            } else {
                df_err
            }
        }
        self.parse_with_native_inner()
            .map_err(unwrap_err)
            .map_err(|df_err| {
                if let DataFusionError::SQL(parser_error, _backtrace) = df_err {
                    parser_error
                } else {
                    ParserError::ParserError(df_err.to_string())
                }
            })
    }

    fn parse_with_native_inner(&mut self) -> Result<Statement, DataFusionError> {
        let stmt = DFParserBuilder::new(self.sql).build()?.parse_statement()?;
        self.parser.parse_statement()?;
        Ok(Statement::Datafusion(stmt))
    }

    pub fn parse_vacuum(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parser.parse_object_name(false)?;
        match self.parser.peek_token().token {
            Token::Word(w) => match w.keyword {
                Keyword::RETAIN => {
                    self.parser.next_token();
                    let retention_hours = match self.parser.parse_number_value()?.value {
                        Value::Number(value_str, _) => value_str
                            .parse()
                            .map_err(|_| ParserError::ParserError(format!("Unexpected token {w}"))),
                        _ => Err(ParserError::ParserError(
                            "Expected numeric value for retention hours".to_string(),
                        )),
                    }?;
                    if !self.parser.parse_keyword(Keyword::HOURS) {
                        return Err(ParserError::ParserError(
                            "Expected keyword 'HOURS'".to_string(),
                        ));
                    };
                    Ok(Statement::Vacuum(VacuumStatement {
                        table: table_name,
                        retention_hours: Some(retention_hours),
                        dry_run: self.parser.parse_keywords(&[Keyword::DRY, Keyword::RUN]),
                    }))
                }
                Keyword::DRY => {
                    self.parser.next_token();
                    if self.parser.parse_keyword(Keyword::RUN) {
                        Ok(Statement::Vacuum(VacuumStatement {
                            table: table_name,
                            retention_hours: None,
                            dry_run: true,
                        }))
                    } else {
                        Err(ParserError::ParserError(
                            "Expected keyword 'RUN'".to_string(),
                        ))
                    }
                }
                _ => Err(ParserError::ParserError(format!("Unexpected token {w}"))),
            },
            _ => {
                let token = self.parser.next_token();
                if token == Token::EOF || token == Token::SemiColon {
                    Ok(Statement::Vacuum(VacuumStatement {
                        table: table_name,
                        retention_hours: None,
                        dry_run: false,
                    }))
                } else {
                    Err(ParserError::ParserError(format!(
                        "Unexpected token {token}"
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::{Ident, ObjectNamePart};
    use datafusion::sql::sqlparser::tokenizer::Span;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = DeltaParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected, "actual:\n{:#?}", statements[0]);
        Ok(())
    }

    #[test]
    fn test_parse_vacuum() {
        let stmt = Statement::Vacuum(VacuumStatement {
            table: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "data_table".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            retention_hours: None,
            dry_run: false,
        });
        assert!(expect_parse_ok("VACUUM data_table", stmt).is_ok());

        let stmt = Statement::Vacuum(VacuumStatement {
            table: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "data_table".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            retention_hours: Some(10),
            dry_run: false,
        });
        assert!(expect_parse_ok("VACUUM data_table RETAIN 10 HOURS", stmt).is_ok());

        let stmt = Statement::Vacuum(VacuumStatement {
            table: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "data_table".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            retention_hours: Some(10),
            dry_run: true,
        });
        assert!(expect_parse_ok("VACUUM data_table RETAIN 10 HOURS DRY RUN", stmt).is_ok());

        let stmt = Statement::Vacuum(VacuumStatement {
            table: ObjectName(vec![ObjectNamePart::Identifier(Ident {
                value: "data_table".to_string(),
                quote_style: None,
                span: Span::empty(),
            })]),
            retention_hours: None,
            dry_run: true,
        });
        assert!(expect_parse_ok("VACUUM data_table DRY RUN", stmt).is_ok());

        // Error cases

        let res = DeltaParser::parse_sql("VACUUM data_table DRY").unwrap_err();
        match res {
            ParserError::ParserError(msg) => {
                assert_eq!("Expected keyword 'RUN'", msg);
            }
            _ => unreachable!(),
        }
    }
}
