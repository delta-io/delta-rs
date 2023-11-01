//! Configuration for how to write statistics on Add actions.

use std::collections::HashMap;

/// An error that occurred when trying to parse a [Value](serde_json::Value) into a [WriteStatsFilterPath]
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum WriteStatsFilterPathParseError {
    /// Only arrays or objects are supported when parsing paths.
    #[error("Expected an array or object, got {0:?}")]
    ExpectedArrayOrObject(serde_json::Value),

    /// When specifying columns via an array, all elements in the array should be a string.
    #[error("List elements should be a string, got {0:?}")]
    ArrayElementShouldBeString(serde_json::Value),

    /// Additional context on where a given error occurred within the path structure.
    #[error("when parsing key {key:?}: {err:?}")]
    Context {
        /// The key being parsed when the error occurred.
        key: String,
        /// The error.
        err: Box<Self>,
    },
}

/// A path structure representing the columns available in a schema.
#[derive(Debug, PartialEq, Clone)]
pub enum WriteStatsFilterPath {
    /// A leaf in the path structure.
    Leaf,
    /// An object representing multiple sub-fields.
    Object(HashMap<String, WriteStatsFilterPath>),
}

impl WriteStatsFilterPath {
    #[allow(dead_code)]
    fn includes(&self, path: &[String]) -> bool {
        if path.is_empty() {
            // If we have run out of path segments, we can assume everything under here is represented in the structure.
            return true;
        }
        match self {
            WriteStatsFilterPath::Leaf => true, // leaves represent termination in the tree, so everything under this leaf is represented.
            WriteStatsFilterPath::Object(o) => {
                let front = &path[0];
                match o.get(front) {
                    Some(p) => p.includes(&path[1..]), // the front element in the path is here; recursively check for the next element.
                    None => false,
                }
            }
        }
    }
}

impl TryFrom<serde_json::Value> for WriteStatsFilterPath {
    type Error = WriteStatsFilterPathParseError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Array(arr) => Ok(Self::Object(
                arr.into_iter()
                    .map(|v| match v {
                        serde_json::Value::String(s) => Ok(s),
                        got => Err(WriteStatsFilterPathParseError::ArrayElementShouldBeString(
                            got,
                        )),
                    })
                    .collect::<Result<Vec<_>, Self::Error>>()?
                    .into_iter()
                    .map(|s| (s, Self::Leaf))
                    .collect(),
            )),
            serde_json::Value::Object(o) => {
                if o.len() == 0 {
                    Ok(Self::Leaf)
                } else {
                    Ok(Self::Object(
                        o.into_iter()
                            .map(|(k, v)| {
                                WriteStatsFilterPath::try_from(v)
                                    .map(|p| (k.to_owned(), p))
                                    .map_err(|e| WriteStatsFilterPathParseError::Context {
                                        key: k.to_owned(),
                                        err: Box::new(e),
                                    })
                            })
                            .collect::<Result<Vec<_>, Self::Error>>()?
                            .into_iter()
                            .collect(),
                    ))
                }
            }
            got => return Err(WriteStatsFilterPathParseError::ExpectedArrayOrObject(got)),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
/// Configuration specifying which columns to write statistics for in add [Action](crate::protocol::Action)s.
pub enum WriteStatsFilterConfig {
    /// Default configuration. Write statistics for all columns.
    Default,
    /// Include columns under this path structure. All others are excluded.
    Include(WriteStatsFilterPath),
    /// Exclude columns under this path structure. All others are included.
    Exclude(WriteStatsFilterPath),
}

impl Default for WriteStatsFilterConfig {
    fn default() -> Self {
        Self::Default
    }
}

/// An error occurred when parsing a [WriteStatsFilterConfig] object from JSON.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum WriteStatsFilterConfigParseError {
    /// Erorr when translating the JSON structure into the field paths.
    #[error("path parse error: {0:?}")]
    PathError(#[from] WriteStatsFilterPathParseError),
    /// Expected to find an object, instead found this JSON value
    #[error("expected an object, got {0:?}")]
    ExpectedObject(serde_json::Value),
    /// Expected to find a single key `include` or `exclude`, instead these keys were found.
    #[error("expected either 'include' or 'exclude' as a single key, got keys {0:?}")]
    ExpectedEitherIncludeOrExclude(Vec<String>),
}

impl TryFrom<serde_json::Value> for WriteStatsFilterConfig {
    type Error = WriteStatsFilterConfigParseError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Object(mut o) => {
                let keys = o.keys().collect::<Vec<&String>>();
                match keys {
                    k if k == vec!["include"] => {
                        let v = o.remove("include").unwrap();
                        let path = WriteStatsFilterPath::try_from(v)?;
                        Ok(Self::Include(path))
                    }
                    k if k == vec!["exclude"] => {
                        let v = o.remove("exclude").unwrap();
                        let path = WriteStatsFilterPath::try_from(v)?;
                        Ok(Self::Exclude(path))
                    }
                    got_keys => Err(
                        WriteStatsFilterConfigParseError::ExpectedEitherIncludeOrExclude(
                            got_keys.into_iter().map(|s| s.to_owned()).collect(),
                        ),
                    ),
                }
            }
            got => Err(WriteStatsFilterConfigParseError::ExpectedObject(got)),
        }
    }
}

impl WriteStatsFilterConfig {
    /// Create a new instance of [WriteStatsFilterConfig], including these columns.
    pub fn include_columns<S: AsRef<str>, L: IntoIterator<Item = S>>(columns: L) -> Self {
        Self::Include(WriteStatsFilterPath::Object(
            columns
                .into_iter()
                .map(|s| (s.as_ref().to_owned(), WriteStatsFilterPath::Leaf))
                .collect(),
        ))
    }

    /// Create a new instance of [WriteStatsFilterConfig], excluding these columns.
    pub fn exclude_columns<S: AsRef<str>, L: IntoIterator<Item = S>>(columns: L) -> Self {
        Self::Exclude(WriteStatsFilterPath::Object(
            columns
                .into_iter()
                .map(|s| (s.as_ref().to_owned(), WriteStatsFilterPath::Leaf))
                .collect(),
        ))
    }

    pub(crate) fn should_write(&self, field_path: &[String]) -> bool {
        match self {
            WriteStatsFilterConfig::Default => true,
            WriteStatsFilterConfig::Include(path) => path.includes(field_path),
            WriteStatsFilterConfig::Exclude(path) => !path.includes(field_path),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::{
        WriteStatsFilterConfig, WriteStatsFilterConfigParseError, WriteStatsFilterPath,
        WriteStatsFilterPathParseError,
    };

    #[test]
    fn test_parse_write_stats_config_include() {
        let config = serde_json::json! ({
            "include" : {
                "foo" : [ "col_a", "col_b" ],
                "bar" : {}
            }
        });

        let parsed = WriteStatsFilterConfig::try_from(config);

        let expected = WriteStatsFilterConfig::Include(WriteStatsFilterPath::Object(
            vec![
                (
                    "foo".to_owned(),
                    WriteStatsFilterPath::Object(
                        vec![
                            ("col_a".to_owned(), WriteStatsFilterPath::Leaf),
                            ("col_b".to_owned(), WriteStatsFilterPath::Leaf),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                ),
                ("bar".to_owned(), WriteStatsFilterPath::Leaf),
            ]
            .into_iter()
            .collect(),
        ));

        assert_eq!(parsed, Ok(expected));
    }

    #[test]
    fn test_parse_write_stats_config_invalid_type() {
        let config = serde_json::json! ({
            "include" : {
                "foo" : [ "col_a", "col_b" ],
                "bar" : true
            }
        });

        let parsed = WriteStatsFilterConfig::try_from(config);

        assert_eq!(
            parsed,
            Err(WriteStatsFilterConfigParseError::PathError(
                WriteStatsFilterPathParseError::Context {
                    key: "bar".to_owned(),
                    err: Box::new(WriteStatsFilterPathParseError::ExpectedArrayOrObject(
                        true.into()
                    ))
                }
            ))
        );
    }

    #[test]
    fn test_parse_write_stats_config_invalid_array_type() {
        let config = serde_json::json! ({
            "include" : {
                "foo" : [ "col_a", 3 ],
            }
        });

        let parsed = WriteStatsFilterConfig::try_from(config);

        assert_eq!(
            parsed,
            Err(WriteStatsFilterConfigParseError::PathError(
                WriteStatsFilterPathParseError::Context {
                    key: "foo".to_owned(),
                    err: Box::new(WriteStatsFilterPathParseError::ArrayElementShouldBeString(
                        3.into()
                    ))
                }
            ))
        );
    }

    #[test]
    fn test_parse_write_stats_config_invalid_not_include_exclude() {
        let config = serde_json::json! ({
            "foo" : {
                "bar" : [ "col_a", "col_a" ],
            }
        });

        let parsed = WriteStatsFilterConfig::try_from(config);

        assert_eq!(
            parsed,
            Err(
                WriteStatsFilterConfigParseError::ExpectedEitherIncludeOrExclude(vec![
                    "foo".to_owned()
                ])
            )
        );
    }

    #[test]
    fn test_parse_write_stats_config_invalid_not_object() {
        let config = serde_json::json!(true);

        let parsed = WriteStatsFilterConfig::try_from(config);

        assert_eq!(
            parsed,
            Err(WriteStatsFilterConfigParseError::ExpectedObject(
                true.into()
            ))
        );
    }
}
