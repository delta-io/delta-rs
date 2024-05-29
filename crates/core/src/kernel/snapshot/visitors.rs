//! Log replay visitors.
//!
//! Log replay visitors allow to extract additional actions during log replay.

use std::collections::HashMap;

use arrow::compute::{filter_record_batch, is_not_null};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray, StructArray};

use super::ActionType;
use crate::errors::DeltaResult;
use crate::kernel::arrow::extract as ex;
use crate::kernel::Transaction;

/// Allows hooking into the reading of commit files and checkpoints whenever a table is loaded or updated.
pub trait ReplayVisitor: std::fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;

    /// Process a batch
    fn visit_batch(&mut self, batch: &RecordBatch) -> DeltaResult<()>;

    /// return all relevant actions for the visitor
    fn required_actions(&self) -> Vec<ActionType>;
}

/// Get the relevant visitor for the given action type
pub fn get_visitor(action: &ActionType) -> Option<Box<dyn ReplayVisitor>> {
    match action {
        ActionType::Txn => Some(Box::new(AppTransactionVisitor::new())),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub(crate) struct AppTransactionVisitor {
    pub(crate) app_transaction_version: HashMap<String, Transaction>,
}

impl AppTransactionVisitor {
    pub(crate) fn new() -> Self {
        Self {
            app_transaction_version: HashMap::new(),
        }
    }
}

impl AppTransactionVisitor {
    pub fn merge(&self, map: &HashMap<String, Transaction>) -> HashMap<String, Transaction> {
        let mut clone = map.clone();
        for (key, value) in &self.app_transaction_version {
            clone.insert(key.clone(), value.clone());
        }
        clone
    }
}

impl ReplayVisitor for AppTransactionVisitor {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn visit_batch(&mut self, batch: &arrow_array::RecordBatch) -> DeltaResult<()> {
        if batch.column_by_name("txn").is_none() {
            return Ok(());
        }

        let txn_col = ex::extract_and_cast::<StructArray>(batch, "txn")?;
        let filtered = filter_record_batch(batch, &is_not_null(txn_col)?)?;
        let arr = ex::extract_and_cast::<StructArray>(&filtered, "txn")?;

        let id = ex::extract_and_cast::<StringArray>(arr, "appId")?;
        let version = ex::extract_and_cast::<Int64Array>(arr, "version")?;
        let last_updated = ex::extract_and_cast_opt::<Int64Array>(arr, "lastUpdated");

        for idx in 0..id.len() {
            if id.is_valid(idx) {
                let app_id = ex::read_str(id, idx)?;
                if self.app_transaction_version.contains_key(app_id) {
                    continue;
                }
                self.app_transaction_version.insert(
                    app_id.to_owned(),
                    Transaction {
                        app_id: app_id.into(),
                        version: ex::read_primitive(version, idx)?,
                        last_updated: last_updated.and_then(|arr| ex::read_primitive_opt(arr, idx)),
                    },
                );
            }
        }

        Ok(())
    }

    fn required_actions(&self) -> Vec<ActionType> {
        vec![ActionType::Txn]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn test_app_txn_visitor() {
        let fields: Fields = vec![
            Field::new("appId", DataType::Utf8, true),
            Field::new("version", DataType::Int64, true),
            Field::new("lastUpdated", DataType::Int64, true),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "txn",
            DataType::Struct(fields.clone()),
            false,
        )]));

        let mut data_app = vec![None, Some("my-app"), None];
        let mut data_version = vec![None, Some(1), None];
        let mut data_last_updated = vec![None, Some(123), None];
        let arr = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(data_app.clone())),
                Arc::new(Int64Array::from(data_version.clone())),
                Arc::new(Int64Array::from(data_last_updated.clone())),
            ],
            None,
        ));

        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let mut visitor = AppTransactionVisitor::new();
        visitor.visit_batch(&batch).unwrap();

        let app_txns = visitor.app_transaction_version;
        assert_eq!(app_txns.len(), 1);
        assert_eq!(app_txns.get("my-app").map(|t| t.version), Some(1));
        assert_eq!(
            app_txns.get("my-app").map(|t| t.last_updated),
            Some(Some(123))
        );

        // test that only the first encountered txn ist tacked for every app id.
        data_app.extend([None, Some("my-app")]);
        data_version.extend([None, Some(2)]);
        data_last_updated.extend([None, Some(124)]);
        let arr = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(data_app.clone())),
                Arc::new(Int64Array::from(data_version.clone())),
                Arc::new(Int64Array::from(data_last_updated.clone())),
            ],
            None,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let mut visitor = AppTransactionVisitor::new();
        visitor.visit_batch(&batch).unwrap();

        let app_txns = visitor.app_transaction_version;
        assert_eq!(app_txns.len(), 1);
        assert_eq!(app_txns.get("my-app").map(|t| t.version), Some(1));
        assert_eq!(
            app_txns.get("my-app").map(|t| t.last_updated),
            Some(Some(123))
        );

        // test that multiple app ids are tracked
        data_app.extend([Some("my-other-app")]);
        data_version.extend([Some(10)]);
        data_last_updated.extend([Some(123)]);
        let arr = Arc::new(StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(data_app.clone())),
                Arc::new(Int64Array::from(data_version.clone())),
                Arc::new(Int64Array::from(data_last_updated.clone())),
            ],
            None,
        ));
        let batch = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
        let mut visitor = AppTransactionVisitor::new();
        visitor.visit_batch(&batch).unwrap();

        let app_txns = visitor.app_transaction_version;
        assert_eq!(app_txns.len(), 2);
        assert_eq!(app_txns.get("my-other-app").map(|t| t.version), Some(10));
    }
}
