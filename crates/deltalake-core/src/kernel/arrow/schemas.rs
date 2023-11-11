//! Arrow schemas for the delta log

use arrow_schema::{Field, Fields, Schema};
use lazy_static::lazy_static;

use super::super::ActionType;

lazy_static! {
    static ref ARROW_METADATA_FIELD: Field =
        ActionType::Metadata.schema_field().try_into().unwrap();
    static ref ARROW_PROTOCOL_FIELD: Field =
        ActionType::Protocol.schema_field().try_into().unwrap();
    static ref ARROW_COMMIT_INFO_FIELD: Field =
        ActionType::CommitInfo.schema_field().try_into().unwrap();
    static ref ARROW_ADD_FIELD: Field = ActionType::Add.schema_field().try_into().unwrap();
    static ref ARROW_REMOVE_FIELD: Field = ActionType::Remove.schema_field().try_into().unwrap();
    static ref ARROW_CDC_FIELD: Field = ActionType::Cdc.schema_field().try_into().unwrap();
    static ref ARROW_TXN_FIELD: Field = ActionType::Txn.schema_field().try_into().unwrap();
    static ref ARROW_DOMAIN_METADATA_FIELD: Field = ActionType::DomainMetadata
        .schema_field()
        .try_into()
        .unwrap();
    static ref ARROW_CHECKPOINT_METADATA_FIELD: Field = ActionType::CheckpointMetadata
        .schema_field()
        .try_into()
        .unwrap();
    static ref ARROW_SIDECAR_FIELD: Field = ActionType::Sidecar.schema_field().try_into().unwrap();
}

impl ActionType {
    /// Returns the root field for the action type
    pub fn arrow_field(&self) -> &Field {
        match self {
            Self::Metadata => &ARROW_METADATA_FIELD,
            Self::Protocol => &ARROW_PROTOCOL_FIELD,
            Self::CommitInfo => &ARROW_COMMIT_INFO_FIELD,
            Self::Add => &ARROW_ADD_FIELD,
            Self::Remove => &ARROW_REMOVE_FIELD,
            Self::Cdc => &ARROW_CDC_FIELD,
            Self::Txn => &ARROW_TXN_FIELD,
            Self::DomainMetadata => &ARROW_DOMAIN_METADATA_FIELD,
            Self::CheckpointMetadata => &ARROW_CHECKPOINT_METADATA_FIELD,
            Self::Sidecar => &ARROW_SIDECAR_FIELD,
        }
    }
}

/// Returns the schema for the delta log
pub fn get_log_schema() -> Schema {
    Schema {
        fields: Fields::from_iter([
            ActionType::Add.arrow_field().clone(),
            ActionType::Cdc.arrow_field().clone(),
            ActionType::CommitInfo.arrow_field().clone(),
            ActionType::DomainMetadata.arrow_field().clone(),
            ActionType::Metadata.arrow_field().clone(),
            ActionType::Protocol.arrow_field().clone(),
            ActionType::Remove.arrow_field().clone(),
            ActionType::Txn.arrow_field().clone(),
        ]),
        metadata: Default::default(),
    }
}
