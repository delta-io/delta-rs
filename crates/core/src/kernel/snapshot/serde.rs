use std::fmt;
use std::sync::Arc;

use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use delta_kernel::FileMeta;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::log_segment::{LogSegment, LogSegmentFiles};
use delta_kernel::path::ParsedLogPath;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_configuration::TableConfiguration;
use itertools::Itertools;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize, ser::SerializeSeq};
use url::Url;

use crate::DeltaTableConfig;

use super::{
    EagerSnapshot, MaterializedFiles, MaterializedFilesPolicy, MaterializedFilesScope, Snapshot,
    SnapshotIdentity, SnapshotMaterializationMode,
};

#[derive(Serialize, Deserialize)]
enum MaterializedFilesScopeWire {
    FullTable,
    #[serde(other)]
    Unsupported,
}

impl From<MaterializedFilesScope> for MaterializedFilesScopeWire {
    fn from(value: MaterializedFilesScope) -> Self {
        match value {
            MaterializedFilesScope::FullTable => Self::FullTable,
        }
    }
}

impl MaterializedFilesScopeWire {
    fn into_materialized(self) -> Option<MaterializedFilesScope> {
        match self {
            Self::FullTable => Some(MaterializedFilesScope::FullTable),
            Self::Unsupported => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum MaterializedFilesPolicyWire {
    FullTablePreserveRaw,
    FullTableWithoutStats,
    #[serde(other)]
    Unsupported,
}

impl From<MaterializedFilesPolicy> for MaterializedFilesPolicyWire {
    fn from(value: MaterializedFilesPolicy) -> Self {
        match value {
            MaterializedFilesPolicy::FullTablePreserveRaw => Self::FullTablePreserveRaw,
            MaterializedFilesPolicy::FullTableWithoutStats => Self::FullTableWithoutStats,
        }
    }
}

impl MaterializedFilesPolicyWire {
    fn into_materialized(self) -> Option<MaterializedFilesPolicy> {
        match self {
            Self::FullTablePreserveRaw => Some(MaterializedFilesPolicy::FullTablePreserveRaw),
            Self::FullTableWithoutStats => Some(MaterializedFilesPolicy::FullTableWithoutStats),
            Self::Unsupported => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct MaterializedFilesWire {
    version: delta_kernel::Version,
    scope: MaterializedFilesScopeWire,
    batches: Vec<u8>,
    #[serde(default)]
    identity: Option<SnapshotIdentity>,
    #[serde(default)]
    policy: Option<MaterializedFilesPolicyWire>,
}

impl MaterializedFilesWire {
    fn try_from_materialized(value: &MaterializedFiles) -> Result<Self, String> {
        if value.existing_predicate.is_some() {
            return Err(
                "serializing predicate-specific materialized state is not supported".into(),
            );
        }

        Ok(Self {
            version: value.identity.version,
            scope: value.scope.into(),
            batches: serialize_batches(value.batches.as_ref())?,
            identity: Some(value.identity.clone()),
            policy: Some(value.policy.into()),
        })
    }

    fn into_materialized(
        self,
        owning_snapshot: &Snapshot,
    ) -> Result<Option<MaterializedFiles>, String> {
        let Some(scope) = self.scope.into_materialized() else {
            tracing::trace!(
                snapshot_version = owning_snapshot.version(),
                "dropping materialized snapshot cache with unsupported scope"
            );
            return Ok(None);
        };
        let expected_policy = owning_snapshot.materialized_files_policy();
        let policy = match self.policy {
            Some(policy) => {
                let Some(policy) = policy.into_materialized() else {
                    tracing::trace!(
                        snapshot_version = owning_snapshot.version(),
                        "dropping materialized snapshot cache with unsupported policy"
                    );
                    return Ok(None);
                };
                policy
            }
            None => expected_policy,
        };
        let identity = self.identity.unwrap_or_else(|| owning_snapshot.identity());

        if self.version != identity.version
            || !identity.is_for(owning_snapshot)
            || policy != expected_policy
        {
            tracing::trace!(
                wire_version = self.version,
                identity_version = identity.version,
                snapshot_version = owning_snapshot.version(),
                wire_policy = ?policy,
                expected_policy = ?expected_policy,
                "dropping incompatible materialized snapshot cache from serde payload"
            );
            return Ok(None);
        }

        let materialized_files = MaterializedFiles {
            identity,
            policy,
            scope,
            existing_predicate: None,
            batches: deserialize_batches(self.batches)?.into(),
        };

        Ok(Some(materialized_files))
    }
}

fn materialized_files_from_legacy_eager_payload(
    snapshot: &Snapshot,
    legacy_payload: Option<Vec<u8>>,
) -> Result<Option<Arc<MaterializedFiles>>, String> {
    if let Some(materialized_files) = snapshot.materialized_files().cloned() {
        return Ok(Some(materialized_files));
    }

    let Some(legacy_payload) = legacy_payload else {
        return Ok(None);
    };

    if legacy_payload.is_empty()
        && snapshot.materialization_mode() == SnapshotMaterializationMode::Lazy
    {
        return Ok(None);
    }

    Ok(Some(Arc::new(MaterializedFiles::full(
        snapshot,
        deserialize_batches(legacy_payload)?,
    ))))
}

fn serialize_batches(batches: &[arrow_array::RecordBatch]) -> Result<Vec<u8>, String> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let mut buffer = vec![];
    let mut writer = FileWriter::try_new(&mut buffer, batches[0].schema().as_ref())
        .map_err(|e| format!("failed to create ipc writer: {e}"))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("failed to write ipc batch: {e}"))?;
    }
    writer
        .finish()
        .map_err(|e| format!("failed to finish ipc writer: {e}"))?;
    drop(writer);
    Ok(buffer)
}

fn deserialize_batches(data: Vec<u8>) -> Result<Vec<arrow_array::RecordBatch>, String> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    FileReader::try_new(std::io::Cursor::new(data), None)
        .map_err(|e| format!("failed to read ipc record batch: {e}"))?
        .try_collect()
        .map_err(|e| format!("failed to read ipc record batch: {e}"))
}

impl Serialize for Snapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ascending_commit_files = self
            .inner
            .log_segment()
            .listed
            .ascending_commit_files
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let ascending_compaction_files = self
            .inner
            .log_segment()
            .listed
            .ascending_compaction_files
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let checkpoint_parts = self
            .inner
            .log_segment()
            .listed
            .checkpoint_parts
            .iter()
            .map(|f| FileMetaSerde::from(&f.location))
            .collect_vec();
        let latest_crc_file = self
            .inner
            .log_segment()
            .listed
            .latest_crc_file
            .as_ref()
            .map(|f| FileMetaSerde::from(&f.location));
        let latest_commit_file = self
            .inner
            .log_segment()
            .listed
            .latest_commit_file
            .as_ref()
            .map(|f| FileMetaSerde::from(&f.location));

        let mut seq = serializer.serialize_seq(None)?;

        seq.serialize_element(&self.version())?;
        seq.serialize_element(&self.inner.table_root())?;
        seq.serialize_element(&self.protocol())?;
        seq.serialize_element(&self.metadata())?;
        seq.serialize_element(&ascending_commit_files)?;
        seq.serialize_element(&ascending_compaction_files)?;
        seq.serialize_element(&checkpoint_parts)?;
        seq.serialize_element(&latest_crc_file)?;
        seq.serialize_element(&latest_commit_file)?;

        seq.serialize_element(&self.config)?;
        let materialized_files = self
            .materialized_files()
            .map(|value| MaterializedFilesWire::try_from_materialized(value))
            .transpose()
            .map_err(serde::ser::Error::custom)?;
        seq.serialize_element(&materialized_files)?;

        seq.end()
    }
}

struct SnapshotVisitor;

#[derive(Serialize, Deserialize)]
struct FileMetaSerde {
    /// The fully qualified path to the object
    pub location: Url,
    /// The last modified time as milliseconds since unix epoch
    pub last_modified: i64,
    /// The size in bytes of the object
    pub size: u64,
}

impl FileMetaSerde {
    fn into_kernel(self) -> FileMeta {
        FileMeta {
            location: self.location,
            last_modified: self.last_modified,
            size: self.size,
        }
    }
}

impl From<&FileMeta> for FileMetaSerde {
    fn from(file_meta: &FileMeta) -> Self {
        FileMetaSerde {
            location: file_meta.location.clone(),
            last_modified: file_meta.last_modified,
            size: file_meta.size,
        }
    }
}

impl<'de> Visitor<'de> for SnapshotVisitor {
    type Value = Snapshot;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct Snapshot")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<Snapshot, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let version: i64 = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let table_url: Url = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let protocol: Protocol = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
        let metadata: Metadata = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(3, &self))?;
        let ascending_commit_files: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(4, &self))?;
        let ascending_compaction_files: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(5, &self))?;
        let checkpoint_parts: Vec<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(6, &self))?;
        let latest_crc_file: Option<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(7, &self))?;
        let latest_commit_file: Option<FileMetaSerde> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(8, &self))?;
        let config: DeltaTableConfig = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(9, &self))?;
        let materialized_files: Option<MaterializedFilesWire> = seq.next_element()?.unwrap_or(None);

        let ascending_commit_files = ascending_commit_files
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let ascending_compaction_files = ascending_compaction_files
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let checkpoint_parts = checkpoint_parts
            .into_iter()
            .filter_map(|meta| {
                ParsedLogPath::try_from(meta.into_kernel())
                    .map_err(de::Error::custom)
                    .transpose()
            })
            .collect::<Result<Vec<ParsedLogPath>, _>>()?;

        let latest_crc_file = latest_crc_file
            .map(|meta| ParsedLogPath::try_from(meta.into_kernel()).map_err(de::Error::custom))
            .transpose()?
            .flatten();

        let latest_commit_file = latest_commit_file
            .map(|meta| ParsedLogPath::try_from(meta.into_kernel()).map_err(de::Error::custom))
            .transpose()?
            .flatten();

        let listed_log_files = LogSegmentFiles {
            ascending_commit_files,
            ascending_compaction_files,
            checkpoint_parts,
            latest_crc_file,
            latest_commit_file,
            ..Default::default()
        };

        let log_root = if !table_url.path().ends_with("/") {
            let mut aux = table_url.clone();
            aux.set_path(&format!("{}/", table_url.path()));
            aux.join("_delta_log/").unwrap()
        } else {
            table_url.join("_delta_log/").unwrap()
        };

        let log_segment =
            LogSegment::try_new(listed_log_files, log_root, Some(version as u64), None)
                .map_err(de::Error::custom)?;

        let table_configuration =
            TableConfiguration::try_new(metadata, protocol, table_url.clone(), version as u64)
                .map_err(de::Error::custom)?;

        let snapshot = KernelSnapshot::new(log_segment, table_configuration);

        let snapshot = Snapshot {
            inner: Arc::new(snapshot),
            config,
            materialized_files: None,
        };
        let materialized_files = materialized_files
            .map(|value| {
                value
                    .into_materialized(&snapshot)
                    .map_err(de::Error::custom)
            })
            .transpose()?
            .flatten()
            .map(Arc::new);

        Ok(snapshot.with_materialized_files(materialized_files))
    }
}

impl<'de> Deserialize<'de> for Snapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(SnapshotVisitor)
    }
}

impl Serialize for EagerSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.snapshot)?;
        seq.end()
    }
}

// Deserialize the eager snapshot
struct EagerSnapshotVisitor;

impl<'de> Visitor<'de> for EagerSnapshotVisitor {
    type Value = EagerSnapshot;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct EagerSnapshot")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<EagerSnapshot, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let snapshot: Arc<Snapshot> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let legacy_payload: Option<Vec<u8>> = seq.next_element()?;
        let snapshot =
            match materialized_files_from_legacy_eager_payload(snapshot.as_ref(), legacy_payload)
                .map_err(de::Error::custom)?
            {
                Some(materialized_files) if snapshot.materialized_files().is_none() => {
                    Arc::new(snapshot.with_materialized_files(Some(materialized_files)))
                }
                _ => snapshot,
            };
        if snapshot.materialization_mode() == SnapshotMaterializationMode::Eager
            && snapshot.materialized_files().is_none()
        {
            return Err(de::Error::custom(
                "cannot deserialize eager snapshot without valid materialized files",
            ));
        }
        Ok(EagerSnapshot { snapshot })
    }
}

impl<'de> Deserialize<'de> for EagerSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<EagerSnapshot, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(EagerSnapshotVisitor)
    }
}
