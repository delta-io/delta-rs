use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use chrono::{DateTime, Utc};
use object_store::ObjectMeta;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use std::fmt;

use super::log_segment::LogSegment;
use super::EagerSnapshot;

#[derive(Serialize, Deserialize, Debug)]
struct FileInfo {
    path: String,
    size: usize,
    last_modified: DateTime<Utc>,
}

impl Serialize for LogSegment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let commit_files = self
            .commit_files
            .iter()
            .map(|f| FileInfo {
                path: f.location.to_string(),
                size: f.size,
                last_modified: f.last_modified,
            })
            .collect::<Vec<_>>();
        let checkpoint_files = self
            .checkpoint_files
            .iter()
            .map(|f| FileInfo {
                path: f.location.to_string(),
                size: f.size,
                last_modified: f.last_modified,
            })
            .collect::<Vec<_>>();

        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.version)?;
        seq.serialize_element(&commit_files)?;
        seq.serialize_element(&checkpoint_files)?;
        seq.end()
    }
}

// Deserialize the log segment
struct LogSegmentVisitor;

impl<'de> Visitor<'de> for LogSegmentVisitor {
    type Value = LogSegment;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct LogSegment")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<LogSegment, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let version = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let commit_files: Vec<FileInfo> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let checkpoint_files: Vec<FileInfo> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;

        Ok(LogSegment {
            version,
            commit_files: commit_files
                .into_iter()
                .map(|f| ObjectMeta {
                    location: f.path.into(),
                    size: f.size,
                    last_modified: f.last_modified,
                    version: None,
                    e_tag: None,
                })
                .collect(),
            checkpoint_files: checkpoint_files
                .into_iter()
                .map(|f| ObjectMeta {
                    location: f.path.into(),
                    size: f.size,
                    last_modified: f.last_modified,
                    version: None,
                    e_tag: None,
                })
                .collect(),
        })
    }
}

impl<'de> Deserialize<'de> for LogSegment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(LogSegmentVisitor)
    }
}

struct RecordBatchData(RecordBatch);

impl Serialize for RecordBatchData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buffer = vec![];
        let mut writer = FileWriter::try_new(&mut buffer, self.0.schema().as_ref())
            .map_err(|e| serde::ser::Error::custom(e))?;
        writer
            .write(&self.0)
            .map_err(|e| serde::ser::Error::custom(e))?;
        writer.finish().map_err(|e| serde::ser::Error::custom(e))?;
        let data = writer
            .into_inner()
            .map_err(|e| serde::ser::Error::custom(e))?;
        serializer.serialize_bytes(&data)
    }
}

struct RecordBatchesVisitor;

impl<'de> Visitor<'de> for RecordBatchesVisitor {
    type Value = RecordBatchData;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("struct RecordBatchData")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut reader = FileReader::try_new(std::io::Cursor::new(v), None)
            .map_err(|e| de::Error::custom(format!("failed to read ipc record batch: {}", e)))?;
        let rb = reader
            .next()
            .ok_or(de::Error::custom("missing ipc data"))?
            .map_err(|e| de::Error::custom(format!("failed to read ipc record batch: {}", e)))?;
        Ok(RecordBatchData(rb))
    }
}

impl<'de> Deserialize<'de> for RecordBatchData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(RecordBatchesVisitor)
    }
}

impl Serialize for EagerSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data = self
            .files
            .iter()
            .map(|rb| RecordBatchData(rb.clone()))
            .collect::<Vec<_>>();
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.snapshot)?;
        seq.serialize_element(&data)?;
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
        let snapshot = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let data: Vec<RecordBatchData> = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let files = data.into_iter().map(|rb| rb.0).collect::<Vec<_>>();
        Ok(EagerSnapshot { snapshot, files })
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
