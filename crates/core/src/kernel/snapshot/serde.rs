use std::fmt;

use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use chrono::{DateTime, TimeZone, Utc};
use object_store::ObjectMeta;
use serde::de::{self, Deserializer, SeqAccess, Visitor};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

use super::log_segment::LogSegment;
use super::EagerSnapshot;

#[derive(Serialize, Deserialize, Debug)]
struct FileInfo {
    path: String,
    size: usize,
    last_modified: i64,
    e_tag: Option<String>,
    version: Option<String>,
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
                last_modified: f.last_modified.timestamp_nanos_opt().unwrap(),
                e_tag: f.e_tag.clone(),
                version: f.version.clone(),
            })
            .collect::<Vec<_>>();
        let checkpoint_files = self
            .checkpoint_files
            .iter()
            .map(|f| FileInfo {
                path: f.location.to_string(),
                size: f.size,
                last_modified: f.last_modified.timestamp_nanos_opt().unwrap(),
                e_tag: f.e_tag.clone(),
                version: f.version.clone(),
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
                .map(|f| {
                    let seconds = f.last_modified / 1_000_000_000;
                    let nano_seconds = (f.last_modified % 1_000_000_000) as u32;
                    ObjectMeta {
                        location: f.path.into(),
                        size: f.size,
                        last_modified: Utc.timestamp_opt(seconds, nano_seconds).single().unwrap(),
                        version: f.version,
                        e_tag: f.e_tag,
                    }
                })
                .collect(),
            checkpoint_files: checkpoint_files
                .into_iter()
                .map(|f| ObjectMeta {
                    location: f.path.into(),
                    size: f.size,
                    last_modified: DateTime::from_timestamp_millis(f.last_modified).unwrap(),

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

impl Serialize for EagerSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.snapshot)?;
        seq.serialize_element(&self.tracked_actions)?;
        seq.serialize_element(&self.transactions)?;
        for batch in self.files.iter() {
            let mut buffer = vec![];
            let mut writer = FileWriter::try_new(&mut buffer, batch.schema().as_ref())
                .map_err(serde::ser::Error::custom)?;
            writer.write(batch).map_err(serde::ser::Error::custom)?;
            writer.finish().map_err(serde::ser::Error::custom)?;
            let data = writer.into_inner().map_err(serde::ser::Error::custom)?;
            seq.serialize_element(&data)?;
        }
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
        let tracked_actions = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let transactions = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
        let mut files = Vec::new();
        while let Some(elem) = seq.next_element::<Vec<u8>>()? {
            let mut reader =
                FileReader::try_new(std::io::Cursor::new(elem), None).map_err(|e| {
                    de::Error::custom(format!("failed to read ipc record batch: {}", e))
                })?;
            let rb = reader
                .next()
                .ok_or(de::Error::custom("missing ipc data"))?
                .map_err(|e| {
                    de::Error::custom(format!("failed to read ipc record batch: {}", e))
                })?;
            files.push(rb);
        }
        Ok(EagerSnapshot {
            snapshot,
            files,
            tracked_actions,
            transactions,
        })
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
