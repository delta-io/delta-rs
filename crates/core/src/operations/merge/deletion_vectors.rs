use std::collections::{HashMap, HashSet};

use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{PutMode, PutOptions, path::Path};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use roaring::RoaringTreemap;
use serde_json::Value;

use super::barrier::DeletedRows;
use crate::kernel::{Action, ActiveAddOptions, Add, AddStatsPolicy, DeletionVectorDescriptor};
use crate::kernel::{EagerSnapshot, StorageType};
use crate::logstore::parquet_reader::ParquetObjectReader;
use crate::logstore::{LogStoreRef, TablePathResolver};
use crate::{DeltaResult, DeltaTableError};

const FILE_NORMALIZATION_BATCH_SIZE: usize = 128;

pub(super) async fn prepare_deletion_vectors(
    snapshot: &EagerSnapshot,
    log_store: &LogStoreRef,
    deleted_rows: &DeletedRows,
) -> DeltaResult<Vec<Action>> {
    if deleted_rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut actions = Vec::new();
    let mut updated_paths = HashSet::new();
    let root = snapshot.table_configuration().table_root().clone();
    let pending: Vec<_> = deleted_rows
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();
    let file_root = root.clone();
    let (resolver, mut changes) = crate::kernel::spawn_blocking_with_span(move || {
        let resolver = TablePathResolver::new(file_root)?;
        let mut changes: HashMap<String, RoaringTreemap> = HashMap::new();
        for (path, rows) in pending {
            let id = resolver.file_id(&path)?.ok_or_else(|| {
                DeltaTableError::InvalidTableLocation(
                    "Selected DV file is outside the table root".into(),
                )
            })?;
            *changes.entry(id).or_default() |= rows;
        }
        Ok::<_, DeltaTableError>((resolver, changes))
    })
    .await
    .map_err(|error| {
        DeltaTableError::Generic(format!("DV file identity task failed: {error}"))
    })??;
    let mut files = snapshot.snapshot().active_adds(
        log_store.as_ref(),
        ActiveAddOptions {
            predicate: None,
            stats: AddStatsPolicy::RawJson,
        },
    );
    loop {
        let batch = (&mut files)
            .take(FILE_NORMALIZATION_BATCH_SIZE)
            .try_collect::<Vec<_>>()
            .await?;
        if batch.is_empty() {
            break;
        }
        let resolver = resolver.clone();
        let normalized = crate::kernel::spawn_blocking_with_span(move || {
            batch
                .into_iter()
                .map(|file| {
                    let file_id = resolver.file_id(file.path_raw())?.ok_or_else(|| {
                        DeltaTableError::InvalidTableLocation(
                            "Active DV file is outside the table root".into(),
                        )
                    })?;
                    Ok((file, file_id))
                })
                .collect::<DeltaResult<Vec<_>>>()
        })
        .await
        .map_err(|error| {
            DeltaTableError::Generic(format!("DV file normalization task failed: {error}"))
        })??;
        for (file, file_id) in normalized {
            if updated_paths.contains(file_id.as_str()) {
                return Err(DeltaTableError::Generic(format!(
                    "DV MERGE cannot distinguish multiple active logical files for {}",
                    file.path()
                )));
            }
            let Some(rows) = changes.remove(file_id.as_str()) else {
                continue;
            };
            updated_paths.insert(file_id.to_string());
            let file_url = url::Url::parse(&file_id).map_err(|error| {
                DeltaTableError::InvalidTableLocation(format!("Invalid DV data file URL: {error}"))
            })?;
            let relative_path = root
                .make_relative(&file_url)
                .filter(|path| !path.starts_with("../"))
                .ok_or_else(|| {
                    DeltaTableError::InvalidTableLocation(format!(
                        "DV data file is outside table root: {file_url}"
                    ))
                })?;
            let path = Path::from_url_path(relative_path)?;
            let remove = file.remove_action(true);
            let mut add = file.to_add();
            let (mut stats, num_records) = physical_row_count(&add, &path, log_store).await?;
            let old_dv = add.deletion_vector.clone();
            let engine = log_store.engine();
            let dv_root = root.clone();
            let data_path = add.path.clone();
            let (bytes, result) = tokio::task::spawn_blocking(move || -> DeltaResult<_> {
            let mut complete = match old_dv {
                Some(descriptor) => {
                    let expected = descriptor.cardinality;
                    let descriptor: delta_kernel::actions::deletion_vector::DeletionVectorDescriptor =
                        descriptor.try_into()?;
                    let vector = descriptor.read(engine.storage_handler(), &dv_root)?;
                    if i64::try_from(vector.len()).ok() != Some(expected) {
                        return Err(DeltaTableError::Generic(format!(
                            "DV cardinality does not match its descriptor for {data_path}"
                        )));
                    }
                    vector
                }
                None => RoaringTreemap::new(),
            };
            complete |= rows;
            if complete
                .max()
                .is_some_and(|index| index > i64::MAX as u64 || index >= num_records)
            {
                return Err(DeltaTableError::Generic(format!(
                    "DV row index exceeds the physical row count {num_records} for {data_path}"
                )));
            }
            let mut vector = KernelDeletionVector::new();
            vector.add_deleted_row_indexes(complete.iter());
            let mut bytes = Vec::new();
            let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
            let result = writer.write_deletion_vector(vector)?;
            writer.finalize()?;
            Ok((bytes, result))
        })
        .await
        .map_err(|error| {
            DeltaTableError::Generic(format!("DV preparation task failed: {error}"))
        })??;
            let dv_path = Path::from(format!("deletion_vector_{}.bin", uuid::Uuid::new_v4()));
            let dv_url = root.join(dv_path.as_ref()).map_err(|error| {
                DeltaTableError::Generic(format!("Invalid DV output path: {error}"))
            })?;
            log_store
                .object_store()
                .put_opts(
                    &dv_path,
                    bytes.into(),
                    PutOptions {
                        mode: PutMode::Create,
                        ..Default::default()
                    },
                )
                .await?;
            add.deletion_vector = Some(DeletionVectorDescriptor {
                storage_type: StorageType::AbsolutePath,
                path_or_inline_dv: dv_url.to_string(),
                offset: Some(result.offset),
                size_in_bytes: result.size_in_bytes,
                cardinality: result.cardinality,
            });
            stats.insert("tightBounds".into(), Value::Bool(false));
            add.stats = Some(serde_json::to_string(&stats)?);
            actions.push(Action::Remove(remove));
            actions.push(Action::Add(add));
        }
    }
    if !changes.is_empty() {
        return Err(DeltaTableError::Generic(
            "DV MERGE selected a file that is not active in its snapshot".into(),
        ));
    }
    Ok(actions)
}

async fn physical_row_count(
    add: &Add,
    path: &Path,
    log_store: &LogStoreRef,
) -> DeltaResult<(serde_json::Map<String, Value>, u64)> {
    let mut stats = match add.stats.as_deref() {
        Some(json) => serde_json::from_str::<Value>(json)?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                DeltaTableError::Generic(format!("File statistics are not an object: {}", add.path))
            })?,
        None => serde_json::Map::new(),
    };
    let num_records = match stats.get("numRecords").filter(|count| !count.is_null()) {
        Some(count) => count.as_u64().ok_or_else(|| {
            DeltaTableError::Generic(format!("Invalid numRecords statistic for {}", add.path))
        })?,
        None => {
            let reader = ParquetObjectReader::new(log_store.object_store(), path.clone());
            let metadata = ParquetRecordBatchStreamBuilder::new(reader).await?;
            let count = u64::try_from(metadata.metadata().file_metadata().num_rows())
                .map_err(|error| DeltaTableError::Generic(format!("Invalid row count: {error}")))?;
            stats.insert("numRecords".into(), Value::from(count));
            count
        }
    };
    Ok((stats, num_records))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;

    use super::*;
    use crate::DeltaTable;
    use crate::delta_datafusion::normalize_path_as_file_id;
    use crate::kernel::{DataType, PrimitiveType};

    #[tokio::test]
    async fn normalization_batches_preserve_selected_file_updates() {
        let adds: Vec<_> = (0..=FILE_NORMALIZATION_BATCH_SIZE)
            .map(|index| {
                let mut add =
                    crate::test_utils::make_test_add(format!("file-{index}.parquet"), &[], 0);
                add.stats = Some(r#"{"numRecords":3}"#.into());
                Action::Add(add)
            })
            .collect();
        let table = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_configuration_property(crate::TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions(adds)
            .await
            .unwrap();
        let snapshot = table.snapshot().unwrap().snapshot();
        let rows = DeletedRows::default();
        for index in 0..=FILE_NORMALIZATION_BATCH_SIZE {
            let id = normalize_path_as_file_id(
                &format!("file-{index}.parquet"),
                snapshot.table_configuration().table_root(),
                "test",
            )
            .unwrap();
            rows.insert(id, RoaringTreemap::from_iter([0]));
        }
        let actions = prepare_deletion_vectors(snapshot, &table.log_store(), &rows)
            .await
            .unwrap();
        assert_eq!(actions.len(), 2 * (FILE_NORMALIZATION_BATCH_SIZE + 1));
        assert_eq!(actions.iter().filter(|action| {
            matches!(action, Action::Add(add) if add.deletion_vector.as_ref().is_some_and(|dv| dv.cardinality == 1))
        }).count(), FILE_NORMALIZATION_BATCH_SIZE + 1);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn missing_row_count_is_read_from_parquet_metadata(
        #[values(None, Some("{}"), Some(r#"{"numRecords":null}"#))] statistics: Option<&str>,
    ) {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_column(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                false,
                None,
            )
            .await
            .unwrap()
            .write(vec![
                RecordBatch::try_from_iter([(
                    "id",
                    Arc::new(Int32Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
                )])
                .unwrap(),
            ])
            .await
            .unwrap();
        let files = table
            .get_active_add_actions_by_partitions(&[])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut add = files[0].to_add();
        add.stats = statistics.map(ToOwned::to_owned);
        let (stats, count) =
            physical_row_count(&add, &files[0].object_store_path(), &table.log_store())
                .await
                .unwrap();
        assert_eq!(count, 3);
        assert_eq!(stats["numRecords"], 3);
        add.stats = Some(r#"{"numRecords":-1}"#.into());
        assert!(
            physical_row_count(&add, &files[0].object_store_path(), &table.log_store())
                .await
                .is_err()
        );
    }

    async fn table_with_dv_file(add: Add) -> DeltaTable {
        DeltaTable::new_in_memory()
            .create()
            .with_column(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_configuration_property(crate::TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions([Action::Add(add)])
            .await
            .unwrap()
    }

    fn selected_rows(
        table: &DeltaTable,
        file: &str,
        positions: impl IntoIterator<Item = u64>,
    ) -> DeletedRows {
        let rows = DeletedRows::default();
        let id = normalize_path_as_file_id(
            file,
            table
                .snapshot()
                .unwrap()
                .snapshot()
                .table_configuration()
                .table_root(),
            "test",
        )
        .unwrap();
        rows.insert(id, RoaringTreemap::from_iter(positions));
        rows
    }

    #[rstest::rstest]
    #[case(r#"{"numRecords":3}"#, Some(3))]
    #[case(r#"{"numRecords":0}"#, Some(0))]
    #[case(r#"{"numRecords":-1}"#, None)]
    #[case(r#"{"numRecords":1.5}"#, None)]
    #[case(r#"{"numRecords":"3"}"#, None)]
    #[case("[]", None)]
    #[case("true", None)]
    #[case("{broken", None)]
    #[tokio::test]
    async fn physical_row_count_validates_statistics(
        #[case] statistics: &str,
        #[case] expected: Option<u64>,
    ) {
        let mut add = crate::test_utils::make_test_add("file.parquet", &[], 0);
        add.stats = Some(statistics.to_string());
        let table = DeltaTable::new_in_memory();
        let result =
            physical_row_count(&add, &Path::from("file.parquet"), &table.log_store()).await;
        match expected {
            Some(count) => assert_eq!(result.unwrap().1, count),
            None => assert!(result.is_err()),
        }
    }

    #[rstest::rstest]
    #[case(3)]
    #[case(i64::MAX as u64 + 1)]
    #[tokio::test]
    async fn invalid_physical_positions_fail_before_writing_vectors(#[case] position: u64) {
        let mut add = crate::test_utils::make_test_add("file.parquet", &[], 0);
        add.stats = Some(r#"{"numRecords":3}"#.into());
        let table = table_with_dv_file(add).await;
        let rows = selected_rows(&table, "file.parquet", [position]);
        let error = prepare_deletion_vectors(
            table.snapshot().unwrap().snapshot(),
            &table.log_store(),
            &rows,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("exceeds the physical row count"));
        assert!(
            table
                .object_store()
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .all(|file| !file.location.as_ref().ends_with(".bin"))
        );
    }

    #[tokio::test]
    async fn empty_and_inactive_selections_do_not_write_vectors() {
        let mut add = crate::test_utils::make_test_add("file.parquet", &[], 0);
        add.stats = Some(r#"{"numRecords":3}"#.into());
        let table = table_with_dv_file(add).await;
        let snapshot = table.snapshot().unwrap().snapshot();
        assert!(
            prepare_deletion_vectors(snapshot, &table.log_store(), &DeletedRows::default())
                .await
                .unwrap()
                .is_empty()
        );
        let rows = selected_rows(&table, "inactive.parquet", [0]);
        let error = prepare_deletion_vectors(snapshot, &table.log_store(), &rows)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not active in its snapshot"));
        assert!(
            table
                .object_store()
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .all(|file| !file.location.as_ref().ends_with(".bin"))
        );
    }

    #[rstest::rstest]
    #[case::wrong_cardinality(false)]
    #[case::corrupt_vector(true)]
    #[tokio::test]
    async fn invalid_prior_vectors_fail_before_writing_replacements(#[case] corrupt: bool) {
        let table = DeltaTable::new_in_memory();
        let path = Path::from("prior.bin");
        let root = table.log_store().root_url().clone();
        let mut vector = KernelDeletionVector::new();
        vector.add_deleted_row_indexes([0]);
        let mut bytes = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
        let descriptor = writer.write_deletion_vector(vector).unwrap();
        writer.finalize().unwrap();
        if corrupt {
            let magic = usize::try_from(descriptor.offset).unwrap() + 4;
            bytes[magic] ^= 255;
        }
        table.object_store().put(&path, bytes.into()).await.unwrap();
        let mut add = crate::test_utils::make_test_add("file.parquet", &[], 0);
        add.stats = Some(r#"{"numRecords":3}"#.into());
        add.deletion_vector = Some(DeletionVectorDescriptor {
            storage_type: StorageType::AbsolutePath,
            path_or_inline_dv: root.join(path.as_ref()).unwrap().to_string(),
            offset: Some(descriptor.offset),
            size_in_bytes: descriptor.size_in_bytes,
            cardinality: if corrupt { 1 } else { 2 },
        });
        let table = table
            .create()
            .with_column(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
                None,
            )
            .with_configuration_property(crate::TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions([Action::Add(add)])
            .await
            .unwrap();
        let rows = selected_rows(&table, "file.parquet", [1]);
        let result = prepare_deletion_vectors(
            table.snapshot().unwrap().snapshot(),
            &table.log_store(),
            &rows,
        )
        .await;
        assert!(result.is_err());
        if !corrupt {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("cardinality does not match")
            );
        }
        assert_eq!(
            table
                .object_store()
                .list(None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .filter(|file| file.location.as_ref().ends_with(".bin"))
                .count(),
            1
        );
    }
}
