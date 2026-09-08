//! Immutable deletion vector lookup by absolute physical Parquet row position.

use arrow_array::{BooleanArray, Int64Array};
use datafusion::common::{HashMap, Result, exec_err, plan_err};

#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub(super) struct DeletionVectorEntry {
    pub keep_mask: Vec<bool>,
    pub physical_record_count: u64,
    pub deleted_cardinality: u64,
}

#[derive(Debug, Default)]
pub(super) struct DeletionVectorIndex {
    pub(super) entries: HashMap<String, DeletionVectorEntry>,
    selected: HashMap<String, Option<u64>>,
}

impl DeletionVectorIndex {
    pub fn try_new(
        entries: HashMap<String, DeletionVectorEntry>,
        selected: HashMap<String, Option<u64>>,
    ) -> Result<Self> {
        for (file_id, entry) in &entries {
            if selected.get(file_id) != Some(&Some(entry.physical_record_count)) {
                return plan_err!(
                    "selected file row count does not match deletion vector for compact file id '{file_id}'"
                );
            }
            if entry.physical_record_count > i64::MAX as u64 {
                return plan_err!("numRecords exceeds Parquet Int64 coordinates");
            }
            let mask_len = u64::try_from(entry.keep_mask.len()).map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "deletion vector mask length overflows u64 for compact file id '{file_id}'"
                ))
            })?;
            if mask_len > entry.physical_record_count {
                return plan_err!(
                    "deletion vector mask length {mask_len} exceeds numRecords {} for compact file id '{file_id}'",
                    entry.physical_record_count
                );
            }
            let actual_deleted = u64::try_from(
                entry.keep_mask.iter().filter(|keep| !**keep).count(),
            )
            .map_err(|_| {
                datafusion::common::DataFusionError::Plan(format!(
                    "deletion vector cardinality overflows u64 for compact file id '{file_id}'"
                ))
            })?;
            if actual_deleted != entry.deleted_cardinality {
                return plan_err!(
                    "deletion vector cardinality mismatch for compact file id '{file_id}': descriptor={}, actual={actual_deleted}",
                    entry.deleted_cardinality
                );
            }
        }
        Ok(Self { entries, selected })
    }

    pub fn has_vectors(&self) -> bool {
        !self.entries.is_empty()
    }

    pub fn file_count(&self) -> usize {
        self.entries.len()
    }

    pub fn has_vector(&self, compact_file_id: &str) -> bool {
        self.entries.contains_key(compact_file_id)
    }

    pub fn selection_for_positions(
        &self,
        compact_file_id: &str,
        positions: &Int64Array,
    ) -> Result<BooleanArray> {
        let count = self.selected.get(compact_file_id).ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(format!(
                "unknown selected compact file id '{compact_file_id}'"
            ))
        })?;
        let entry = self.entries.get(compact_file_id);
        positions
            .iter()
            .map(|position| {
                let Some(position) = position else {
                    return exec_err!(
                        "null physical row position for compact file id '{compact_file_id}'"
                    );
                };
                let position = u64::try_from(position).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "negative physical row position for compact file id '{compact_file_id}'"
                    ))
                })?;
                if let Some(count) = count && position >= *count {
                    return exec_err!(
                        "physical row position {position} exceeds numRecords {} for compact file id '{compact_file_id}'",
                        count
                    );
                }
                // Rows past the end of the mask are kept.
                let keep = match entry {
                    Some(entry) if position < entry.keep_mask.len() as u64 => {
                        let index = usize::try_from(position).map_err(|_| datafusion::common::DataFusionError::Execution("materialized position overflows usize".into()))?;
                        entry.keep_mask[index]
                    }
                    _ => true,
                };
                Ok(keep)
            })
            .collect::<Result<BooleanArray>>()
    }

    pub fn live_row_count(
        &self,
        compact_file_id: &str,
        physical_record_count: usize,
    ) -> Result<usize> {
        let Some(count) = self.selected.get(compact_file_id) else {
            return exec_err!("unknown selected compact file id '{compact_file_id}'");
        };
        let supplied_record_count = u64::try_from(physical_record_count).map_err(|_| {
            datafusion::common::DataFusionError::Execution(format!(
                "physical record count overflows u64 for compact file id '{compact_file_id}'"
            ))
        })?;
        if count.is_some_and(|count| count != supplied_record_count) {
            return exec_err!(
                "physical record count mismatch for compact file id '{compact_file_id}'"
            );
        }
        let Some(entry) = self.entries.get(compact_file_id) else {
            return Ok(physical_record_count);
        };
        usize::try_from(entry.physical_record_count - entry.deleted_cardinality).map_err(|_| {
            datafusion::common::DataFusionError::Execution(format!(
                "live row count overflows usize for compact file id '{compact_file_id}'"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use datafusion::common::HashMap;

    use super::*;

    fn index(mask: Vec<bool>, records: u64, deleted: u64) -> Result<DeletionVectorIndex> {
        DeletionVectorIndex::try_new(
            HashMap::from([(
                "7".to_string(),
                DeletionVectorEntry {
                    keep_mask: mask,
                    physical_record_count: records,
                    deleted_cardinality: deleted,
                },
            )]),
            HashMap::from([("7".to_string(), Some(records))]),
        )
    }

    #[test]
    fn unknown_file_cannot_be_counted_as_all_live() -> Result<()> {
        let index = index(vec![false], 3, 1)?;
        assert!(index.live_row_count("unknown", 3).is_err());
        Ok(())
    }

    #[test]
    fn large_implicit_tail_needs_no_materialized_mask() -> Result<()> {
        let index = index(vec![false], (1_u64 << 40) + 1, 1)?;
        assert_eq!(
            index
                .selection_for_positions("7", &Int64Array::from(vec![1_i64 << 40]))?
                .true_count(),
            1
        );
        Ok(())
    }

    #[test]
    fn selects_by_absolute_position_and_keeps_sparse_tail() -> Result<()> {
        let index = index(vec![true, false, true], 6, 1)?;
        let positions = Int64Array::from(vec![5, 1, 3, 0, 2]);

        let selection = index.selection_for_positions("7", &positions)?;

        assert_eq!(
            selection.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(true), Some(true), Some(true),]
        );
        assert_eq!(index.live_row_count("7", 6)?, 5);
        assert!(index.live_row_count("7", 5).is_err());
        assert!(index.live_row_count("7", 7).is_err());
        Ok(())
    }

    #[test]
    fn rejects_invalid_positions() -> Result<()> {
        let index = index(vec![true, false], 3, 1)?;

        for positions in [
            Int64Array::from(vec![Some(0), None]),
            Int64Array::from(vec![-1]),
            Int64Array::from(vec![3]),
        ] {
            assert!(index.selection_for_positions("7", &positions).is_err());
        }
        assert!(
            index
                .selection_for_positions("unknown", &Int64Array::from(vec![0]))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_planning_metadata() {
        assert!(index(vec![true, false, true], 2, 1).is_err());
        assert!(index(vec![true, false], 2, 0).is_err());
        assert!(index(vec![true, false], 2, 2).is_err());
    }

    #[test]
    fn all_live_and_all_deleted_masks_remain_valid() -> Result<()> {
        let all_live = index(Vec::new(), 3, 0)?;
        assert_eq!(all_live.live_row_count("7", 3)?, 3);

        let all_deleted = index(vec![false, false, false], 3, 3)?;
        let selection =
            all_deleted.selection_for_positions("7", &Int64Array::from(vec![0, 1, 2]))?;
        assert_eq!(selection.true_count(), 0);
        assert_eq!(all_deleted.live_row_count("7", 3)?, 0);
        Ok(())
    }

    #[test]
    fn indexed_lookup_matches_reference_for_reordered_predicate_results() -> Result<()> {
        let mut state = 0x5eed_cafe_u64;
        for record_count in 1..=64_u64 {
            let mut full_mask = Vec::with_capacity(record_count as usize);
            let mut deleted_positions = std::collections::HashSet::new();
            for position in 0..record_count {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                if state.is_multiple_of(5) {
                    deleted_positions.insert(position as i64);
                }
                full_mask.push(!state.is_multiple_of(5));
            }
            let deleted = full_mask.iter().filter(|keep| !**keep).count() as u64;
            let sparse_len = full_mask
                .iter()
                .rposition(|keep| !*keep)
                .map_or(0, |position| position + 1);
            let sparse_mask = full_mask[..sparse_len].to_vec();
            let index = index(sparse_mask.clone(), record_count, deleted)?;

            let mut positions = (0..record_count as i64)
                .filter(|position| position % 3 != 1)
                .collect::<Vec<_>>();
            positions.reverse();
            let selection =
                index.selection_for_positions("7", &Int64Array::from(positions.clone()))?;

            let actual = positions
                .into_iter()
                .zip(selection.values())
                .filter_map(|(position, keep)| keep.then_some(position))
                .collect::<Vec<_>>();
            let expected = (0..record_count as i64)
                .filter(|position| position % 3 != 1)
                .rev()
                .filter(|position| !deleted_positions.contains(position))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "record_count={record_count}");
        }
        Ok(())
    }
}
