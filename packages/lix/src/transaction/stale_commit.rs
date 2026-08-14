use std::collections::{BTreeSet, HashSet};

use crate::common::SharedStr;
use crate::forktree::StateKeyRef;

use super::staging::PreparedWriteSet;

const BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, PartialEq, Eq)]
pub(super) enum StaleCommitPlan {
    Direct,
    RevalidateOrdinaryInsert,
    ReconcilePlugin(StalePluginReconciliationPlan),
    Unsafe,
}

impl StaleCommitPlan {
    pub(super) const fn kind(&self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::RevalidateOrdinaryInsert => "revalidate_ordinary_insert",
            Self::ReconcilePlugin(_) => "reconcile_plugin",
            Self::Unsafe => "unsafe",
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) struct StalePluginReconciliationPlan {
    pub(super) semantic_conflict_indices: Vec<usize>,
    pub(super) file_ids: BTreeSet<String>,
}

pub(super) fn classify_stale_commit<'a>(
    prepared_writes: &'a PreparedWriteSet,
    concurrent: impl Iterator<Item = StateKeyRef<'a>>,
) -> StaleCommitPlan {
    let overlapping_indices = indexed_overlap_indices(
        prepared_writes
            .state_rows
            .iter()
            .enumerate()
            .map(|(index, row)| {
                (
                    index,
                    StateKeyRef {
                        schema_key: row.schema_key.as_str(),
                        file_id: row.file_id.map(SharedStr::as_str),
                        entity_pk: row.entity_pk,
                    },
                )
            }),
        concurrent,
    );
    if overlapping_indices.is_empty() {
        return StaleCommitPlan::Direct;
    }

    // Ordinary INSERT races keep their established constraint surface:
    // commit-time validation reports the exact UNIQUE/statement-index error
    // from the current snapshot.
    if overlapping_indices.iter().all(|&index| {
        prepared_writes.insert_selection.contains(index)
            && prepared_writes.state_rows.row(index).file_id.is_none()
    }) {
        return StaleCommitPlan::RevalidateOrdinaryInsert;
    }

    let file_ids = overlapping_indices
        .iter()
        .filter_map(|&index| {
            prepared_writes
                .state_rows
                .row(index)
                .file_id
                .map(ToString::to_string)
        })
        .collect::<BTreeSet<_>>();
    if file_ids.is_empty()
        || overlapping_indices
            .iter()
            .any(|&index| prepared_writes.state_rows.row(index).file_id.is_none())
    {
        return StaleCommitPlan::Unsafe;
    }

    let semantic_conflict_indices = overlapping_indices
        .iter()
        .copied()
        .filter(|&index| {
            !matches!(
                prepared_writes.state_rows.row(index).schema_key.as_str(),
                BLOB_REF_SCHEMA_KEY
            )
        })
        .collect();
    StaleCommitPlan::ReconcilePlugin(StalePluginReconciliationPlan {
        semantic_conflict_indices,
        file_ids,
    })
}

fn indexed_overlap_indices<'a>(
    prepared: impl Iterator<Item = (usize, StateKeyRef<'a>)>,
    concurrent: impl Iterator<Item = StateKeyRef<'a>>,
) -> Vec<usize> {
    let concurrent_keys = concurrent.collect::<HashSet<_>>();
    prepared
        .filter_map(|(index, key)| concurrent_keys.contains(&key).then_some(index))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::catalog::SchemaPlanId;
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::forktree::StateKey;
    use crate::transaction::types::{PreparedRowFacts, PreparedStateBatch};

    fn test_key_ref(key: &StateKey) -> StateKeyRef<'_> {
        StateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        }
    }

    fn overlap_fixture(rows: usize) -> (Vec<StateKey>, Vec<StateKey>) {
        let prepared = (0..rows)
            .map(|index| StateKey {
                schema_key: "plugin_entity".to_owned(),
                file_id: Some("hot-file".to_owned()),
                entity_pk: EntityPk::single(format!("row-{index}")),
            })
            .collect::<Vec<_>>();
        let concurrent = (0..rows)
            .map(|index| StateKey {
                schema_key: "plugin_entity".to_owned(),
                file_id: Some("hot-file".to_owned()),
                entity_pk: EntityPk::single(if index.is_multiple_of(2) {
                    format!("row-{index}")
                } else {
                    format!("other-{index}")
                }),
            })
            .collect::<Vec<_>>();
        (prepared, concurrent)
    }

    fn legacy_nested_overlap_indices(prepared: &[StateKey], concurrent: &[StateKey]) -> Vec<usize> {
        prepared
            .iter()
            .enumerate()
            .filter_map(|(index, key)| concurrent.iter().any(|other| other == key).then_some(index))
            .collect()
    }

    #[test]
    fn indexed_overlap_discovery_matches_legacy_order_and_membership() {
        let (prepared, concurrent) = overlap_fixture(257);
        let indexed = indexed_overlap_indices(
            prepared
                .iter()
                .enumerate()
                .map(|(index, key)| (index, test_key_ref(key))),
            concurrent.iter().map(test_key_ref),
        );
        assert_eq!(
            indexed,
            legacy_nested_overlap_indices(&prepared, &concurrent)
        );
    }

    #[test]
    fn unrelated_owner_does_not_enter_stale_plan() {
        let mut state_rows = PreparedStateBatch::with_capacity(1);
        state_rows.push_parts_with_change_addressability(
            SchemaPlanId::for_test(0),
            PreparedRowFacts::default(),
            EntityPk::single("row-a"),
            "plugin_entity".to_owned().into(),
            Some("file-a".to_owned().into()),
            None,
            None,
            None,
            None,
            LixTimestamp::from_unix_millis_utc_lossy(0),
            LixTimestamp::from_unix_millis_utc_lossy(0),
            false,
            None,
            false,
            None,
            false,
            "main".to_owned().into(),
        );
        let prepared = PreparedWriteSet {
            state_rows,
            insert_selection: super::super::staging::PreparedInsertSelection::new(),
            commit_change_refs_by_branch: Default::default(),
            first_commit_parent_override_by_branch: Default::default(),
            checkpoint_publications: Vec::new(),
            extra_commit_parents_by_branch: Default::default(),
            intermediate_commits: Vec::new(),
            file_content_writes: Vec::new(),
            branch_ref_intents: Vec::new(),
            historical_blob_manifest_edges: Default::default(),
        };
        let unrelated = StateKey {
            schema_key: "plugin_entity".to_owned(),
            file_id: Some("file-b".to_owned()),
            entity_pk: EntityPk::single("row-a"),
        };

        assert_eq!(
            classify_stale_commit(&prepared, std::iter::once(test_key_ref(&unrelated))),
            StaleCommitPlan::Direct
        );
    }

    #[test]
    #[ignore = "release-only stale overlap discovery benchmark probe"]
    fn stale_overlap_discovery_benchmark_probe() {
        let rows = std::env::var("LIX_STALE_OVERLAP_BENCH_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(5_000);
        let rounds = std::env::var("LIX_STALE_OVERLAP_BENCH_ROUNDS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(8);
        let (prepared, concurrent) = overlap_fixture(rows);
        let expected = legacy_nested_overlap_indices(&prepared, &concurrent);

        let mut legacy_samples = Vec::with_capacity(rounds);
        let mut indexed_samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = Instant::now();
            let legacy = std::hint::black_box(legacy_nested_overlap_indices(
                std::hint::black_box(&prepared),
                std::hint::black_box(&concurrent),
            ));
            legacy_samples.push(started.elapsed());
            assert_eq!(legacy, expected);

            let started = Instant::now();
            let indexed = std::hint::black_box(indexed_overlap_indices(
                prepared
                    .iter()
                    .enumerate()
                    .map(|(index, key)| (index, test_key_ref(key))),
                concurrent.iter().map(test_key_ref),
            ));
            indexed_samples.push(started.elapsed());
            assert_eq!(indexed, expected);
        }
        legacy_samples.sort_unstable();
        indexed_samples.sort_unstable();
        let p50 = |samples: &[std::time::Duration]| samples[(samples.len() - 1) / 2];
        let legacy_p50 = p50(&legacy_samples);
        let indexed_p50 = p50(&indexed_samples);
        println!(
            "stale_overlap_discovery rows={rows} overlaps={} rounds={rounds} \
             legacy_p50_us={} indexed_p50_us={} speedup={:.2}",
            expected.len(),
            legacy_p50.as_micros(),
            indexed_p50.as_micros(),
            legacy_p50.as_secs_f64() / indexed_p50.as_secs_f64(),
        );
        assert!(
            indexed_p50 < legacy_p50,
            "indexed overlap discovery should beat the nested legacy scan"
        );
    }
}
