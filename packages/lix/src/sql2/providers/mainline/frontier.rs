//! Bounded discovery across known first-parent checkpoints after a required miss.
//! Keep the first error and never evaluate speculative SQL expressions.
use super::*;
use crate::LixError;
use crate::tracked_state::{NativeMetadataRef, NativeObjectRef};

const MAX_CHECKPOINTS: usize = 16;

/// Extract only a proven conjunctive checkpoint constraint. Do not evaluate SQL
/// expressions or user functions during optional dependency discovery.
pub(super) fn checkpoint_constraint(filters: &[Expr]) -> Option<bool> {
    use datafusion::common::ScalarValue;
    fn column(expr: &Expr) -> bool {
        matches!(expr, Expr::Column(c) if c.name == "lixcol_commit_is_checkpoint")
    }
    filters.iter().find_map(|expr| match expr {
        Expr::Column(_) if column(expr) => Some(true),
        Expr::Not(inner) | Expr::IsFalse(inner) if column(inner) => Some(false),
        Expr::IsTrue(inner) if column(inner) => Some(true),
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (c, Expr::Literal(ScalarValue::Boolean(Some(value)), _))
                | (Expr::Literal(ScalarValue::Boolean(Some(value)), _), c) if column(c) => Some(*value),
                _ => None,
            }
        }
        _ => None,
    })
}

enum MissingFrontier {
    Objects(Vec<NativeObjectRef>),
    Metadata(Vec<NativeMetadataRef>),
}
impl MissingFrontier {
    fn from_error(error: &LixError) -> Result<Option<Self>, LixError> {
        if error.automatic_retry_is_forbidden() {
            return Ok(None);
        }
        if let Some(items) = NativeObjectRef::batch_from_missing_error(error)? {
            return Ok(Some(Self::Objects(items)));
        }
        if let Some(item) = NativeObjectRef::from_missing_error(error)? {
            return Ok(Some(Self::Objects(vec![item])));
        }
        let items = match NativeMetadataRef::batch_from_missing_error(error)? {
            Some(items) => items,
            None => match NativeMetadataRef::from_missing_error(error)? {
                Some(item) => vec![item],
                None => return Ok(None),
            },
        };
        // Discover only within known topology. Hydrating optional graph records
        // one at a time would suppress the ordinary required metadata walk.
        if items.iter().any(|item| matches!(item, NativeMetadataRef::CommitGraphRecord(_))) {
            return Ok(None);
        }
        Ok(Some(Self::Metadata(items)))
    }
    fn len(&self) -> usize {
        match self {
            Self::Objects(v) => v.len(),
            Self::Metadata(v) => v.len(),
        }
    }
    fn full(&self) -> bool {
        match self {
            Self::Objects(v) => v.len() >= NativeObjectRef::MAX_MISSING_BATCH,
            Self::Metadata(v) => v.len() >= NativeMetadataRef::MAX_MISSING_BATCH,
        }
    }
    fn extend(&mut self, next: Self) {
        match (self, next) {
            (Self::Objects(items), Self::Objects(more)) => {
                for item in more {
                    if items.len() >= NativeObjectRef::MAX_MISSING_BATCH {
                        break;
                    }
                    if !items.contains(&item) {
                        items.push(item);
                    }
                }
            }
            (Self::Metadata(items), Self::Metadata(more)) => {
                for item in more {
                    if items.len() >= NativeMetadataRef::MAX_MISSING_BATCH {
                        break;
                    }
                    if !items.contains(&item) {
                        items.push(item);
                    }
                }
            }
            _ => {} // Another dependency layer waits for normal execution.
        }
    }
    fn annotate(self, error: LixError) -> LixError {
        match self {
            Self::Objects(v) => NativeObjectRef::annotate_missing_batch(v, error),
            Self::Metadata(v) => NativeMetadataRef::annotate_missing_batch(v, error),
        }
    }
}

pub(super) async fn discover<S: StorageAdapterRead + Clone + Send + Sync + 'static>(
    diff: &DiffSpec<S>,
    mut next: CommitId,
    projection: &Vec<usize>,
    filters: &[Expr],
    resumed_batches: usize,
    checkpoint: Option<bool>,
    error: LixError,
) -> LixError {
    if resumed_batches == 0 {
        return error;
    }
    let Ok(Some(mut frontier)) = MissingFrontier::from_error(&error) else {
        return error;
    };
    let required = frontier.len();
    let mut graph = CommitGraphContext::new().reader(diff.store.clone());
    let mut seen = BTreeSet::new();
    for _ in 1..resumed_batches.saturating_add(1).min(MAX_CHECKPOINTS) {
        if frontier.full() || !seen.insert(next) {
            break;
        }
        // Stop at unknown topology. The ordinary required graph miss will select
        // the next metadata walk; do not replace it with speculative coordinates.
        let Ok(Some(node)) = graph.load_node(&next).await else {
            break;
        };
        let Some(parent) = node.parent_commit_ids.first().copied() else {
            break;
        };
        if checkpoint.is_some_and(|required| node.is_checkpoint != required) {
            next = parent;
            continue;
        }
        match diff
            .prepare_history_inputs(&parent.to_string(), &next.to_string(), projection, filters)
            .await
        {
            Ok(()) => {}
            Err(speculative) => match MissingFrontier::from_error(&speculative) {
                Ok(Some(more)) => frontier.extend(more),
                _ => break, // Speculative corruption never replaces the required error.
            },
        }
        next = parent;
    }
    if frontier.len() > required {
        crate::tracked_state::NativeHistoryFrontier::annotate_optional_suffix(
            frontier.annotate(error),
            required,
        )
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{SharedStorageAdapterRead, StorageKey, StorageValue};
    use bytes::Bytes;

    #[test]
    fn checkpoint_constraints_require_a_proven_conjunct() {
        use datafusion::prelude::{col, lit};
        let checkpoint = col("lixcol_commit_is_checkpoint");
        assert_eq!(checkpoint_constraint(&[checkpoint.clone()]), Some(true));
        assert_eq!(checkpoint_constraint(&[!checkpoint.clone()]), Some(false));
        assert_eq!(checkpoint_constraint(&[checkpoint.clone().eq(lit(true))]), Some(true));
        assert_eq!(checkpoint_constraint(&[lit(false).eq(checkpoint.clone())]), Some(false));
        assert_eq!(checkpoint_constraint(&[checkpoint.clone().is_true()]), Some(true));
        assert_eq!(checkpoint_constraint(&[checkpoint.clone().is_false()]), Some(false));
        assert_eq!(checkpoint_constraint(&[checkpoint.or(col("other"))]), None);
        assert_eq!(checkpoint_constraint(&[col("other").eq(lit(true))]), None);
        assert_eq!(checkpoint_constraint(&[]), None);
    }

    #[tokio::test]
    async fn discovery_is_bounded_and_preserves_required_error_across_speculative_corruption() {
        let lix = crate::open_lix().await.unwrap();
        let mut commits = Vec::new();
        for i in 0..20 {
            lix.upsert_file_content("/frontier.txt", format!("value {i}").into_bytes())
                .await
                .unwrap();
            let result = lix
                .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .unwrap();
            commits.push(
                CommitId::parse_lix(
                    &result.rows()[0].get::<String>("commit_id").unwrap(),
                    "checkpoint",
                )
                .unwrap(),
            );
        }
        let adapter = lix.storage_adapter();
        let header_space = crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE;
        let mut writes = adapter.new_write_set();
        for id in &commits {
            writes.delete(
                header_space,
                crate::tracked_state::commit_state_authority_key(*id),
            );
        }
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let make_error = || {
            NativeMetadataRef::CommitStateHeader(commits[19].to_string()).annotate_missing(
                LixError::new("REQUIRED_HISTORY_INPUT", "required checkpoint header"),
            )
        };
        let run = async {
            let store = SharedStorageAdapterRead::new(
                adapter.begin_read(Default::default()).await.unwrap(),
            );
            let relation =
                DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file").unwrap();
            let projection = vec![relation.schema.index_of("to_path").unwrap()];
            let diff = DiffSpec {
                blob_reader: Arc::new(
                    crate::binary_cas::BinaryCasContext::new().reader(store.clone()),
                ),
                store,
                read_interests: None,
                interest_endpoints: None,
                relation,
                from_commit_id: commits[18].to_string(),
                to_commit_id: commits[19].to_string(),
                active_branch_id: None,
                mode: DiffMode::General,
            };
            let first = discover(&diff, commits[18], &projection, &[], 0, None, make_error()).await;
            assert!(NativeMetadataRef::batch_from_missing_error(&first).unwrap().is_none());
            assert_eq!(first.details, make_error().details);
            // This known checkpoint is excluded by the metadata selection. Its
            // missing header must not become an optional hydration demand.
            let excluded = discover(&diff, commits[18], &projection, &[], 1, Some(false), make_error()).await;
            assert_eq!(excluded.details, make_error().details);
            let resumed = discover(&diff, commits[18], &projection, &[], 1, Some(true), make_error()).await;
            let short = NativeMetadataRef::batch_from_missing_error(&resumed).unwrap().unwrap();
            assert!((2..=3).contains(&short.len()), "one resumed batch permits only one extra checkpoint");
            discover(&diff, commits[18], &projection, &[], MAX_CHECKPOINTS, None, make_error()).await
        }
        .await;
        let batch = NativeMetadataRef::batch_from_missing_error(&run)
            .unwrap()
            .unwrap();
        assert!(
            batch.len() > 1,
            "independent checkpoint misses should form a frontier"
        );
        assert!(batch.len() <= MAX_CHECKPOINTS);
        assert_eq!(
            batch[0],
            NativeMetadataRef::CommitStateHeader(commits[19].to_string())
        );
        assert_eq!(run.code, "REQUIRED_HISTORY_INPUT");
        assert_eq!(run.message, "required checkpoint header");

        // The first speculative diff reads its older endpoint (checkpoint17).
        // That corruption must not replace the original checkpoint19 miss.
        let mut writes = adapter.new_write_set();
        writes.put(
            header_space,
            StorageKey(crate::tracked_state::commit_state_authority_key(commits[17]).0),
            StorageValue {
                bytes: Bytes::from_static(b"corrupt optional header"),
            },
        );
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let store =
            SharedStorageAdapterRead::new(adapter.begin_read(Default::default()).await.unwrap());
        let relation =
            DiffRelation::from_catalog(PublicCatalog::fixed_system(), "lix_file").unwrap();
        let projection = vec![relation.schema.index_of("to_path").unwrap()];
        let diff = DiffSpec {
            blob_reader: Arc::new(crate::binary_cas::BinaryCasContext::new().reader(store.clone())),
            store,
            read_interests: None,
            interest_endpoints: None,
            relation,
            from_commit_id: commits[18].to_string(),
            to_commit_id: commits[19].to_string(),
            active_branch_id: None,
            mode: DiffMode::General,
        };
        let preserved = discover(&diff, commits[18], &projection, &[], MAX_CHECKPOINTS, None, make_error()).await;
        assert_eq!(preserved.code, run.code);
        assert_eq!(preserved.message, run.message);
        assert!(
            NativeMetadataRef::batch_from_missing_error(&preserved)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            NativeMetadataRef::from_missing_error(&preserved).unwrap(),
            Some(batch[0].clone())
        );

        // Missing speculative topology must stay deferred until normal execution
        // reaches it, so the required graph miss can select a metadata walk.
        let mut writes = adapter.new_write_set();
        crate::changelog::stage_delete_commits(&mut writes, [commits[17]]);
        writes.delete(header_space, crate::tracked_state::commit_state_authority_key(commits[17]));
        adapter.commit_write_set(writes, Default::default()).await.unwrap();
        let store = SharedStorageAdapterRead::new(adapter.begin_read(Default::default()).await.unwrap());
        let graph_diff = DiffSpec {
            blob_reader: Arc::new(crate::binary_cas::BinaryCasContext::new().reader(store.clone())),
            store,
            ..diff
        };
        let missing = graph_diff.prepare_history_inputs(
            &commits[17].to_string(), &commits[18].to_string(), &projection, &[]
        ).await.unwrap_err();
        let addresses = NativeMetadataRef::batch_from_missing_error(&missing).unwrap()
            .unwrap_or_else(|| vec![NativeMetadataRef::from_missing_error(&missing).unwrap().unwrap()]);
        assert!(addresses.iter().any(|item| matches!(item, NativeMetadataRef::CommitGraphRecord(_))));
        let preserved = discover(&graph_diff, commits[18], &projection, &[], MAX_CHECKPOINTS, None, make_error()).await;
        assert_eq!(preserved.details, make_error().details);
        let required_graph = discover(&graph_diff, commits[18], &projection, &[], MAX_CHECKPOINTS, None, missing.clone()).await;
        assert_eq!(required_graph.details, missing.details);

    }
}
