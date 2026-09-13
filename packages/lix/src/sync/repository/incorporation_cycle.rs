use super::{
    CommitId, LixError, StorageAdapterRead, load_commit_record,
    load_published_commit_state_topology,
};
use crate::tracked_state::{CommitStateIncorporation, NativeMetadataRef};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(super) struct Dependencies {
    pub(super) edges: Vec<CommitId>,
}

/// Iterative three-color DFS visits the shared declared graph once across all
/// new source claims. Generation pruning is invalid across complete sources.
pub(super) struct Guard {
    pending: Vec<(CommitId, bool)>,
    active: BTreeSet<CommitId>,
    complete: BTreeSet<CommitId>,
    #[cfg(test)]
    edges: usize,
}

impl Guard {
    pub(super) fn new(roots: impl IntoIterator<Item = CommitId>) -> Self {
        Self {
            pending: roots.into_iter().map(|id| (id, false)).collect(),
            active: BTreeSet::new(),
            complete: BTreeSet::new(),
            #[cfg(test)]
            edges: 0,
        }
    }

    pub(super) async fn run(
        &mut self,
        read: &(impl StorageAdapterRead + ?Sized),
        incoming: &BTreeMap<CommitId, Dependencies>,
        work_slice: usize,
    ) -> Result<bool, LixError> {
        if work_slice == 0 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "incorporation work slice must be positive",
            ));
        }
        let mut work = 0;
        while let Some((id, exiting)) = self.pending.pop() {
            if exiting {
                self.active.remove(&id);
                self.complete.insert(id);
                continue;
            }
            if self.complete.contains(&id) {
                continue;
            }
            if self.active.contains(&id) {
                return Ok(true);
            }
            work += 1;
            if work == work_slice {
                tokio::task::yield_now().await;
                work = 0;
            }
            let dependencies = if let Some(value) = incoming.get(&id) {
                value.clone()
            } else {
                // Keep the unfinished edge so an owner can resume after a
                // typed metadata miss without rescanning the completed prefix.
                self.pending.push((id, false));
                let record = load_commit_record(read, id).await?.ok_or_else(|| {
                    NativeMetadataRef::CommitGraphRecord(id.to_string()).annotate_missing(
                        LixError::new(
                            LixError::CODE_COMMIT_NOT_FOUND,
                            "incorporation cycle proof requires a source graph header",
                        ),
                    )
                })?;
                let topology = load_published_commit_state_topology(read, id)
                    .await?
                    .ok_or_else(|| {
                        NativeMetadataRef::CommitStateHeader(id.to_string()).annotate_missing(
                            LixError::new(
                                LixError::CODE_COMMIT_NOT_FOUND,
                                "incorporation cycle proof requires source provenance",
                            ),
                        )
                    })?;
                let mut edges = record.parent_commit_ids;
                edges.extend(
                    crate::sync::commit::load_complete_state_alias_source(
                        read,
                        id,
                        topology.complete_state_source_commit_id(),
                    )
                    .await?,
                );
                if let CommitStateIncorporation::Complete(source) = topology.incorporation() {
                    edges.push(source);
                }
                // LegacyUnknown is semantic uncertainty, not an undeclared
                // edge in this immutable physical dependency graph.
                let value = Dependencies { edges };
                self.pending.pop();
                value
            };
            self.active.insert(id);
            self.pending.push((id, true));
            #[cfg(test)]
            {
                self.edges += dependencies.edges.len();
            }
            self.pending
                .extend(dependencies.edges.into_iter().map(|edge| (edge, false)));
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter};

    #[tokio::test]
    async fn exact_cycle_guard_has_no_fixed_history_ceiling() {
        let adapter = StorageAdapter::new(Memory::new());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let ids = (0..4097)
            .map(|n| CommitId::for_test_label(&format!("cycle-node-{n}")))
            .collect::<Vec<_>>();
        let mut incoming = BTreeMap::new();
        for (index, id) in ids.iter().enumerate() {
            incoming.insert(
                *id,
                Dependencies {
                    edges: index
                        .checked_sub(1)
                        .map(|previous| ids[previous])
                        .into_iter()
                        .collect(),
                },
            );
        }
        let fresh = CommitId::for_test_label("fresh-cycle-candidate");
        incoming.insert(
            fresh,
            Dependencies {
                edges: vec![*ids.last().unwrap()],
            },
        );
        let mut walk = Guard::new([fresh]);
        assert!(!walk.run(&read, &incoming, 32).await.unwrap());
        assert_eq!(walk.complete.len(), ids.len() + 1);
        incoming.get_mut(&ids[0]).unwrap().edges.push(fresh);
        assert!(Guard::new([fresh]).run(&read, &incoming, 32).await.unwrap());
    }

    #[tokio::test]
    async fn missing_metadata_retains_unfinished_cycle_frontier() {
        let adapter = StorageAdapter::new(Memory::new());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let target = CommitId::for_test_label("cycle-target");
        let source = CommitId::for_test_label("cycle-source");
        let absent = CommitId::for_test_label("cycle-absent");
        let mut incoming = BTreeMap::from([(
            source,
            Dependencies {
                edges: vec![absent],
            },
        )]);
        incoming.insert(
            target,
            Dependencies {
                edges: vec![source],
            },
        );
        let mut walk = Guard::new([target]);
        let missing = walk.run(&read, &incoming, 1).await.unwrap_err();
        assert!(
            NativeMetadataRef::from_missing_error(&missing)
                .unwrap()
                .is_some()
        );
        assert_eq!(walk.active, BTreeSet::from([source, target]));
        incoming.insert(
            absent,
            Dependencies {
                edges: vec![target],
            },
        );
        assert!(walk.run(&read, &incoming, 1).await.unwrap());
        assert_eq!(walk.active, BTreeSet::from([source, absent, target]));
    }

    #[tokio::test]
    async fn shared_ancestry_is_visited_once_for_many_source_claims() {
        let adapter = StorageAdapter::new(Memory::new());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        for history in [32, 3200] {
            for claims in [1, 32] {
                let ids = (0..history + claims)
                    .map(|n| CommitId::for_test_label(&format!("shared-{n}")))
                    .collect::<Vec<_>>();
                let mut incoming = BTreeMap::new();
                for index in 0..history {
                    incoming.insert(
                        ids[index],
                        Dependencies {
                            edges: index
                                .checked_sub(1)
                                .map(|previous| ids[previous])
                                .into_iter()
                                .collect(),
                        },
                    );
                }
                for id in &ids[history..] {
                    incoming.insert(
                        *id,
                        Dependencies {
                            edges: vec![ids[history - 1]],
                        },
                    );
                }
                let mut guard = Guard::new(ids[history..].iter().copied());
                let started = std::time::Instant::now();
                assert!(!guard.run(&read, &incoming, 32).await.unwrap());
                assert_eq!(guard.complete.len(), history + claims);
                assert_eq!(guard.edges, history - 1 + claims);
                eprintln!(
                    "cycle history={history} claims={claims} nodes={} edges={} micros={}",
                    guard.complete.len(),
                    guard.edges,
                    started.elapsed().as_micros()
                );
            }
        }
    }

    #[tokio::test]
    async fn legacy_unknown_declares_no_extra_edges_but_missing_header_is_demanded() {
        use crate::storage_adapter::StorageKey;
        use bytes::Bytes;
        let lix = crate::open_lix().await.unwrap();
        let source = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let source = CommitId::parse_lix(&source, "cycle source").unwrap();
        let adapter = lix.storage_adapter();
        crate::migration::mark_header_incorporation_unknown_for_test(&adapter).await;
        let fresh = CommitId::for_test_label("fresh-legacy-claim");
        let incoming = BTreeMap::from([(
            fresh,
            Dependencies {
                edges: vec![source],
            },
        )]);
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(!Guard::new([fresh]).run(&read, &incoming, 32).await.unwrap());
        drop(read);
        let mut writes = adapter.new_write_set();
        writes.delete(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            StorageKey(Bytes::copy_from_slice(source.as_uuid().as_bytes())),
        );
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let error = Guard::new([fresh])
            .run(&read, &incoming, 32)
            .await
            .unwrap_err();
        assert_eq!(
            NativeMetadataRef::from_missing_error(&error).unwrap(),
            Some(NativeMetadataRef::CommitStateHeader(source.to_string()))
        );
    }
}
