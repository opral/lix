//! Opening metadata for a partial replica with on-demand sync.
//!
//! This is a coordinate descriptor, not a complete-state certificate or an
//! installed replica. Native roots identify objects to hydrate later; absent
//! roots never imply that a query's result is empty.
//!
//! Native snapshot headers store zero or one physical first-parent source,
//! not the commit graph's merge ancestry. Canonical writers keep these headers
//! compact; their parent-vector representation alone is not an enforced wire
//! byte bound. Default-branch resolution uses an indexed tracked point read.
//! The descriptor avoids repository-wide work but does not promise constant
//! storage-tree height or latency independent of storage backend behavior.

use serde::{Deserialize, Serialize};

pub(crate) const PARTIAL_REPLICA_DESCRIPTOR_VERSION: u32 = 1;
pub(crate) const MAX_PARTIAL_REPLICA_DESCRIPTOR_BYTES: usize = 4096;

use crate::branch::BranchHeadControlContext;
use crate::changelog::CommitId;
use crate::storage_adapter::{Storage, StorageAdapterRead, StorageReadOptions};
use crate::tracked_state::load_published_commit_state_topology;
use crate::{Lix, LixError};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartialReplicaDescriptor {
    pub(crate) descriptor_version: u32,
    pub(crate) lix_id: String,
    pub(crate) default_branch_id: String,
    pub(crate) cursor: u64,
    pub(crate) selected_branch: PartialReplicaBranch,
    pub(crate) global_branch: PartialReplicaBranch,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartialReplicaBranch {
    pub(crate) branch_id: String,
    /// Canonical public branch-ref metadata copied from the same control read.
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) ref_change_id: String,
    pub(crate) head: PartialReplicaCommitRoots,
    pub(crate) checkpoint: PartialReplicaCommitRoots,
}

/// Native immutable serving and identity-catalog roots. These are deliberately
/// distinct from sync's complete live-value hash, which requires reading rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartialReplicaCommitRoots {
    pub(crate) commit_id: String,
    pub(crate) scoped_range_root_id: Option<[u8; 32]>,
    pub(crate) scoped_range_root_digest: Option<[u8; 32]>,
    pub(crate) row_pk_index_root_id: Option<[u8; 32]>,
}

impl PartialReplicaDescriptor {
    /// Validate wire or durable opening metadata before it supplies local
    /// branch controls. Object residency and coverage require separate proof.
    pub(crate) fn validate(
        &self,
        expected_lix_id: &str,
        selected_branch_id: Option<&str>,
    ) -> Result<(), LixError> {
        if self.descriptor_version != PARTIAL_REPLICA_DESCRIPTOR_VERSION {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "unsupported partial replica descriptor version",
            ));
        }
        if self.lix_id != expected_lix_id {
            return Err(super::sync_repository_id_mismatch(
                expected_lix_id,
                &self.lix_id,
            ));
        }
        let expected_branch = selected_branch_id.unwrap_or(&self.default_branch_id);
        if self.selected_branch.branch_id != expected_branch
            || self.global_branch.branch_id != crate::GLOBAL_BRANCH_ID
        {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "partial replica descriptor branch coordinates disagree with the request",
            ));
        }
        for id in [
            &self.lix_id,
            &self.default_branch_id,
            &self.selected_branch.branch_id,
            &self.global_branch.branch_id,
        ] {
            if crate::storage_codec::id_string::uuid_bytes_from_canonical(id).is_none() {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "partial replica descriptor contains an invalid repository or branch ID",
                ));
            }
        }
        if self.selected_branch.branch_id == self.global_branch.branch_id
            && self.selected_branch != self.global_branch
        {
            return Err(LixError::new(
                super::SYNC_PROTOCOL_MISMATCH_CODE,
                "partial replica descriptor repeats conflicting branch coordinates",
            ));
        }
        for branch in [&self.selected_branch, &self.global_branch] {
            if branch.head.commit_id == branch.checkpoint.commit_id
                && branch.head != branch.checkpoint
            {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "partial replica descriptor repeats conflicting commit roots",
                ));
            }
            if crate::common::LixTimestamp::parse(&branch.created_at).is_err()
                || crate::common::LixTimestamp::parse(&branch.updated_at).is_err()
                || crate::changelog::ChangeId::parse(&branch.ref_change_id).is_err()
                || crate::storage_codec::id_string::uuid_bytes_from_canonical(&branch.ref_change_id)
                    .is_none()
            {
                return Err(LixError::new(
                    super::SYNC_PROTOCOL_MISMATCH_CODE,
                    "partial replica descriptor contains invalid branch-ref metadata",
                ));
            }
            for roots in [&branch.head, &branch.checkpoint] {
                if crate::storage_codec::id_string::uuid_bytes_from_canonical(&roots.commit_id)
                    .is_none()
                    || roots.scoped_range_root_id.is_some()
                        != roots.scoped_range_root_digest.is_some()
                    || [
                        &roots.scoped_range_root_id,
                        &roots.scoped_range_root_digest,
                        &roots.row_pk_index_root_id,
                    ]
                    .into_iter()
                    .any(|root| *root == Some([0; 32]))
                {
                    return Err(LixError::new(
                        super::SYNC_PROTOCOL_MISMATCH_CODE,
                        "partial replica descriptor contains invalid native root coordinates",
                    ));
                }
            }
        }
        Ok(())
    }
}

async fn commit_roots(
    read: &(impl StorageAdapterRead + ?Sized),
    commit_id: CommitId,
) -> Result<PartialReplicaCommitRoots, LixError> {
    // Header-only load: never read mutation inventories, history, tree pages,
    // complete live-value certificates, or blob payloads here.
    let topology = load_published_commit_state_topology(read, commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("partial replica commit '{commit_id}' has no authority header"),
            )
        })?;
    let scoped = topology.current_state_scoped_ranges();
    Ok(PartialReplicaCommitRoots {
        commit_id: commit_id.to_string(),
        scoped_range_root_id: scoped.map(|root| root.tree.root_id),
        scoped_range_root_digest: scoped.map(|root| root.tree.root_digest),
        row_pk_index_root_id: topology.row_pk_index_root_id().map(|root| *root.as_bytes()),
    })
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Read selected/default and global coordinates from one storage snapshot.
    /// The number of requested branch controls and commit headers is constant.
    /// This primitive does not activate sync or certify local data coverage.
    pub(crate) async fn partial_replica_descriptor(
        &self,
        selected_branch_id: Option<&str>,
    ) -> Result<PartialReplicaDescriptor, LixError> {
        if let Some(branch_id) = selected_branch_id {
            super::validate_sync_branch_id(branch_id)?;
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        self.partial_replica_descriptor_with_read(&read, selected_branch_id)
            .await
    }

    pub(super) async fn partial_replica_descriptor_with_read(
        &self,
        read: &(impl StorageAdapterRead + ?Sized),
        selected_branch_id: Option<&str>,
    ) -> Result<PartialReplicaDescriptor, LixError> {
        let default_branch_id = self.repository_default_branch_id_for_sync(read).await?;
        let selected_branch_id = selected_branch_id.unwrap_or(&default_branch_id);
        let branch_ids = [
            selected_branch_id.to_owned(),
            crate::GLOBAL_BRANCH_ID.to_owned(),
        ];
        let controls = BranchHeadControlContext::default()
            .reader(read)
            .load_many(&branch_ids)
            .await?;
        let mut branches = Vec::with_capacity(2);
        for (branch_id, control) in branch_ids.into_iter().zip(controls) {
            let control = control.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("partial replica branch '{branch_id}' does not exist"),
                )
            })?;
            let checkpoint_id = control.working_diff_checkpoint_commit_id.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("partial replica branch '{branch_id}' has no checkpoint"),
                )
            })?;
            let head = commit_roots(read, control.head_commit_id).await?;
            let checkpoint = if checkpoint_id == control.head_commit_id {
                head.clone()
            } else {
                commit_roots(read, checkpoint_id).await?
            };
            branches.push(PartialReplicaBranch {
                branch_id,
                created_at: control.created_at.to_string(),
                updated_at: control.updated_at.to_string(),
                ref_change_id: control.ref_change_id.to_string(),
                head,
                checkpoint,
            });
        }
        let global_branch = branches.pop().expect("global branch was loaded");
        let selected_branch = branches.pop().expect("selected branch was loaded");
        let (cursor, _) = super::repository::load_sequence(read).await?;
        Ok(PartialReplicaDescriptor {
            descriptor_version: PARTIAL_REPLICA_DESCRIPTOR_VERSION,
            lix_id: self.lix_id().to_owned(),
            default_branch_id,
            cursor,
            selected_branch,
            global_branch,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{
        BeginScanOptions, GetManyRequest, GetManyResult, KeyRange, ProjectedValue, ScanCursor,
        StorageError, StorageSpace,
    };
    use crate::storage_adapter::StorageAdapterRead;
    use crate::{CreateBranchOptions, Value, open_lix};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct ReadMetrics {
        calls: AtomicUsize,
        keys: AtomicUsize,
        bytes: AtomicUsize,
        scans: AtomicUsize,
    }

    struct CountedRead<R> {
        inner: R,
        metrics: ReadMetrics,
    }

    impl<R: StorageAdapterRead> StorageAdapterRead for CountedRead<R> {
        // Deliberately do not expose the wrapped snapshot's cache key: these
        // measurements must not be satisfied by a prior SQL snapshot cache.
        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            self.metrics.calls.fetch_add(1, Ordering::Relaxed);
            self.metrics.keys.fetch_add(
                requests.iter().map(|request| request.keys.len()).sum(),
                Ordering::Relaxed,
            );
            let result = self.inner.get_many(requests).await?;
            self.metrics.bytes.fetch_add(
                result
                    .values
                    .iter()
                    .flatten()
                    .map(|value| match value {
                        ProjectedValue::FullValue(bytes) => bytes.len(),
                        ProjectedValue::KeyOnly => 0,
                    })
                    .sum(),
                Ordering::Relaxed,
            );
            Ok(result)
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            opts: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.metrics.scans.fetch_add(1, Ordering::Relaxed);
            self.inner.begin_scan(space, range, opts).await
        }
    }

    #[tokio::test]
    async fn descriptor_selects_default_or_explicit_branch() {
        let lix = open_lix().await.unwrap();
        let default = lix.partial_replica_descriptor(None).await.unwrap();
        assert_eq!(default.lix_id, lix.lix_id());
        assert_eq!(default.selected_branch.branch_id, default.default_branch_id);
        assert_eq!(default.global_branch.branch_id, crate::GLOBAL_BRANCH_ID);
        let other = lix
            .create_branch(CreateBranchOptions {
                id: None,
                name: "other".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let selected = lix
            .partial_replica_descriptor(Some(&other.id))
            .await
            .unwrap();
        assert_eq!(selected.selected_branch.branch_id, other.id);
        assert_eq!(selected.default_branch_id, default.default_branch_id);
        assert!(
            lix.partial_replica_descriptor(Some("invalid"))
                .await
                .is_err()
        );
        assert!(
            lix.partial_replica_descriptor(Some("01920000-0000-7000-8000-000000000999"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn descriptor_carries_exact_native_branch_ref_metadata() {
        let lix = open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        descriptor.validate(lix.lix_id(), None).unwrap();
        let read = lix
            .storage_adapter()
            .begin_read(Default::default())
            .await
            .unwrap();
        for branch in [&descriptor.selected_branch, &descriptor.global_branch] {
            let control = crate::branch::BranchHeadControlContext::default()
                .reader(&read)
                .load(&branch.branch_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(branch.created_at, control.created_at.to_string());
            assert_eq!(branch.updated_at, control.updated_at.to_string());
            assert_eq!(branch.ref_change_id, control.ref_change_id.to_string());
            assert_eq!(
                crate::common::LixTimestamp::parse(&branch.created_at).unwrap(),
                control.created_at
            );
            assert_eq!(
                crate::changelog::ChangeId::parse(&branch.ref_change_id).unwrap(),
                control.ref_change_id
            );
        }
        let encoded = serde_json::to_vec(&descriptor).unwrap();
        assert!(encoded.len() <= super::MAX_PARTIAL_REPLICA_DESCRIPTOR_BYTES);
        let restored: super::PartialReplicaDescriptor = serde_json::from_slice(&encoded).unwrap();
        restored.validate(lix.lix_id(), None).unwrap();
        assert_eq!(restored, descriptor);
    }

    #[tokio::test]
    async fn descriptor_payload_does_not_accumulate_rows_branches_or_history() {
        let lix = open_lix().await.unwrap();
        for count in [0, 16, 64] {
            for index in 0..count {
                lix.execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                    &[Value::Text(format!("entry-{index}")), Value::Text("x".repeat(1024))],
                ).await.unwrap();
            }
            if count > 0 {
                for index in 0..8 {
                    lix.create_branch(CreateBranchOptions {
                        id: None,
                        name: format!("branch-{count}-{index}"),
                        from_commit_id: None,
                    })
                    .await
                    .unwrap();
                }
            }
            let adapter = lix.storage_adapter();
            let read = CountedRead {
                inner: adapter.begin_read(Default::default()).await.unwrap(),
                metrics: ReadMetrics::default(),
            };
            let started = std::time::Instant::now();
            let descriptor = lix
                .partial_replica_descriptor_with_read(&read, None)
                .await
                .unwrap();
            let elapsed = started.elapsed();
            let encoded = serde_json::to_vec(&descriptor).unwrap();
            eprintln!(
                "partial-replica descriptor row/history scale={count}: {} bytes in {elapsed:?}",
                encoded.len()
            );
            eprintln!(
                "descriptor input I/O: calls={} keys={} bytes={} scans={}",
                read.metrics.calls.load(Ordering::Relaxed),
                read.metrics.keys.load(Ordering::Relaxed),
                read.metrics.bytes.load(Ordering::Relaxed),
                read.metrics.scans.load(Ordering::Relaxed)
            );
            assert_eq!(
                read.metrics.scans.load(Ordering::Relaxed),
                0,
                "opening must not enumerate storage"
            );
            assert!(
                encoded.len() <= super::MAX_PARTIAL_REPLICA_DESCRIPTOR_BYTES,
                "descriptor grew to {} bytes",
                encoded.len()
            );
            for branch in [&descriptor.selected_branch, &descriptor.global_branch] {
                for roots in [&branch.head, &branch.checkpoint] {
                    // Initial empty branches can lack scoped/catalog roots.
                    // Missing metadata is not an empty-domain certificate.
                    assert_eq!(
                        roots.scoped_range_root_id.is_some(),
                        roots.scoped_range_root_digest.is_some()
                    );
                    crate::changelog::CommitId::parse_lix(&roots.commit_id, "descriptor commit")
                        .unwrap();
                }
            }
        }
    }

    async fn seed_profile_rows(lix: &crate::Lix, rows: usize) {
        // One planned SQL statement per batch, not one SQL transaction per row.
        for start in (0..rows).step_by(256) {
            let stop = (start + 256).min(rows);
            let mut params = Vec::with_capacity((stop - start) * 2);
            let values = (start..stop)
                .map(|index| {
                    params.push(Value::Text(format!("descriptor-row-{index:06}")));
                    params.push(Value::Text(format!(
                        "payload-{index:06}-{}",
                        "x".repeat(128)
                    )));
                    format!("(${}, ${})", params.len() - 1, params.len())
                })
                .collect::<Vec<_>>()
                .join(",");
            lix.execute(
                &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                &params,
            )
            .await
            .unwrap();
        }
    }

    async fn profile_descriptor_inputs(
        lix: &crate::Lix,
        label: &str,
        rows: usize,
        branches: usize,
        history: usize,
        schemas: usize,
    ) {
        let selected_id = lix.active_branch_id().await.unwrap();
        for (selection, requested) in [("default", None), ("explicit", Some(selected_id.as_str()))]
        {
            // Fresh uncached coherent reads expose tree routing and header I/O.
            // Both modes intentionally include default-ID metadata resolution.
            for sample in 0..3 {
                let read = CountedRead {
                    inner: lix
                        .storage_adapter()
                        .begin_read(Default::default())
                        .await
                        .unwrap(),
                    metrics: ReadMetrics::default(),
                };
                let started = std::time::Instant::now();
                let descriptor = lix
                    .partial_replica_descriptor_with_read(&read, requested)
                    .await
                    .unwrap();
                let elapsed = started.elapsed();
                let wire_bytes = serde_json::to_vec(&descriptor).unwrap().len();
                let scans = read.metrics.scans.load(Ordering::Relaxed);
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "profile": "partial_replica_descriptor", "fixture": label,
                        "rows": rows, "branches": branches, "history": history, "schemas": schemas,
                        "selection": selection, "sample": sample,
                        "elapsedMicros": elapsed.as_micros(), "wireBytes": wire_bytes,
                        "getCalls": read.metrics.calls.load(Ordering::Relaxed),
                        "getKeys": read.metrics.keys.load(Ordering::Relaxed),
                        "readBytes": read.metrics.bytes.load(Ordering::Relaxed), "scans": scans,
                    })
                );
                assert_eq!(
                    scans, 0,
                    "descriptor must not enumerate storage ({label}/{selection})"
                );
                assert!(wire_bytes <= super::MAX_PARTIAL_REPLICA_DESCRIPTOR_BYTES);
            }
        }
    }

    #[tokio::test]
    #[ignore = "manual large-fixture partial replica descriptor I/O profile"]
    async fn partial_replica_descriptor_large_profile() {
        for (label, rows, branches, history, schemas) in [
            ("base", 16, 0, 0, 0),
            ("rows_1600", 1600, 0, 0, 0),
            ("rows_16000", 16000, 0, 0, 0),
            ("branches", 16, 128, 0, 0),
            ("history", 16, 0, 256, 0),
            ("schemas", 16, 0, 0, 128),
        ] {
            eprintln!("seeding descriptor profile {label}");
            let lix = open_lix().await.unwrap();
            lix.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
            seed_profile_rows(&lix, rows).await;
            for index in 0..branches {
                lix.create_branch(CreateBranchOptions {
                    id: None,
                    name: format!("descriptor-branch-{index}"),
                    from_commit_id: None,
                })
                .await
                .unwrap();
            }
            for index in 0..history {
                lix.execute(
                    "UPDATE lix_key_value SET value = $1 WHERE key = 'descriptor-row-000000'",
                    &[Value::Text(format!("history-{index}"))],
                )
                .await
                .unwrap();
            }
            if schemas > 0 {
                let statements = (0..schemas).map(|index| crate::ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_registered_schema (value, lixcol_global) VALUES ($1, true)".into(),
                    params: vec![Value::Jsonb(serde_json::json!({
                        "$schema": "https://lix.dev/schema-v1.json",
                        "key": format!("descriptor_schema_{index}"),
                        "columns": [{"name":"id", "type":"text", "nullable":false}],
                        "primary_key": ["id"],
                    }).into())],
                }).collect::<Vec<_>>();
                lix.execute_batch(&statements).await.unwrap();
            }
            // Rooted checkpoints put large current state into its native tree;
            // fixture construction and materialization are outside the timer.
            lix.create_checkpoint().await.unwrap();
            profile_descriptor_inputs(&lix, label, rows, branches, history, schemas).await;
            lix.close().await.unwrap();
        }
    }
}
