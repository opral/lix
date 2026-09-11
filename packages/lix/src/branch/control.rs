//! Durable branch-head control records.
//!
//! A branch head is a tiny mutable control-plane record, not a user row.  It
//! therefore has its own space and exact-byte CAS token.  The current tracked
//! serving generation lives beside the head, so readers can bind packed
//! hot rows to the same atomic publication without consulting
//! `lix_branch_ref` through the mutable live-state index.

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_64;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageBeginScanOptions, StorageGetOptions, StorageKey,
    StoragePrecondition, StoragePrefix, StorageProjectedValue, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::storage_codec;

pub(crate) const BRANCH_HEAD_CONTROL_NAMESPACE: &str = "branch.head_control.v11";
pub(crate) const BRANCH_HEAD_CONTROL_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0020),
    BRANCH_HEAD_CONTROL_NAMESPACE,
    ValueSemantics::Mutable,
);

const SCHEMA_PRESENCE_BLOOM_WORDS: usize = 4;
const BRANCH_HEAD_CONTROL_MAGIC: &[u8; 4] = b"LBC1";
const BRANCH_HEAD_CONTROL_DIGEST_BYTES: usize = 32;
const BRANCH_HEAD_CONTROL_DIGEST_CONTEXT: &str = "lix branch-head control v1";

/// The one mutable publication record for a branch.
///
/// `tracked_generation` is the branch's single authenticated serving snapshot.
/// Tracked and history-free untracked rows share it; the per-row
/// `HEAD_VALUE_UNTRACKED` flag is what separates the two semantic planes, not
/// a second generation root. An untracked-only publication therefore mutates
/// this generation in place and never mints a new one.
/// The optional checkpoint binds the sparse working-diff accelerator to that
/// exact generation. `current_state_revision` advances for every in-place
/// current-state mutation, including history-free untracked writes. It is
/// private storage protocol state and turns the control's CAS into a real
/// write fence even when the public branch ref does not move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct BranchHeadControl {
    pub(crate) head_commit_id: CommitId,
    pub(crate) tracked_generation: CommitId,
    pub(crate) current_state_revision: u64,
    #[musli(with = storage_codec::option)]
    pub(crate) working_diff_checkpoint_commit_id: Option<CommitId>,
    /// Stable public `lixcol_created_at` for the synthesized branch-ref row.
    pub(crate) created_at: LixTimestamp,
    /// Public `lixcol_updated_at` for the last head publication.
    pub(crate) updated_at: LixTimestamp,
    /// Public `lixcol_change_id` for the last head publication.
    pub(crate) ref_change_id: ChangeId,
    /// Conservative schema-presence summary for this complete hot generation.
    ///
    /// Bits are only added during in-place commits. Lifecycle publications
    /// rebuild the summary from their complete snapshot. A false result can
    /// therefore skip an otherwise-empty schema range scan; a collision only
    /// falls back to the normal scan.
    pub(crate) schema_presence_bloom: [u64; SCHEMA_PRESENCE_BLOOM_WORDS],
}

/// Canonical classification of one authenticated branch control for
/// destructive reachability work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BranchHeadTrackedReachability {
    /// Semantic chronology roots. These must have commit graph and physical
    /// manifest authority.
    pub(crate) chronology_roots: [Option<CommitId>; 2],
    /// Current-serving selector. This UUID is interpreted only with the
    /// branch id through `TrackedHead`; it is not itself a semantic commit.
    pub(crate) serving_generation: CommitId,
    /// Checkpoint context required to interpret the serving generation's
    /// sparse working-diff visibility. This is already a chronology root
    /// above; carrying it here prevents serving-owner readers from selecting
    /// control fields independently.
    pub(crate) serving_checkpoint_commit_id: Option<CommitId>,
}

impl BranchHeadControl {
    /// Canonical tracked projection for destructive reachability work.
    ///
    /// `tracked_generation` is the atomic serving selector, not chronology.
    /// Its row/scoped owners are resolved through the authenticated
    /// `TrackedHead` reader before GC evaluates retirement. Untracked rows
    /// live in that same generation and are skipped by the commit-provenance
    /// projection because they carry no `commit_id`.
    pub(crate) fn tracked_reachability(self) -> BranchHeadTrackedReachability {
        BranchHeadTrackedReachability {
            chronology_roots: [
                Some(self.head_commit_id),
                self.working_diff_checkpoint_commit_id,
            ],
            serving_generation: self.tracked_generation,
            serving_checkpoint_commit_id: self.working_diff_checkpoint_commit_id,
        }
    }

    /// Returns the same public branch ref with a fresh private current-state
    /// revision. A mutable hot-row write must publish this alongside its exact
    /// control precondition; otherwise two writers could both compare the
    /// same unchanged control bytes and lose one hot-state update.
    pub(crate) fn next_current_state_revision(mut self) -> Result<Self, LixError> {
        self.current_state_revision =
            self.current_state_revision.checked_add(1).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "branch current-state revision overflowed",
                )
            })?;
        Ok(self)
    }

    pub(crate) fn note_schema(&mut self, schema_key: &str) {
        let hash = xxh3_64(schema_key.as_bytes()).to_be_bytes();
        for pair in hash.chunks_exact(2) {
            let bit = usize::from(u16::from_be_bytes([pair[0], pair[1]]))
                % (SCHEMA_PRESENCE_BLOOM_WORDS * u64::BITS as usize);
            self.schema_presence_bloom[bit / u64::BITS as usize] |=
                1_u64 << (bit % u64::BITS as usize);
        }
    }

    pub(crate) fn note_schemas<'a>(&mut self, schema_keys: impl IntoIterator<Item = &'a str>) {
        for schema_key in schema_keys {
            self.note_schema(schema_key);
        }
    }

    pub(crate) fn reset_schema_presence(&mut self) {
        self.schema_presence_bloom = [0; SCHEMA_PRESENCE_BLOOM_WORDS];
    }

    pub(crate) fn may_have_schema(&self, schema_key: &str) -> bool {
        let hash = xxh3_64(schema_key.as_bytes()).to_be_bytes();
        hash.chunks_exact(2).all(|pair| {
            let bit = usize::from(u16::from_be_bytes([pair[0], pair[1]]))
                % (SCHEMA_PRESENCE_BLOOM_WORDS * u64::BITS as usize);
            self.schema_presence_bloom[bit / u64::BITS as usize]
                & (1_u64 << (bit % u64::BITS as usize))
                != 0
        })
    }
}

/// One coherent point-read observation used for both generation selection and
/// the final exact-byte CAS guard. Keeping the decoded control and original
/// bytes together prevents a materializer from issuing a second control read
/// merely to build its publication precondition.
#[derive(Debug, Clone)]
pub(crate) struct BranchHeadControlObservation {
    pub(crate) control: Option<BranchHeadControl>,
    pub(crate) raw_token: Option<Bytes>,
}

#[derive(musli::Encode, musli::Decode)]
#[musli(packed)]
struct BranchHeadControlKey {
    branch_id: String,
}

#[derive(musli::Encode)]
#[musli(packed)]
struct BranchHeadControlKeyRef<'a> {
    branch_id: &'a str,
}

/// Read-side access for direct branch-head control records.
pub(crate) struct BranchHeadControlReader<S> {
    store: S,
}

impl<S> BranchHeadControlReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn load(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchHeadControl>, LixError> {
        let mut values = self.load_many(&[branch_id.to_string()]).await?;
        Ok(values.pop().flatten())
    }

    /// Preserves request cardinality and order, including duplicates.
    pub(crate) async fn load_many(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<Option<BranchHeadControl>>, LixError> {
        Ok(self
            .load_observed(branch_ids)
            .await?
            .into_iter()
            .map(|observation| observation.control)
            .collect())
    }

    /// One point batch that returns both the decoded control and its opaque
    /// exact persisted bytes. Publication callers must retain this result
    /// through their write-set construction rather than reading the control
    /// again for CAS.
    pub(crate) async fn load_observed(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<BranchHeadControlObservation>, LixError> {
        if branch_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = branch_ids
            .iter()
            .map(|branch_id| Ok(StorageKey(Bytes::from(encode_key(branch_id)?))))
            .collect::<Result<Vec<_>, LixError>>()?;
        let values = PointReadPlan::new(BRANCH_HEAD_CONTROL_SPACE, &keys)
            .materialize(&self.store, StorageGetOptions::default())
            .await?
            .value;
        if branch_ids
            .iter()
            .zip(&values)
            .any(|(_, value)| value.is_none())
            && let Some((state, _)) = crate::sync::load_partial_replica_state(&self.store).await?
        {
            let mut missing = branch_ids
                .iter()
                .zip(&values)
                .filter_map(|(id, value)| (value.is_none()).then_some(id))
                .collect::<Vec<_>>();
            let absent = absent_native_branch_descriptors(&self.store, &missing).await?;
            missing.retain(|id| !absent.contains(*id));
            if !missing.is_empty() {
                return Err(LixError::new(
                    "LIX_SYNC_BRANCH_CONTROLS_REQUIRED",
                    "branch coordinates are not resident in this partial replica",
                )
                .with_details(serde_json::json!({
                    "repositoryId": state.repository_id(),
                    "epochId": state.epoch_id(),
                    "branchIds": missing,
                })));
            }
        }
        branch_ids
            .into_iter()
            .zip(values)
            .map(|(branch_id, value)| match value {
                None => Ok(BranchHeadControlObservation {
                    control: None,
                    raw_token: None,
                }),
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    let control = decode_control(branch_id, &bytes)?;
                    Ok(BranchHeadControlObservation {
                        control: Some(control),
                        raw_token: Some(bytes),
                    })
                }
                Some(StorageProjectedValue::KeyOnly) => Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "branch-head control point read unexpectedly omitted its value",
                )),
            })
            .collect()
    }

    /// Returns every durable branch control in deterministic branch-id order.
    pub(crate) async fn scan(&self) -> Result<Vec<(String, BranchHeadControl)>, LixError> {
        if let Some((state, _)) = crate::sync::load_partial_replica_state(&self.store).await? {
            return Err(LixError::new(
                "LIX_SYNC_BRANCH_INVENTORY_REQUIRED",
                "complete branch inventory is not resident in this partial replica",
            )
            .with_details(serde_json::json!({
                "repositoryId": state.repository_id(),
                "epochId": state.epoch_id(),
            })));
        }
        let range = StoragePrefix {
            bytes: Bytes::new(),
        }
        .to_range()?;
        let mut rows = Vec::new();
        let mut cursor = self
            .store
            .begin_scan(
                BRANCH_HEAD_CONTROL_SPACE,
                range,
                StorageBeginScanOptions::default(),
            )
            .await?;
        loop {
            let (page, page_has_more) = cursor
                .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            for entry in page {
                let key = storage_codec::decode::<BranchHeadControlKey>(
                    "branch-head control key",
                    entry.key.0.as_ref(),
                )?;
                let control = decode_projected_value(&key.branch_id, entry.value)?;
                rows.push((key.branch_id, control));
            }
            if !page_has_more {
                break;
            }
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }
}

/// Storage factory for branch-head controls.
#[derive(Clone, Copy, Default)]
pub(crate) struct BranchHeadControlContext;

impl BranchHeadControlContext {
    pub(crate) fn new() -> Self {
        Self
    }

    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> BranchHeadControlReader<S>
    where
        S: StorageAdapterRead,
    {
        BranchHeadControlReader { store }
    }
}

pub(crate) fn stage_branch_head_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<(), LixError> {
    writes.put(
        BRANCH_HEAD_CONTROL_SPACE,
        StorageKey(Bytes::from(encode_key(branch_id)?)),
        StorageValue {
            bytes: Bytes::from(encode_control(branch_id, &control)?),
        },
    );
    Ok(())
}

pub(crate) fn stage_delete_branch_head_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
) -> Result<(), LixError> {
    writes.delete(
        BRANCH_HEAD_CONTROL_SPACE,
        StorageKey(Bytes::from(encode_key(branch_id)?)),
    );
    Ok(())
}

/// Converts an observed opaque value into the one backend-neutral publication
/// guard. A missing control is guarded as absent, so concurrent branch
/// creation cannot both succeed.
pub(crate) fn branch_head_control_precondition(
    branch_id: &str,
    expected: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    let key = StorageKey(Bytes::from(encode_key(branch_id)?));
    Ok(match expected {
        None => StoragePrecondition::KeyAbsent {
            space: BRANCH_HEAD_CONTROL_SPACE,
            key,
        },
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: BRANCH_HEAD_CONTROL_SPACE,
            key,
            expected,
        },
    })
}

fn encode_key(branch_id: &str) -> Result<Vec<u8>, LixError> {
    storage_codec::encode(
        "branch-head control key",
        &BranchHeadControlKeyRef { branch_id },
    )
}

fn encode_control(branch_id: &str, control: &BranchHeadControl) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode("branch-head control", control)?;
    let mut encoded = Vec::with_capacity(
        BRANCH_HEAD_CONTROL_MAGIC.len() + payload.len() + BRANCH_HEAD_CONTROL_DIGEST_BYTES,
    );
    encoded.extend_from_slice(BRANCH_HEAD_CONTROL_MAGIC);
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&control_digest(branch_id, &encoded));
    Ok(encoded)
}

fn decode_control(branch_id: &str, bytes: &[u8]) -> Result<BranchHeadControl, LixError> {
    let payload_end = bytes
        .len()
        .checked_sub(BRANCH_HEAD_CONTROL_DIGEST_BYTES)
        .filter(|payload_end| *payload_end >= BRANCH_HEAD_CONTROL_MAGIC.len())
        .ok_or_else(branch_head_control_corruption)?;
    let (authenticated, stored_digest) = bytes.split_at(payload_end);
    if !authenticated.starts_with(BRANCH_HEAD_CONTROL_MAGIC)
        || stored_digest != control_digest(branch_id, authenticated)
    {
        return Err(branch_head_control_corruption());
    }
    storage_codec::decode(
        "branch-head control",
        &authenticated[BRANCH_HEAD_CONTROL_MAGIC.len()..],
    )
}

// Keep this native-input operation behind an erased owner boundary: branch
// reads sit beneath public adapter futures whose consumers use default limits.
// StorageAdapterRead and its futures are Send on every engine target.
type DescriptorAbsenceFuture<'a> =
    futures_util::future::BoxFuture<'a, Result<std::collections::BTreeSet<String>, LixError>>;

fn absent_native_branch_descriptors<'a, S: StorageAdapterRead + ?Sized>(
    store: &'a S,
    missing: &'a [&'a String],
) -> DescriptorAbsenceFuture<'a> {
    Box::pin(async move {
        // Read the global control directly to avoid reentering branch lookup.
        let global_key = StorageKey(Bytes::from(encode_key(crate::GLOBAL_BRANCH_ID)?));
        let global = PointReadPlan::new(BRANCH_HEAD_CONTROL_SPACE, &[global_key])
            .materialize(store, StorageGetOptions::default())
            .await?
            .value;
        let Some(Some(StorageProjectedValue::FullValue(bytes))) = global.into_iter().next() else {
            return Ok(std::collections::BTreeSet::new());
        };
        let global = decode_control(crate::GLOBAL_BRANCH_ID, &bytes)?;
        let candidates = missing
            .iter()
            .filter_map(|id| {
                crate::row_pk::RowPk::uuid_from_canonical(id)
                    .ok()
                    .map(|row_pk| {
                        (
                            (*id).clone(),
                            crate::tracked_state::TrackedStateKey {
                                schema_key: crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY.to_owned(),
                                file_id: None,
                                row_pk,
                            },
                        )
                    })
            })
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(std::collections::BTreeSet::new());
        }
        let keys = candidates
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        // Missing native inputs propagate demand; only complete index facts
        // establish absence. Existing descriptors still require real controls.
        let facts = crate::tracked_state::TrackedStateContext::new()
            .reader(store)
            .index_values_at_commit(&global.head_commit_id.to_string(), &keys)
            .await?;
        Ok(candidates
            .into_iter()
            .zip(facts)
            .filter_map(|((id, _), fact)| fact.is_none_or(|fact| fact.deleted).then_some(id))
            .collect())
    })
}

fn control_digest(branch_id: &str, authenticated: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key(BRANCH_HEAD_CONTROL_DIGEST_CONTEXT);
    hasher.update(&(branch_id.len() as u64).to_be_bytes());
    hasher.update(branch_id.as_bytes());
    hasher.update(authenticated);
    *hasher.finalize().as_bytes()
}

fn branch_head_control_corruption() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "branch-head control authentication digest mismatch",
    )
}

fn decode_projected_value(
    branch_id: &str,
    value: StorageProjectedValue,
) -> Result<BranchHeadControl, LixError> {
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "branch-head control read unexpectedly omitted its value",
        ));
    };
    decode_control(branch_id, &bytes)
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    use super::*;

    #[tokio::test]
    async fn native_descriptor_absence_distinguishes_new_branch_from_unseen_existing_control() {
        let authority = crate::open_lix().await.unwrap();
        let existing = authority
            .create_branch(crate::CreateBranchOptions {
                id: None,
                name: "existing-unseen".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let state = crate::sync::PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.into(),
            "00000000-0000-7000-8000-000000000099".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let storage = authority.storage_adapter();
        let mut writes = storage.new_write_set();
        writes.put(
            crate::sync::PARTIAL_REPLICA_STATE_SPACE,
            crate::sync::partial_replica_state_key(),
            serde_json::to_vec(&state).unwrap(),
        );
        writes.delete(
            BRANCH_HEAD_CONTROL_SPACE,
            StorageKey(Bytes::from(encode_key(&existing.id).unwrap())),
        );
        storage
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let reader = BranchHeadControlContext::new().reader(&read);
        let fresh = "00000000-0000-7000-8000-000000000098";
        let observed = reader
            .load_observed(&[fresh.to_owned()])
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert!(observed.control.is_none());
        assert!(matches!(
            branch_head_control_precondition(fresh, observed.raw_token).unwrap(),
            StoragePrecondition::KeyAbsent { .. }
        ));
        assert_eq!(
            reader.load(&existing.id).await.unwrap_err().code,
            "LIX_SYNC_BRANCH_CONTROLS_REQUIRED"
        );
        drop(read);
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn partial_missing_control_without_native_absence_proof_stays_unknown() {
        let authority = crate::open_lix().await.unwrap();
        let state = crate::sync::PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            crate::ANONYMOUS_ACCOUNT_ID.into(),
            "00000000-0000-7000-8000-000000000099".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        writes.put(
            crate::sync::PARTIAL_REPLICA_STATE_SPACE,
            crate::sync::partial_replica_state_key(),
            serde_json::to_vec(&state).unwrap(),
        );
        storage
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let reader = BranchHeadControlContext::new().reader(&read);
        let fresh = "00000000-0000-7000-8000-000000000098".to_string();
        let unknown = "00000000-0000-7000-8000-000000000097".to_string();
        assert_eq!(
            reader.load(&fresh).await.unwrap_err().code,
            "LIX_SYNC_BRANCH_CONTROLS_REQUIRED"
        );
        assert_eq!(
            reader
                .load_observed(&[fresh, unknown])
                .await
                .unwrap_err()
                .code,
            "LIX_SYNC_BRANCH_CONTROLS_REQUIRED"
        );
        authority.close().await.unwrap();
    }

    #[tokio::test]
    async fn point_reads_scans_and_exact_byte_cas_controls() {
        let storage = StorageAdapter::new(Memory::new());
        let first = BranchHeadControl {
            head_commit_id: CommitId::for_test_label("first-head"),
            tracked_generation: CommitId::for_test_label("first-generation"),
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: LixTimestamp::expect_parse("first created_at", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("first updated_at", "2026-01-01T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("first-ref-change"),
        };
        let second = BranchHeadControl {
            head_commit_id: CommitId::for_test_label("second-head"),
            tracked_generation: CommitId::for_test_label("first-generation"),
            current_state_revision: 1,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: first.created_at,
            updated_at: LixTimestamp::expect_parse("second updated_at", "2026-01-02T00:00:00Z"),
            ref_change_id: ChangeId::for_test_label("second-ref-change"),
        };
        let branch_a = "01920000-0000-7000-8000-0000000000a1".to_string();
        let branch_b = "01920000-0000-7000-8000-0000000000b1".to_string();

        let mut presence = first;
        presence.schema_presence_bloom = [0; 4];
        assert!(!presence.may_have_schema("present"));
        presence.note_schema("present");
        assert!(presence.may_have_schema("present"));
        assert!(!presence.may_have_schema("absent"));
        presence.reset_schema_presence();
        assert!(!presence.may_have_schema("present"));

        let mut writes = storage.new_write_set();
        stage_branch_head_control(&mut writes, &branch_b, first)
            .expect("branch b control should stage");
        stage_branch_head_control(&mut writes, &branch_a, first)
            .expect("branch a control should stage");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("controls should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let reader = BranchHeadControlContext::new().reader(read);
        assert_eq!(
            reader
                .load_many(&[branch_b.clone(), branch_a.clone(), branch_a.clone()])
                .await
                .expect("point reads should load"),
            vec![Some(first), Some(first), Some(first)]
        );
        assert_eq!(
            reader.scan().await.expect("scan should load"),
            vec![(branch_a.clone(), first), (branch_b, first)]
        );
        let token = reader
            .load_observed(std::slice::from_ref(&branch_a))
            .await
            .expect("control observation should load")
            .pop()
            .and_then(|observation| observation.raw_token)
            .expect("stored control should have token");

        let mut winner = storage.new_write_set();
        stage_branch_head_control(&mut winner, &branch_a, second).expect("winner should stage");
        storage
            .commit_write_set(
                winner,
                StorageWriteOptions {
                    preconditions: vec![
                        branch_head_control_precondition(&branch_a, Some(token.clone()))
                            .expect("winner guard should encode"),
                    ],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("winner should commit");

        let mut stale = storage.new_write_set();
        stage_branch_head_control(&mut stale, &branch_a, first).expect("stale write should stage");
        let error = storage
            .commit_write_set(
                stale,
                StorageWriteOptions {
                    preconditions: vec![
                        branch_head_control_precondition(&branch_a, Some(token))
                            .expect("stale guard should encode"),
                    ],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("stale publish must fail");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_)
            )
        ));

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corruption read should open");
        let mut corrupted = BranchHeadControlContext::new()
            .reader(read)
            .load_observed(std::slice::from_ref(&branch_a))
            .await
            .expect("winner control should load")
            .pop()
            .and_then(|observation| observation.raw_token)
            .expect("winner control should have authenticated bytes")
            .to_vec();
        let payload_byte = BRANCH_HEAD_CONTROL_MAGIC.len();
        corrupted[payload_byte] ^= 1;
        let mut writes = storage.new_write_set();
        writes.put(
            BRANCH_HEAD_CONTROL_SPACE,
            StorageKey(Bytes::from(encode_key(&branch_a).unwrap())),
            StorageValue {
                bytes: Bytes::from(corrupted),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("mutable control corruption fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt control read should open");
        let error = BranchHeadControlContext::new()
            .reader(read)
            .load(&branch_a)
            .await
            .expect_err("corrupt branch control must fail closed");
        assert!(error.to_string().contains("authentication digest mismatch"));
    }
}
