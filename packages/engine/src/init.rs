#![allow(clippy::clone_on_copy, clippy::unnecessary_wraps)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{
    BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY, BranchHeadControl,
    stage_branch_head_control,
};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitId,
    CommitRecord,
};
use crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::functions::FunctionProviderHandle;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::live_state::{CurrentStateDeltaRef, TrackedHeadContext};
use crate::schema::{
    registered_schema_entity_pk, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{PointReadPlan, SharedStorageAdapterRead, StorageAdapterRead};
use crate::storage_adapter::{
    StorageAdapter, StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpace,
    StorageSpaceId, StorageWriteSet,
};
use crate::tracked_state::{
    ArrowStateInputRowRef, CommitDeltaReplacementScope, CommitStateManifest,
    TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef,
    TrackedStateIndexValueRef, TrackedStateKeyRef, encode_authoritative_arrow_state_rows,
    encode_key_ref, stage_commit_deltas_for_commit_state,
};
use bytes::Bytes;
use serde_json::json;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Repository-wide compatibility gate for physical storage protocols.
///
/// V50 is the Arrow-native state-tree hard cut. Commits publish one
/// content-addressed catalog of canonical Arrow leaves; compact event storage
/// carries authored identity and coordinates only. Older repositories fail
/// closed instead of invoking a legacy reader, replay, or migration path.
pub(crate) const REPOSITORY_PROTOCOL_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0004_0011), "repository.protocol.v1");
pub(crate) const REPOSITORY_PROTOCOL_KEY: &[u8] = b"current";
const REPOSITORY_PROTOCOL_VALUE: &[u8] = b"sparse-current-state-parts.v50";

/// Raw status of the repository protocol marker. Engine opening consults this
/// before it touches any tracked-head space, whose physical IDs deliberately
/// remain stable across hard layout cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RepositoryProtocolStatus {
    /// The current layout has one authoritative current-state plane.
    Current,
    Missing,
    Unsupported,
}

pub(crate) fn stage_repository_protocol(writes: &mut StorageWriteSet) {
    writes.put(
        REPOSITORY_PROTOCOL_SPACE,
        REPOSITORY_PROTOCOL_KEY,
        REPOSITORY_PROTOCOL_VALUE,
    );
}

pub(crate) async fn repository_protocol_status(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<RepositoryProtocolStatus, LixError> {
    let values = PointReadPlan::new(
        REPOSITORY_PROTOCOL_SPACE,
        &[StorageKey(Bytes::from_static(REPOSITORY_PROTOCOL_KEY))],
    )
    .materialize(read, StorageGetOptions::default())
    .await?;
    Ok(match values.value.into_iter().next().flatten() {
        Some(StorageProjectedValue::FullValue(value))
            if value.as_ref() == REPOSITORY_PROTOCOL_VALUE =>
        {
            RepositoryProtocolStatus::Current
        }
        Some(_) => RepositoryProtocolStatus::Unsupported,
        None => RepositoryProtocolStatus::Missing,
    })
}

pub(crate) fn unsupported_repository_protocol_error() -> LixError {
    LixError::new(
        "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT",
        "repository uses an unsupported storage protocol; recreate the repository",
    )
}

/// Pure seed plan for initializing an engine repository.
///
/// Tracked bootstrap facts go to the changelog. Moving heads are seeded in
/// the direct current-state control plane and retain a standalone immutable
/// branch-ref ledger change; ordinary untracked data shares the same current
/// state generation without creating a changelog fact.
pub(crate) struct InitSeedPlan {
    commit: InitSeedCommit,
    changes: Vec<InitSeedChange>,
    branch_controls: Vec<InitBranchHeadControl>,
    untracked_rows: Vec<InitSeedLiveRow>,
    pub(crate) receipt: InitReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedCommit {
    id: CommitId,
    change_id: ChangeId,
    parent_ids: Vec<CommitId>,
    account_id: String,
    created_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedChange {
    id: ChangeId,
    entity_pk: EntityPk,
    schema_key: String,
    snapshot_content: String,
    created_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedLiveRow {
    id: ChangeId,
    entity_pk: EntityPk,
    schema_key: String,
    snapshot_content: String,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    branch_id: String,
}

/// Initial direct branch controls are planned with the seed so their public
/// metadata is deterministic and independent of the flat live-state lane.
#[derive(Debug, Clone, PartialEq, Eq)]
struct InitBranchHeadControl {
    branch_id: String,
    control: BranchHeadControl,
    /// Public `lix_change` fact for the control's initial branch-ref
    /// publication. This deliberately has no flat current-state row.
    branch_ref_change: InitSeedLiveRow,
}

/// Values generated while planning the initial repository seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReceipt {
    pub lix_id: String,
    pub global_branch_id: String,
    pub main_branch_id: String,
    pub initial_commit_id: String,
}

/// Builds the canonical bootstrap changes for a new engine repository.
///
/// The initial commit tracks durable content rows. Branch heads are moving
/// pointers and therefore live in direct control records instead of the
/// changelog or flat current state.
pub(crate) fn plan_init_seed(functions: FunctionProviderHandle) -> Result<InitSeedPlan, LixError> {
    let main_branch_id = functions.call_uuid_v7().to_string();
    let lix_id = functions.call_uuid_v7().to_string();
    let initial_commit_id = CommitId::from(functions.call_uuid_v7());
    let timestamp = functions.call_timestamp();

    let mut registered_schema_changes = Vec::new();
    for schema in seed_schema_definitions() {
        let key = schema_key_from_definition(schema)?;
        registered_schema_changes.push(canonical_change(
            functions.call_uuid_v7(),
            registered_schema_entity_pk(&key.schema_key)?,
            REGISTERED_SCHEMA_KEY,
            registered_schema_snapshot(schema)?,
            timestamp,
        ));
    }

    let global_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::uuid_from_canonical(GLOBAL_BRANCH_ID)
            .expect("global branch sentinel is a canonical UUID"),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(GLOBAL_BRANCH_ID, "global", true)?,
        timestamp,
    );
    let main_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::uuid_from_canonical(&main_branch_id)
            .expect("generated main branch ID is a canonical UUID"),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(&main_branch_id, "main", false)?,
        timestamp,
    );
    let kv_lix_id_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::single(LIX_ID_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(LIX_ID_KEY, &lix_id)?,
        timestamp,
    );
    let initial_checkpoint_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::uuid_from_canonical(&main_branch_id)
            .expect("generated main branch ID is a canonical UUID"),
        CHECKPOINT_MARKER_SCHEMA_KEY,
        checkpoint_marker_snapshot(&main_branch_id)?,
        timestamp,
    );

    let initial_commit = InitSeedCommit {
        id: initial_commit_id,
        change_id: ChangeId::from(functions.call_uuid_v7()),
        parent_ids: Vec::new(),
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        created_at: timestamp,
    };
    // Keep one distinct public ref change id per initial branch, matching the
    // old `lix_branch_ref` current rows without materializing those rows in
    // the ordinary current-state generation.
    let global_branch_ref_change = branch_ref_ledger_change(
        functions.call_uuid_v7(),
        GLOBAL_BRANCH_ID,
        initial_commit_id,
        timestamp,
    )?;
    let global_branch_control = InitBranchHeadControl {
        branch_id: GLOBAL_BRANCH_ID.to_string(),
        control: BranchHeadControl {
            head_commit_id: initial_commit_id,
            generation: initial_commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(initial_commit_id),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: global_branch_ref_change.id,
            schema_presence_bloom: [0; 4],
        },
        branch_ref_change: global_branch_ref_change,
    };
    let main_branch_ref_change = branch_ref_ledger_change(
        functions.call_uuid_v7(),
        &main_branch_id,
        initial_commit_id,
        timestamp,
    )?;
    let main_branch_control = InitBranchHeadControl {
        branch_id: main_branch_id.clone(),
        control: BranchHeadControl {
            head_commit_id: initial_commit_id,
            generation: initial_commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(initial_commit_id),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: main_branch_ref_change.id,
            schema_presence_bloom: [0; 4],
        },
        branch_ref_change: main_branch_ref_change,
    };
    let workspace_branch_row = untracked_row(
        functions.call_uuid_v7(),
        EntityPk::single(WORKSPACE_BRANCH_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(WORKSPACE_BRANCH_KEY, &main_branch_id)?,
        timestamp,
    );

    Ok(InitSeedPlan {
        commit: initial_commit,
        changes: registered_schema_changes
            .into_iter()
            .chain([
                global_branch_descriptor_change,
                main_branch_descriptor_change,
                kv_lix_id_change,
                initial_checkpoint_change,
            ])
            .collect(),
        branch_controls: vec![global_branch_control, main_branch_control],
        untracked_rows: vec![workspace_branch_row],
        receipt: InitReceipt {
            lix_id,
            global_branch_id: GLOBAL_BRANCH_ID.to_string(),
            main_branch_id,
            initial_commit_id: initial_commit_id.to_string(),
        },
    })
}

/// Initializes an empty engine repository in one storage transaction.
///
/// The pure seed planner decides which bootstrap facts exist. This function is
/// only responsible for durably writing those facts to their owning stores:
/// changelog for tracked changes, and live_state for the serving state
/// plus untracked moving refs.
pub(crate) async fn initialize<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    _tracked_state: &TrackedStateContext,
) -> Result<InitReceipt, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut read = SharedStorageAdapterRead::new(
        storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await?,
    );
    assert_empty_repository_for_initialize::<StorageImpl>(&read).await?;

    let functions = FunctionProviderHandle::system();
    let plan = plan_init_seed(functions)?;
    let receipt = plan.receipt.clone();
    let mut writes = StorageWriteSet::new();
    let authored_changes = plan
        .changes
        .iter()
        .map(seed_change_to_change_record)
        .collect::<Vec<_>>();
    let branch_ref_ledger_changes = plan
        .branch_controls
        .iter()
        .map(|branch| seed_untracked_change_to_change_record(&branch.branch_ref_change))
        .collect::<Vec<_>>();

    stage_init_json_payloads(&mut writes, &plan)?;
    stage_init_changelog_commit(
        &mut read,
        &mut writes,
        &plan,
        branch_ref_ledger_changes.clone(),
    )
    .await?;

    {
        let root_deltas = authored_changes
            .iter()
            .map(|change| TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: change.change_id,
                commit_id: plan.commit.id,
                deleted: change.snapshot.is_none(),
                created_at: change.created_at,
                updated_at: change.created_at,
            })
            .collect::<Vec<_>>();
        let commit_deltas = authored_changes
            .iter()
            .zip(root_deltas.iter().copied())
            .map(|(change, delta)| TrackedStateCommitDeltaRef {
                delta,
                snapshot: change.snapshot.as_ref_slot(),
                metadata: change.metadata.as_ref_slot(),
                origin_key: change.origin_key.as_deref(),
                base_coordinate: None,
                authored: true,
            })
            .collect::<Vec<_>>();
        let staged_delta = stage_commit_deltas_for_commit_state(&mut writes, &commit_deltas)?;
        crate::tracked_state::stage_change_locators(&mut writes, &staged_delta.locators);
        let mut mutations = staged_delta.mutation_inventory().clone();
        let mut planned_members = crate::tracked_state::staged_commit_delta_members_for_write(
            &read,
            &writes,
            plan.commit.id,
            &mutations,
        )
        .await?;
        let mut arrow_mutations = crate::live_state::EntityColumnarWriteSets::new();
        let mut changes_by_scope =
            std::collections::BTreeMap::<CommitDeltaReplacementScope, Vec<&ChangeRecord>>::new();
        for change in &authored_changes {
            changes_by_scope
                .entry(CommitDeltaReplacementScope {
                    schema_key: change.schema_key.clone(),
                    file_id: change.file_id.clone(),
                })
                .or_default()
                .push(change);
        }
        for (scope, changes) in &mut changes_by_scope {
            changes.sort_unstable_by(|left, right| left.entity_pk.cmp(&right.entity_pk));
            let encoded_keys = changes
                .iter()
                .map(|change| {
                    encode_key_ref(TrackedStateKeyRef {
                        schema_key: &change.schema_key,
                        file_id: change.file_id.as_deref(),
                        entity_pk: &change.entity_pk,
                    })
                })
                .collect::<Vec<_>>();
            let rows = changes
                .iter()
                .zip(&encoded_keys)
                .map(|(change, encoded_key)| ArrowStateInputRowRef {
                    encoded_key,
                    value: TrackedStateIndexValueRef {
                        change_id: change.change_id,
                        commit_id: plan.commit.id,
                        deleted: change.snapshot.is_none(),
                        created_at: change.created_at,
                        updated_at: change.created_at,
                    },
                    snapshot: change.snapshot.as_ref_slot(),
                    metadata: change.metadata.as_ref_slot(),
                })
                .collect::<Vec<_>>();
            let (row_group_set, _) = encode_authoritative_arrow_state_rows(scope, &rows)?;
            arrow_mutations.insert_scope(plan.commit.id, scope.clone(), row_group_set);
        }
        let catalog_publication =
            crate::tracked_state::stage_current_state_catalog_from_published_parent(
                &read,
                &mut writes,
                None,
                plan.commit.id,
                &mutations,
                &planned_members,
                Some(&arrow_mutations),
            )
            .await?;
        for member in planned_members.iter_mut().filter(|member| member.authored) {
            let encoded_key = encode_key_ref(TrackedStateKeyRef {
                schema_key: &member.key.schema_key,
                file_id: member.key.file_id.as_deref(),
                entity_pk: &member.key.entity_pk,
            });
            if let Some(coordinate) = catalog_publication.coordinates().get(&encoded_key) {
                member.base_coordinate = Some(*coordinate);
            }
            if !member.value.deleted && member.base_coordinate.is_none() {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "repository initialization produced a live event without an Arrow coordinate",
                ));
            }
        }
        mutations = crate::tracked_state::finalize_commit_delta_event_coordinates(
            &mut writes,
            plan.commit.id,
            &mutations,
            &planned_members,
        )?;
        let manifest = CommitStateManifest {
            commit_id: plan.commit.id,
            generation: 0,
            parent_commit_ids: plan.commit.parent_ids.clone(),
            state_parent_commit_id: None,
            commit_change_id: plan.commit.change_id,
            account_id: plan.commit.account_id.clone(),
            created_at: plan.commit.created_at,
            mutations,
            current_state_catalog: catalog_publication.root(),
        };
        crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
            &mut writes,
            &manifest,
            &catalog_publication,
        )?;
        // Publish the shared immutable Arrow root for both visible branches.
        // Only history-free global workspace rows enter mutable HOT storage.
        let tracked_head = TrackedHeadContext::new();
        for branch in &plan.branch_controls {
            let untracked_deltas = if branch.branch_id == GLOBAL_BRANCH_ID {
                plan.untracked_rows
                    .iter()
                    .map(|row| CurrentStateDeltaRef {
                        schema_key: &row.schema_key,
                        file_id: None,
                        entity_pk: &row.entity_pk,
                        change_id: None,
                        commit_id: None,
                        untracked: true,
                        deleted: false,
                        created_at: row.created_at,
                        updated_at: row.updated_at,
                        snapshot: crate::json_store::JsonSlotRef::Inline(&row.snapshot_content),
                        metadata: crate::json_store::JsonSlotRef::None,
                        columnar_base_coordinate: None,
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if !untracked_deltas.is_empty() {
                tracked_head
                    .writer(&read, &mut writes)
                    .stage_complete_current_state(
                        &branch.branch_id,
                        plan.commit.id,
                        crate::live_state::HotTrackedSnapshot::default(),
                        None,
                        &[],
                        &untracked_deltas,
                        &std::collections::BTreeSet::new(),
                    )
                    .await?;
            }
            tracked_head
                .writer(&read, &mut writes)
                .stage_root_current_base(&branch.branch_id, plan.commit.id, plan.commit.id);
            let mut control = branch.control;
            control.schema_presence_bloom = [u64::MAX; 4];
            control.note_schemas(untracked_deltas.iter().map(|delta| delta.schema_key));
            stage_branch_head_control(&mut writes, &branch.branch_id, control)?;
        }
    }
    crate::catalog::stage_catalog_revision(&mut writes);
    stage_repository_protocol(&mut writes);

    storage
        .commit_write_set(
            writes,
            crate::storage_adapter::StorageWriteOptions::default(),
        )
        .await?;
    Ok(receipt)
}

/// Initialization is a create operation, never a migration. The direct-head
/// spaces intentionally retain their physical IDs across protocol cuts, so
/// writing a new protocol marker over existing bytes would make old values
/// look like current layout state.
async fn assert_empty_repository_for_initialize<StorageImpl>(
    read: &SharedStorageAdapterRead<StorageImpl::Read<'_>>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    match repository_protocol_status(read).await? {
        RepositoryProtocolStatus::Current => Err(LixError::new(
            "LIX_ERROR_ALREADY_INITIALIZED",
            "engine storage is already initialized; initialization does not migrate or overwrite repositories",
        )),
        RepositoryProtocolStatus::Unsupported => Err(unsupported_repository_protocol_error()),
        RepositoryProtocolStatus::Missing => {
            if StorageAdapter::<StorageImpl>::load_mutation_revision_from_read(read)
                .await?
                .is_some()
            {
                Err(unsupported_repository_protocol_error())
            } else {
                Ok(())
            }
        }
    }
}

fn seed_change_to_change_record(change: &InitSeedChange) -> ChangeRecord {
    ChangeRecord {
        format_version: 1,
        change_id: change.id,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        entity_pk: change.entity_pk.clone(),
        schema_key: change.schema_key.clone(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&change.snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: change.created_at,
        origin_key: None,
    }
}

fn seed_untracked_change_to_change_record(row: &InitSeedLiveRow) -> ChangeRecord {
    ChangeRecord {
        format_version: 2,
        change_id: row.id,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&row.snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: row.updated_at,
        origin_key: None,
    }
}

fn stage_init_json_payloads(
    writes: &mut StorageWriteSet,
    plan: &InitSeedPlan,
) -> Result<(), LixError> {
    // Only payloads above the inline threshold need store rows; inline
    // Payloads live in their change records. Commit rows are derived directly
    // from changelog.commit and never enter the tracked-state tree.
    JsonStoreContext::new().writer().stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        plan.changes
            .iter()
            .map(|change| change.snapshot_content.as_str())
            .chain(
                plan.untracked_rows
                    .iter()
                    .map(|row| row.snapshot_content.as_str()),
            )
            .chain(
                plan.branch_controls
                    .iter()
                    .map(|branch| branch.branch_ref_change.snapshot_content.as_str()),
            )
            .filter(|snapshot| snapshot.len() > crate::json_store::JSON_INLINE_MAX_BYTES)
            .map(NormalizedJsonRef::new),
    )?;
    Ok(())
}

async fn stage_init_changelog_commit(
    read: &mut impl StorageAdapterRead,
    writes: &mut StorageWriteSet,
    plan: &InitSeedPlan,
    changes: Vec<ChangeRecord>,
) -> Result<(), LixError> {
    let commit = CommitRecord {
        format_version: 1,
        commit_id: plan.commit.id,
        generation: 0,
        parent_commit_ids: plan.commit.parent_ids.clone(),
        change_id: plan.commit.change_id,
        account_id: plan.commit.account_id.clone(),
        created_at: plan.commit.created_at,
    };
    let mut writer = ChangelogContext::new().writer(read, writes);
    writer
        .stage_append(ChangelogAppend {
            commits: vec![commit],
            changes,
        })
        .await
}

fn untracked_row(
    id: uuid::Uuid,
    entity_pk: EntityPk,
    schema_key: &str,
    snapshot_content: String,
    timestamp: LixTimestamp,
) -> InitSeedLiveRow {
    InitSeedLiveRow {
        id: ChangeId::from(id),
        entity_pk,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at: timestamp,
        updated_at: timestamp,
        global: true,
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}

/// The direct control owns a branch ref's current visibility, while this
/// standalone fact preserves the public immutable `lix_change` ledger row.
/// It is not a mutable current-state member.
fn branch_ref_ledger_change(
    id: uuid::Uuid,
    branch_id: &str,
    commit_id: CommitId,
    timestamp: LixTimestamp,
) -> Result<InitSeedLiveRow, LixError> {
    Ok(InitSeedLiveRow {
        id: ChangeId::from(id),
        entity_pk: EntityPk::uuid_from_canonical(branch_id)
            .expect("seed branch IDs are canonical UUIDs"),
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        snapshot_content: branch_ref_snapshot(branch_id, commit_id)?,
        created_at: timestamp,
        updated_at: timestamp,
        global: branch_id == GLOBAL_BRANCH_ID,
        branch_id: branch_id.to_string(),
    })
}

fn canonical_change(
    id: uuid::Uuid,
    entity_pk: EntityPk,
    schema_key: &str,
    snapshot_content: String,
    created_at: LixTimestamp,
) -> InitSeedChange {
    InitSeedChange {
        id: ChangeId::from(id),
        entity_pk,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at,
    }
}

fn branch_descriptor_snapshot(id: &str, name: &str, hidden: bool) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": id,
        "name": name,
        "hidden": hidden,
    }))
}

fn branch_ref_snapshot(branch_id: &str, commit_id: CommitId) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": branch_id,
        "commit_id": commit_id.to_string(),
    }))
}

fn key_value_snapshot(key: &str, value: &str) -> Result<String, LixError> {
    encode_snapshot(json!({
        "key": key,
        "value": value,
    }))
}

fn checkpoint_marker_snapshot(branch_id: &str) -> Result<String, LixError> {
    encode_snapshot(json!({
        "branch_id": branch_id,
    }))
}

fn registered_schema_snapshot(schema: &serde_json::Value) -> Result<String, LixError> {
    encode_snapshot(json!({
        "value": schema,
    }))
}

fn encode_snapshot(value: serde_json::Value) -> Result<String, LixError> {
    serde_json::to_string(&value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine init seed snapshot serialization failed: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::changelog::ChangelogReader;
    use crate::functions::FunctionProvider;
    use crate::storage_adapter::Memory;
    use crate::storage_adapter::StorageAdapter;
    use crate::tracked_state::TrackedStateContext;

    #[test]
    fn plan_init_seed_returns_tracked_changes_and_untracked_workspace_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.changes.len(), seed_schema_definitions().len() + 4);
        assert_eq!(plan.untracked_rows.len(), 1);
        assert_eq!(plan.receipt.global_branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(plan.receipt.main_branch_id, test_uuid(1));
        assert_eq!(plan.receipt.lix_id, test_uuid(2));
        assert_eq!(plan.receipt.initial_commit_id, test_uuid(3));
    }

    #[test]
    fn plan_init_seed_commit_header_tracks_schema_registrations_descriptor_and_lix_id_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.commit.id, plan.receipt.initial_commit_id);
        assert_eq!(
            plan.commit.change_id.to_string(),
            test_uuid(seed_schema_definitions().len() + 8)
        );
        assert!(plan.commit.parent_ids.is_empty());
        assert_eq!(plan.commit.account_id, crate::ANONYMOUS_ACCOUNT_ID);
        assert_eq!(
            plan.commit.created_at.to_string(),
            "2026-01-01T00:00:00.001Z"
        );

        let change_ids = plan
            .changes
            .iter()
            .map(|change| change.id.to_string())
            .collect::<Vec<_>>();
        assert_eq!(change_ids.len(), seed_schema_definitions().len() + 4);
        let first_seed_change_id = test_uuid(4);
        assert!(change_ids.contains(&first_seed_change_id));
        assert!(!change_ids.contains(&plan.commit.change_id.to_string()));

        let registered_schema_change_ids = plan
            .changes
            .iter()
            .filter(|change| change.schema_key == REGISTERED_SCHEMA_KEY)
            .map(|change| change.id.to_string())
            .collect::<Vec<_>>();
        for change_id in registered_schema_change_ids {
            assert!(change_ids.contains(&change_id));
        }
    }

    #[test]
    fn plan_init_seed_registers_seed_schemas_as_initial_commit_rows() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let registered_schema_changes = plan
            .changes
            .iter()
            .filter(|change| change.schema_key == REGISTERED_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(
            registered_schema_changes.len(),
            seed_schema_definitions().len()
        );
        assert!(registered_schema_changes.iter().any(|change| {
            snapshot(change)
                .pointer("/value/x-lix-key")
                .and_then(JsonValue::as_str)
                == Some(REGISTERED_SCHEMA_KEY)
        }));
        assert!(registered_schema_changes.iter().any(|change| {
            snapshot(change)
                .pointer("/value/x-lix-key")
                .and_then(JsonValue::as_str)
                == Some(KEY_VALUE_SCHEMA_KEY)
        }));
    }

    #[test]
    fn plan_init_seed_keeps_branch_heads_out_of_untracked_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        assert_eq!(plan.untracked_rows.len(), 1);
        assert!(
            plan.untracked_rows
                .iter()
                .all(|row| row.schema_key != "lix_branch_ref")
        );
        assert!(
            plan.changes
                .iter()
                .all(|change| change.schema_key != "lix_branch_ref")
        );
        assert_eq!(plan.branch_controls.len(), 2);
        for branch in &plan.branch_controls {
            assert_eq!(branch.branch_ref_change.schema_key, BRANCH_REF_SCHEMA_KEY);
            assert_eq!(branch.control.ref_change_id, branch.branch_ref_change.id);
            let snapshot = untracked_snapshot(&branch.branch_ref_change);
            assert_eq!(
                snapshot.get("id").and_then(JsonValue::as_str),
                Some(branch.branch_id.as_str())
            );
            assert_eq!(
                snapshot.get("commit_id").and_then(JsonValue::as_str),
                Some(plan.receipt.initial_commit_id.as_str())
            );
        }
    }

    #[test]
    fn plan_init_seed_workspace_branch_points_to_main_branch() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let workspace_row = plan
            .untracked_rows
            .iter()
            .find(|row| {
                row.schema_key == KEY_VALUE_SCHEMA_KEY
                    && row.entity_pk == EntityPk::single(WORKSPACE_BRANCH_KEY)
            })
            .expect("workspace branch row should exist");

        assert_eq!(workspace_row.branch_id, GLOBAL_BRANCH_ID);
        assert!(workspace_row.global);
        let snapshot = untracked_snapshot(workspace_row);
        assert_eq!(
            snapshot.get("key").and_then(JsonValue::as_str),
            Some(WORKSPACE_BRANCH_KEY)
        );
        assert_eq!(
            snapshot.get("value").and_then(JsonValue::as_str),
            Some(plan.receipt.main_branch_id.as_str())
        );
    }

    #[tokio::test]
    async fn initialize_writes_initial_commit_through_changelog() {
        let storage = Memory::new();
        let storage = StorageAdapter::new(storage);
        let tracked_state = TrackedStateContext::new();
        let receipt = initialize(storage.clone(), &tracked_state)
            .await
            .expect("engine should initialize");
        let mut reader = ChangelogContext::new().reader(
            storage
                .begin_read(crate::storage_adapter::StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commit_ids = [CommitId::for_test_label(&receipt.initial_commit_id)];
        let commits = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("commit should load");
        let Some(record) = commits.into_iter().next().and_then(|(_, value)| value) else {
            panic!("initial commit should exist");
        };

        assert_eq!(record.commit_id, receipt.initial_commit_id);
        let commit_change_id = record.change_id.clone();
        let membership_read = storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("membership read should open");
        let change_refs =
            crate::tracked_state::load_commit_delta_change_ids(&membership_read, record.commit_id)
                .await
                .expect("initial commit membership should load");
        assert_eq!(change_refs.len(), seed_schema_definitions().len() + 4);
        assert!(
            !change_refs.contains(&record.change_id),
            "initial commit row is derived from changelog.commit, not stored in its packed delta"
        );

        let sampled_change_id = change_refs
            .first()
            .copied()
            .expect("initial commit should reference at least one change");
        let packed_members = crate::tracked_state::load_commit_delta_members_with_payloads(
            &membership_read,
            record.commit_id,
        )
        .await
        .expect("packed initial commit payloads should load");
        assert!(
            packed_members
                .iter()
                .any(|member| member.change.change_id == sampled_change_id),
            "initial tracked changes are authoritative in the packed commit delta"
        );
        let change_ids = [sampled_change_id];
        let changes = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &change_ids,
            })
            .await
            .expect("standalone change index should load");
        assert!(
            changes.iter().all(|(_, value)| value.is_none()),
            "packed tracked changes must not be duplicated in the standalone change space"
        );
        let derivable_change_ids = [commit_change_id];
        let missing_derivable = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &derivable_change_ids,
            })
            .await
            .expect("derivable change lookup should load");
        assert!(missing_derivable.iter().all(|(_, value)| value.is_none()));
        let mut tracked_reader = tracked_state.reader(
            storage
                .begin_read(crate::storage_adapter::StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let rows = tracked_reader
            .scan_batch_at_commit(
                &receipt.initial_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["lix_commit".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("tracked initial root should scan")
            .into_rows();
        assert!(
            rows.is_empty(),
            "initial commit rows are derived from changelog.commit, not stored in tracked roots"
        );
    }

    #[tokio::test]
    async fn repository_protocol_rejects_pre_columnar_policy_cut_marker() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_PROTOCOL_SPACE,
            REPOSITORY_PROTOCOL_KEY,
            &b"lxcd9-generation-indexed-commits.v45"[..],
        );
        storage
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions::default(),
            )
            .await
            .expect("old protocol marker should stage");
        let read = storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("protocol read should open");

        assert_eq!(
            repository_protocol_status(&read)
                .await
                .expect("protocol status should load"),
            RepositoryProtocolStatus::Unsupported
        );
    }

    fn snapshot(change: &InitSeedChange) -> JsonValue {
        serde_json::from_str(&change.snapshot_content).expect("snapshot should be JSON")
    }

    fn untracked_snapshot(row: &InitSeedLiveRow) -> JsonValue {
        serde_json::from_str(&row.snapshot_content).expect("snapshot should be JSON")
    }

    #[expect(trivial_casts)]
    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::shared(
            Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> uuid::Uuid {
            self.uuid_count += 1;
            test_uuid_value(self.uuid_count)
        }

        fn timestamp(&mut self) -> LixTimestamp {
            self.timestamp_count += 1;
            LixTimestamp::expect_parse(
                "timestamp",
                &format!("2026-01-01T00:00:00.{:03}Z", self.timestamp_count),
            )
        }
    }

    fn test_uuid(index: usize) -> String {
        test_uuid_value(index).to_string()
    }

    fn test_uuid_value(index: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000 + index as u128)
    }
}
