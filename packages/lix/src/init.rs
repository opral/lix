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
use crate::checkpoint::{CHECKPOINT_SCHEMA_KEY, checkpoint_snapshot};
use crate::common::LixTimestamp;
use crate::functions::FunctionProviderHandle;
use crate::hot_state::{
    CurrentStateDeltaRef, TrackedHeadContext, TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage,
    stage_tracked_working_diff_epoch,
};
use crate::row_pk::RowPk;
use crate::schema::{
    registered_schema_row_pk, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{PointReadPlan, SharedStorageAdapterRead, StorageAdapterRead};
use crate::storage_adapter::{
    StorageAdapter, StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpace,
    StorageSpaceId, StorageWriteSet, ValueSemantics,
};
use crate::tracked_state::{
    CommitStateManifest, CommitStateMutationInventory, CommitStateReplayDebt,
    TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef,
    stage_commit_deltas_for_commit_state,
};
use bytes::Bytes;
use serde_json::json;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
pub(crate) const LIX_ID_KEY: &str = "lix_id";
pub(crate) const DEFAULT_BRANCH_KEY: &str = "lix_default_branch_id";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const ACCOUNT_SCHEMA_KEY: &str = "lix_account";

/// Repository-wide compatibility gate for physical storage protocols.
///
/// V66 makes the repository default branch a tracked bootstrap fact and
/// removes the mutable repository-branch selector. The hard cut rejects older
/// repositories instead of inferring or migrating the removed selector.
pub(crate) const REPOSITORY_PROTOCOL_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0011),
    "repository.protocol.v1",
    ValueSemantics::Mutable,
);
pub(crate) const REPOSITORY_PROTOCOL_KEY: &[u8] = b"current";
/// v67 adds `CommitRecord::touched_scope_digest`.
///
/// This bump is **not** cosmetic. `CommitRecord` is `#[musli(packed)]`, so a
/// v66 record cannot be decoded by a v67 reader — the failure would otherwise
/// surface deep inside graph traversal as an opaque codec error instead of at
/// open, where `unsupported_repository_protocol_error` tells the operator to
/// recreate the repository. Every hard cut to a persisted record shape has to
/// move this value with it.
///
/// `v68` is a single bump covering both of that round's format changes:
/// the `CurrentStatePartSource` enum with `LOCATOR_PAYLOAD_VERSION` 3 -> 4,
/// and `StoredCheckpointGcState` 3 -> 6 fields with
/// `CHECKPOINT_GC_STATE_FORMAT_VERSION` 1 -> 2. Every record is
/// `#[musli(packed)]`, so one bump carries any number of shape changes at no
/// extra cost to the user, who recreates the repository once either way.
///
/// `v69` hard-cuts the packed `ChangeRecord` snapshot payload from the legacy
/// JSON representation to schema-fingerprinted native typed-row payloads for
/// both plugin-owned and engine-owned Schema v1 rows. Repositories using v68
/// are upgraded by the explicit offline migration before the engine opens.
///
/// `v70` admits complete-state checkpoint fences whose single parent-root
/// edge is a physical state source rather than a semantic commit parent, and
/// persists canonical materialized sync aliases independently from rebuilt
/// snapshot roots. Older readers reject those manifest/sidecar shapes, so the
/// repository gate must fail before they reach either storage plane.
///
/// `v71` requires every live branch chronology root to have a durable tracked-
/// state root. In addition to the branch head, that includes the distinct
/// working-diff checkpoint cursor retained by the branch control. Checkpoint
/// aliases and chronology GC both rely on the complete root set, so v70
/// repositories are repaired by the explicit offline migration before open.
///
/// `v72` requires every live branch's private working-diff epoch to agree with
/// its branch control. The explicit offline migration rebuilds stale derived
/// projections once; application and protocol session opening contains no
/// legacy repair path.
///
/// `v73` durably amends every live branch's persisted `lix_account` schema
/// with the nullable `profile_uri` column. The schema catalog is repository
/// data, so changing only the bundled initialization document would leave v72
/// repositories on the historical SQL surface.
///
/// `v74` authenticates a row-primary-key secondary root beside every tracked-
/// state commit authority, including rootless replay commits. The index makes
/// historical PK reads bounded even when the row's file scope is not known by
/// the SQL predicate.
///
/// `v75` hard-cuts commit records to complete-snapshot semantics. Global
/// commits have no base; every local commit names the exact global commit
/// whose state is composed beneath its immutable local-overlay tracked root.
pub(crate) const CURRENT_FORMAT_VERSION: u32 = 75;
const REPOSITORY_PROTOCOL_PREFIX: &[u8] = b"tracked-default-branch.v";
pub(crate) const REPOSITORY_PROTOCOL_VALUE: &[u8] = b"tracked-default-branch.v75";
pub(crate) const REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP: &[u8] =
    b"tracked-default-branch.v72-row-pk-bootstrap";
/// Offline-migration fences for the v6 commit-record rewrite. The rewrite
/// publishes atomically with one of these markers so no engine — old or
/// current — ever reads v6 records under a marker it trusts as fully
/// readable; each resumes as its source version's migration edge.
pub(crate) const REPOSITORY_PROTOCOL_V72_COMMIT_REWRITE: &[u8] =
    b"tracked-default-branch.v72-commit-rewrite";
pub(crate) const REPOSITORY_PROTOCOL_V73_COMMIT_REWRITE: &[u8] =
    b"tracked-default-branch.v73-commit-rewrite";
pub(crate) const REPOSITORY_PROTOCOL_V74_COMMIT_REWRITE: &[u8] =
    b"tracked-default-branch.v74-commit-rewrite";

/// Raw status of the repository protocol marker. Engine opening consults this
/// before it touches any tracked-head space, whose physical IDs deliberately
/// remain stable across hard layout cuts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RepositoryProtocolStatus {
    /// The current layout has one authoritative current-state plane.
    Current,
    MigrationRequired {
        found_version: u32,
    },
    TooNew {
        found_version: u32,
    },
    Malformed,
    Missing,
}

pub(crate) fn parse_repository_protocol(value: &[u8]) -> RepositoryProtocolStatus {
    // Explicit offline-migration fence. Older engines do not recognize this
    // marker and therefore cannot open partially upgraded LXCS12 authorities;
    // this engine resumes it as the v72 migration edge.
    if value == REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP {
        return RepositoryProtocolStatus::MigrationRequired { found_version: 72 };
    }
    for (marker, found_version) in [
        (REPOSITORY_PROTOCOL_V72_COMMIT_REWRITE, 72),
        (REPOSITORY_PROTOCOL_V73_COMMIT_REWRITE, 73),
        (REPOSITORY_PROTOCOL_V74_COMMIT_REWRITE, 74),
    ] {
        if value == marker {
            return RepositoryProtocolStatus::MigrationRequired { found_version };
        }
    }
    let Some(version_bytes) = value.strip_prefix(REPOSITORY_PROTOCOL_PREFIX) else {
        return RepositoryProtocolStatus::Malformed;
    };
    if version_bytes.is_empty() || !version_bytes.iter().all(u8::is_ascii_digit) {
        return RepositoryProtocolStatus::Malformed;
    }
    let Ok(version_text) = std::str::from_utf8(version_bytes) else {
        return RepositoryProtocolStatus::Malformed;
    };
    let Ok(version) = version_text.parse::<u32>() else {
        return RepositoryProtocolStatus::Malformed;
    };
    if version.to_string().as_bytes() != version_bytes {
        return RepositoryProtocolStatus::Malformed;
    }
    match version.cmp(&CURRENT_FORMAT_VERSION) {
        std::cmp::Ordering::Less => RepositoryProtocolStatus::MigrationRequired {
            found_version: version,
        },
        std::cmp::Ordering::Equal => RepositoryProtocolStatus::Current,
        std::cmp::Ordering::Greater => RepositoryProtocolStatus::TooNew {
            found_version: version,
        },
    }
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
        Some(StorageProjectedValue::FullValue(value)) => parse_repository_protocol(value.as_ref()),
        Some(_) => RepositoryProtocolStatus::Malformed,
        None => RepositoryProtocolStatus::Missing,
    })
}

pub(crate) fn unsupported_repository_protocol_error() -> LixError {
    LixError::new(
        "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT",
        "repository uses an unsupported storage protocol; recreate the repository",
    )
}

pub(crate) fn migration_required_error(found_version: u32) -> LixError {
    LixError::new(
        "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED",
        format!(
            "repository format v{found_version} must be migrated to v{CURRENT_FORMAT_VERSION} with lix::migration::migrate_lix before opening"
        ),
    )
}

/// Pure seed plan for initializing an engine repository.
///
/// Tracked bootstrap facts go to the changelog. Moving heads are seeded in
/// the direct current-state control plane and retain a standalone immutable
/// branch-ref ledger change.
pub(crate) struct InitSeedPlan {
    global_commit: InitSeedCommit,
    main_commit: InitSeedCommit,
    changes: Vec<InitSeedChange>,
    branch_controls: Vec<InitBranchHeadControl>,
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

#[derive(Debug, Clone, PartialEq)]
struct InitSeedChange {
    id: ChangeId,
    row_pk: RowPk,
    schema_key: String,
    #[cfg(test)]
    snapshot_content: serde_json::Value,
    decoded_snapshot: std::sync::Arc<crate::plugin::runtime::WasmTypedRow>,
    created_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
struct InitSeedLiveRow {
    id: ChangeId,
    row_pk: RowPk,
    schema_key: String,
    #[cfg(test)]
    snapshot_content: serde_json::Value,
    decoded_snapshot: std::sync::Arc<crate::plugin::runtime::WasmTypedRow>,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    branch_id: String,
}

/// Initial direct branch controls are planned with the seed so their public
/// metadata is deterministic and independent of the flat live-state lane.
#[derive(Debug, Clone, PartialEq)]
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
    pub initial_global_commit_id: String,
    pub initial_commit_id: String,
}

/// Builds the canonical bootstrap changes for a new engine repository.
///
/// The initial commit tracks durable content rows. Branch heads are moving
/// pointers and therefore live in direct control records instead of the
/// changelog or flat current state.
pub(crate) fn plan_init_seed(functions: FunctionProviderHandle) -> Result<InitSeedPlan, LixError> {
    plan_init_seed_with_main_branch_id(functions, None)
}

/// Plans the repository seed with an optional externally selected main branch.
/// Sync mode uses this only for a brand-new local store: the handshake already
/// identified the server's default branch, so seeding the local selector with
/// that same identity avoids a bootstrap rebind transaction and its synthetic
/// local commit. Ordinary local opens keep the generated UUID path above.
pub(crate) fn plan_init_seed_with_main_branch_id(
    functions: FunctionProviderHandle,
    requested_main_branch_id: Option<&str>,
) -> Result<InitSeedPlan, LixError> {
    let main_branch_id = if let Some(branch_id) = requested_main_branch_id {
        crate::sync::validate_sync_branch_id(branch_id)?;
        branch_id.to_owned()
    } else {
        functions.call_uuid_v7().to_string()
    };
    let lix_id = functions.call_uuid_v7().to_string();
    let initial_global_commit_id = CommitId::with_change_address_space(functions.call_uuid_v7());
    let initial_commit_id = CommitId::with_change_address_space(functions.call_uuid_v7());
    let timestamp = functions.call_timestamp();

    let registered_schema_changes = registered_schema_seed_rows()
        .iter()
        .map(|seed| InitSeedChange {
            id: ChangeId::from(functions.call_uuid_v7()),
            row_pk: seed.row_pk.clone(),
            schema_key: REGISTERED_SCHEMA_KEY.to_owned(),
            #[cfg(test)]
            snapshot_content: seed.snapshot_content.clone(),
            decoded_snapshot: std::sync::Arc::clone(&seed.decoded_snapshot),
            created_at: timestamp,
        })
        .collect::<Vec<_>>();

    let global_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::uuid_from_canonical(GLOBAL_BRANCH_ID)
            .expect("global branch sentinel is a canonical UUID"),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(GLOBAL_BRANCH_ID, "global", true),
        timestamp,
    )?;
    let main_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::uuid_from_canonical(&main_branch_id)
            .expect("generated main branch ID is a canonical UUID"),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(&main_branch_id, "main", false),
        timestamp,
    )?;
    let kv_lix_id_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::single(LIX_ID_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(LIX_ID_KEY, &lix_id),
        timestamp,
    )?;
    let initial_checkpoint_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::uuid_from_canonical(&initial_commit_id.to_string())
            .expect("initial checkpoint commit ID is a canonical UUID"),
        CHECKPOINT_SCHEMA_KEY,
        checkpoint_snapshot(&initial_commit_id),
        timestamp,
    )?;
    let system_account_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::uuid_from_canonical(crate::SYSTEM_ACCOUNT_ID)
            .expect("system account ID is a canonical UUID"),
        ACCOUNT_SCHEMA_KEY,
        account_snapshot(crate::SYSTEM_ACCOUNT_ID, "System", "system"),
        timestamp,
    )?;
    let anonymous_account_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::uuid_from_canonical(crate::ANONYMOUS_ACCOUNT_ID)
            .expect("anonymous account ID is a canonical UUID"),
        ACCOUNT_SCHEMA_KEY,
        account_snapshot(crate::ANONYMOUS_ACCOUNT_ID, "Anonymous", "anonymous"),
        timestamp,
    )?;

    let initial_global_commit = InitSeedCommit {
        id: initial_global_commit_id,
        change_id: ChangeId::from(functions.call_uuid_v7()),
        parent_ids: Vec::new(),
        account_id: crate::SYSTEM_ACCOUNT_ID.to_string(),
        created_at: timestamp,
    };
    let initial_main_commit = InitSeedCommit {
        id: initial_commit_id,
        change_id: ChangeId::from(functions.call_uuid_v7()),
        parent_ids: vec![initial_global_commit_id],
        account_id: crate::SYSTEM_ACCOUNT_ID.to_string(),
        created_at: timestamp,
    };
    // Keep one distinct public ref change id per initial branch, matching the
    // old `lix_branch_ref` current rows without materializing those rows in
    // the ordinary current-state generation.
    let global_branch_ref_change = branch_ref_ledger_change(
        functions.call_uuid_v7(),
        GLOBAL_BRANCH_ID,
        initial_global_commit_id,
        timestamp,
    )?;
    let global_branch_control = InitBranchHeadControl {
        branch_id: GLOBAL_BRANCH_ID.to_string(),
        control: BranchHeadControl {
            head_commit_id: initial_global_commit_id,
            tracked_generation: initial_global_commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(initial_global_commit_id),
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
            tracked_generation: initial_commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: Some(initial_commit_id),
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: main_branch_ref_change.id,
            schema_presence_bloom: [0; 4],
        },
        branch_ref_change: main_branch_ref_change,
    };
    let default_branch_change = canonical_change(
        functions.call_uuid_v7(),
        RowPk::single(DEFAULT_BRANCH_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(DEFAULT_BRANCH_KEY, &main_branch_id),
        timestamp,
    )?;

    Ok(InitSeedPlan {
        global_commit: initial_global_commit,
        main_commit: initial_main_commit,
        changes: registered_schema_changes
            .into_iter()
            .chain([
                global_branch_descriptor_change,
                main_branch_descriptor_change,
                kv_lix_id_change,
                default_branch_change,
                initial_checkpoint_change,
                system_account_change,
                anonymous_account_change,
            ])
            .collect(),
        branch_controls: vec![global_branch_control, main_branch_control],
        receipt: InitReceipt {
            lix_id,
            global_branch_id: GLOBAL_BRANCH_ID.to_string(),
            main_branch_id,
            initial_global_commit_id: initial_global_commit_id.to_string(),
            initial_commit_id: initial_commit_id.to_string(),
        },
    })
}

/// Initializes an empty engine repository in one storage transaction.
///
/// The pure seed planner decides which bootstrap facts exist. This function is
/// only responsible for durably writing those facts to their owning stores:
/// changelog for tracked changes, and hot_state for the serving state
/// plus untracked moving refs.
pub(crate) async fn initialize<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    tracked_state: &TrackedStateContext,
) -> Result<InitReceipt, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    initialize_with_main_branch_id(storage, tracked_state, None).await
}

pub(crate) async fn initialize_with_main_branch_id<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    tracked_state: &TrackedStateContext,
    requested_main_branch_id: Option<&str>,
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
    let plan = plan_init_seed_with_main_branch_id(functions, requested_main_branch_id)?;
    let receipt = plan.receipt.clone();
    let mut writes = StorageWriteSet::new();
    let authored_changes = plan
        .changes
        .iter()
        .map(seed_change_to_change_record)
        .collect::<Result<Vec<_>, _>>()?;
    let branch_ref_ledger_changes = plan
        .branch_controls
        .iter()
        .map(|branch| seed_untracked_change_to_change_record(&branch.branch_ref_change))
        .collect::<Result<Vec<_>, _>>()?;

    // The genesis commit's delta members are exactly the authored seed
    // changes, so its touched-scope digest is derivable here without waiting
    // for the mutation inventory staged further below. Publishing it matters:
    // every history traversal reaches the genesis commit, so leaving it
    // undigested would put one unavoidable replay-state load in every read.
    let init_touched_scopes = authored_changes
        .iter()
        .map(|change| crate::changelog::CommitScopeKey {
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
        })
        .collect::<Vec<_>>();
    stage_init_changelog_commit(
        &mut read,
        &mut writes,
        &plan,
        branch_ref_ledger_changes.clone(),
        &init_touched_scopes,
    )
    .await?;

    {
        let root_deltas = authored_changes
            .iter()
            .map(|change| TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                row_pk: &change.row_pk,
                change_id: change.change_id,
                commit_id: plan.global_commit.id,
                deleted: false,
                created_at: change.created_at,
                updated_at: change.created_at,
            })
            .collect::<Vec<_>>();
        let commit_deltas = authored_changes
            .iter()
            .zip(root_deltas.iter().copied())
            .map(|(change, delta)| TrackedStateCommitDeltaRef {
                delta,
                metadata: change.metadata.as_ref(),
                snapshot: change.snapshot.as_deref(),
                origin_key: change.origin_key.as_deref(),
                base_coordinate: None,
                authored: true,
            })
            .collect::<Vec<_>>();
        let staged_delta = stage_commit_deltas_for_commit_state(&mut writes, &commit_deltas)?;
        crate::tracked_state::stage_change_locators(&mut writes, &staged_delta.locators);
        let mut tracked_writer = tracked_state.writer(&read, &mut writes);
        tracked_writer
            .stage_commit_root(&receipt.initial_global_commit_id, None, root_deltas)
            .await?;
        let snapshot_root = tracked_writer
            .staged_commit_roots()
            .find(|root| root.commit_id == plan.global_commit.id)
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "repository initialization did not stage its snapshot root",
                )
            })?;
        let initial_mutations = staged_delta.mutation_inventory().clone();
        let initial_segments = crate::tracked_state::staged_commit_delta_segment_bytes(
            &writes,
            plan.global_commit.id,
            &initial_mutations,
        )?;
        let initial_members = crate::tracked_state::staged_commit_delta_members(
            &read,
            plan.global_commit.id,
            &plan.global_commit.account_id,
            &initial_mutations,
            initial_segments,
        )
        .await?;
        let mut row_pk_index_overlay = crate::tracked_state::TrackedStateChunkOverlay::new();
        let row_pk_index_root_id = crate::tracked_state::stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut row_pk_index_overlay,
                None,
                &initial_members,
                plan.global_commit.id,
        )
        .await?;
        let physical_publication =
            crate::tracked_state::stage_current_state_scoped_ranges_from_published_parent(
                &read,
                &mut writes,
                None,
                plan.global_commit.id,
                &plan.global_commit.account_id,
                &initial_mutations,
            )
            .await?;
        let _initial_state =
            crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
                &mut writes,
                &CommitStateManifest {
                    commit_id: plan.global_commit.id,
                    change_account_id: plan.global_commit.account_id.clone(),
                    replay_debt: CommitStateReplayDebt::default(),
                    mutations: initial_mutations,
                    touched_scope_filter: physical_publication.touched_scope_filter().clone(),
                    global_scope: true,
                    current_state_scoped_ranges: physical_publication.root(),
                    row_pk_index_root_id,
                    snapshot_root: Some(Box::new(snapshot_root)),
                },
                &physical_publication,
            )?;

        // Main starts as an empty immutable overlay pinned to the global
        // genesis snapshot. Its causal parent is global, but its physical
        // state root deliberately has no parent: the base edge supplies the
        // global half of the effective state.
        let empty_deltas = Vec::<TrackedStateDeltaRef<'_>>::new();
        let mut tracked_writer = tracked_state.writer(&read, &mut writes);
        tracked_writer
            .stage_commit_root(&receipt.initial_commit_id, None, empty_deltas)
            .await?;
        let main_snapshot_root = tracked_writer
            .staged_commit_roots()
            .find(|root| root.commit_id == plan.main_commit.id)
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "repository initialization did not stage the main overlay root",
                )
            })?;
        let main_mutations = CommitStateMutationInventory::default();
        let main_members = Vec::new();
        let mut main_row_pk_overlay = crate::tracked_state::TrackedStateChunkOverlay::new();
        let main_row_pk_index_root_id = crate::tracked_state::stage_row_pk_index_from_members(
            &read,
            &mut writes,
            &mut main_row_pk_overlay,
            None,
            &main_members,
            plan.main_commit.id,
        )
        .await?;
        let main_publication =
            crate::tracked_state::stage_current_state_scoped_ranges_from_published_parent(
                &read,
                &mut writes,
                None,
                plan.main_commit.id,
                &plan.main_commit.account_id,
                &main_mutations,
            )
            .await?;
        let _main_state = crate::tracked_state::stage_certified_commit_state_manifest_with_handle(
            &mut writes,
            &CommitStateManifest {
                commit_id: plan.main_commit.id,
                change_account_id: plan.main_commit.account_id.clone(),
                replay_debt: CommitStateReplayDebt::default(),
                mutations: main_mutations,
                touched_scope_filter: main_publication.touched_scope_filter().clone(),
                global_scope: false,
                current_state_scoped_ranges: main_publication.root(),
                row_pk_index_root_id: main_row_pk_index_root_id,
                snapshot_root: Some(Box::new(main_snapshot_root)),
            },
            &main_publication,
        )?;

        // Seed global with its complete state. Main's physical generation is
        // only the local overlay plus the registered-schema serving cache;
        // ordinary values are resolved through its pinned global base.
        let tracked_head_deltas = authored_changes
            .iter()
            .zip(&plan.changes)
            .map(|(change, seed)| {
                Ok(CurrentStateDeltaRef {
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    row_pk: &change.row_pk,
                    change_id: Some(change.change_id),
                    commit_id: Some(plan.global_commit.id),
                    untracked: false,
                    deleted: false,
                    created_at: change.created_at,
                    updated_at: change.created_at,
                    snapshot: Some(seed.decoded_snapshot.durable_payload_ref().map_err(
                        |error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("failed to encode bootstrap snapshot: {error:?}"),
                            )
                        },
                    )?),
                    metadata: change.metadata.as_ref(),
                    columnar_base_coordinate: None,
                })
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let tracked_head = TrackedHeadContext::new();
        let absence_guards = std::collections::BTreeSet::default();
        for branch in &plan.branch_controls {
            let mut head_deltas = tracked_head_deltas.clone();
            // Checkpoints and built-in accounts are global facts. Staging them
            // onto main would turn the global snapshot into a local overlay.
            if branch.branch_id != GLOBAL_BRANCH_ID {
                head_deltas.retain(|delta| delta.schema_key == REGISTERED_SCHEMA_KEY);
            }
            let generation = if branch.branch_id == GLOBAL_BRANCH_ID {
                plan.global_commit.id
            } else {
                plan.main_commit.id
            };
            let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
            tracked_head
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    &branch.branch_id,
                    None,
                    generation,
                    &head_deltas,
                    &absence_guards,
                    None,
                    Some(generation),
                    &mut working_diff_coverage,
                )
                .await?;
            stage_tracked_working_diff_epoch(
                &mut writes,
                &branch.branch_id,
                TrackedWorkingDiffEpoch {
                    checkpoint_commit_id: generation,
                    generation,
                    coverage: working_diff_coverage,
                },
            )?;
            let mut control = branch.control;
            control.note_schemas(head_deltas.iter().map(|delta| delta.schema_key));
            stage_branch_head_control(&mut writes, &branch.branch_id, control)?;
        }
    }
    crate::catalog::stage_catalog_revision(&mut writes);
    crate::account::stage_account_revision(&mut writes);
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
        RepositoryProtocolStatus::MigrationRequired { .. }
        | RepositoryProtocolStatus::TooNew { .. }
        | RepositoryProtocolStatus::Malformed => Err(unsupported_repository_protocol_error()),
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

fn seed_change_to_change_record(change: &InitSeedChange) -> Result<ChangeRecord, LixError> {
    Ok(ChangeRecord {
        format_version: 1,
        change_id: change.id,
        account_id: crate::SYSTEM_ACCOUNT_ID.to_string(),
        row_pk: change.row_pk.clone(),
        schema_key: change.schema_key.clone(),
        file_id: None,
        metadata: None,
        snapshot: Some(seed_snapshot(&change.decoded_snapshot)?),
        created_at: change.created_at,
        origin_key: None,
    })
}

fn seed_untracked_change_to_change_record(row: &InitSeedLiveRow) -> Result<ChangeRecord, LixError> {
    Ok(ChangeRecord {
        format_version: 2,
        change_id: row.id,
        account_id: crate::SYSTEM_ACCOUNT_ID.to_string(),
        row_pk: row.row_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: None,
        metadata: None,
        snapshot: Some(seed_snapshot(&row.decoded_snapshot)?),
        created_at: row.updated_at,
        origin_key: None,
    })
}

fn seed_snapshot(row: &crate::plugin::runtime::WasmTypedRow) -> Result<Vec<u8>, LixError> {
    row.durable_payload()
        .map(|payload| payload.to_vec())
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to encode typed repository seed row: {error:?}"),
            )
        })
}

async fn stage_init_changelog_commit(
    read: &mut impl StorageAdapterRead,
    writes: &mut StorageWriteSet,
    plan: &InitSeedPlan,
    changes: Vec<ChangeRecord>,
    touched_scopes: &[crate::changelog::CommitScopeKey],
) -> Result<(), LixError> {
    let global_commit = CommitRecord {
        touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::exact(touched_scopes),
        format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
        commit_id: plan.global_commit.id,
        generation: 0,
        parent_commit_ids: plan.global_commit.parent_ids.clone(),
        base_commit_id: None,
        first_parent_jump_commit_id: plan.global_commit.id,
        first_parent_jump_span: 0,
        account_id: plan.global_commit.account_id.clone(),
        created_at: plan.global_commit.created_at,
    };
    let main_commit = CommitRecord {
        touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::exact(&[]),
        format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
        commit_id: plan.main_commit.id,
        generation: 1,
        parent_commit_ids: plan.main_commit.parent_ids.clone(),
        base_commit_id: Some(plan.global_commit.id),
        first_parent_jump_commit_id: plan.global_commit.id,
        first_parent_jump_span: 1,
        account_id: plan.main_commit.account_id.clone(),
        created_at: plan.main_commit.created_at,
    };
    let mut writer = ChangelogContext::new().writer(read, writes);
    writer
        .stage_append(ChangelogAppend {
            commits: vec![global_commit, main_commit],
            changes,
        })
        .await
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
    let row_pk =
        RowPk::uuid_from_canonical(branch_id).expect("seed branch IDs are canonical UUIDs");
    let snapshot_content = branch_ref_snapshot(branch_id, commit_id);
    let decoded_snapshot =
        seed_decoded_snapshot(BRANCH_REF_SCHEMA_KEY, &row_pk, &snapshot_content)?;
    Ok(InitSeedLiveRow {
        id: ChangeId::from(id),
        row_pk,
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        #[cfg(test)]
        snapshot_content,
        decoded_snapshot,
        created_at: timestamp,
        updated_at: timestamp,
        global: branch_id == GLOBAL_BRANCH_ID,
        branch_id: branch_id.to_string(),
    })
}

fn canonical_change(
    id: uuid::Uuid,
    row_pk: RowPk,
    schema_key: &str,
    snapshot_content: serde_json::Value,
    created_at: LixTimestamp,
) -> Result<InitSeedChange, LixError> {
    let decoded_snapshot = seed_decoded_snapshot(schema_key, &row_pk, &snapshot_content)?;
    Ok(InitSeedChange {
        id: ChangeId::from(id),
        row_pk,
        schema_key: schema_key.to_string(),
        #[cfg(test)]
        snapshot_content,
        decoded_snapshot,
        created_at,
    })
}

struct RegisteredSchemaSeedRow {
    row_pk: RowPk,
    #[cfg(test)]
    snapshot_content: serde_json::Value,
    decoded_snapshot: std::sync::Arc<crate::plugin::runtime::WasmTypedRow>,
}

fn registered_schema_seed_rows() -> &'static [RegisteredSchemaSeedRow] {
    static ROWS: std::sync::OnceLock<Vec<RegisteredSchemaSeedRow>> = std::sync::OnceLock::new();
    ROWS.get_or_init(|| {
        seed_schema_definitions()
            .into_iter()
            .map(|schema| {
                let key = schema_key_from_definition(schema)
                    .expect("embedded bootstrap schema has a valid key");
                let row_pk = registered_schema_row_pk(&key.schema_key)
                    .expect("embedded bootstrap schema has a valid identity");
                let snapshot_content = registered_schema_snapshot(schema)
                    .expect("embedded bootstrap schema has a valid registered row");
                let decoded_snapshot =
                    seed_decoded_snapshot(REGISTERED_SCHEMA_KEY, &row_pk, &snapshot_content)
                        .expect("embedded bootstrap schema encodes as a native row");
                RegisteredSchemaSeedRow {
                    row_pk,
                    #[cfg(test)]
                    snapshot_content,
                    decoded_snapshot,
                }
            })
            .collect()
    })
}

fn seed_decoded_snapshot(
    schema_key: &str,
    row_pk: &RowPk,
    snapshot: &serde_json::Value,
) -> Result<std::sync::Arc<crate::plugin::runtime::WasmTypedRow>, LixError> {
    let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
        .plan_for_key(schema_key)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("embedded schema catalog is missing '{schema_key}'"),
            )
        })?;
    Ok(std::sync::Arc::new(
        crate::plugin::runtime::WasmTypedRow::from_normalized_json(plan, row_pk, snapshot)?,
    ))
}

fn branch_descriptor_snapshot(id: &str, name: &str, hidden: bool) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "hidden": hidden,
    })
}

fn branch_ref_snapshot(branch_id: &str, commit_id: CommitId) -> serde_json::Value {
    json!({
        "id": branch_id,
        "commit_id": commit_id.to_string(),
    })
}

fn key_value_snapshot(key: &str, value: &str) -> serde_json::Value {
    json!({
        "key": key,
        "value": value,
    })
}

fn account_snapshot(id: &str, name: &str, kind: &str) -> serde_json::Value {
    json!({
        "id": id,
        "name": name,
        "kind": kind,
        "status": "active",
    })
}

fn registered_schema_snapshot(schema: &serde_json::Value) -> Result<serde_json::Value, LixError> {
    let schema_key = schema_key_from_definition(schema)?;
    Ok(json!({
        "schema_key": schema_key.schema_key,
        "value": schema,
    }))
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
    fn plan_init_seed_returns_tracked_repository_bootstrap_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.changes.len(), seed_schema_definitions().len() + 7);
        assert_eq!(plan.receipt.global_branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(plan.receipt.main_branch_id, test_uuid(1));
        assert_eq!(plan.receipt.lix_id, test_uuid(2));
        // The initial commit id reserves its low 32 bits for packed change
        // ordinals like every other commit id, so it is not the raw v7 value.
        assert_eq!(
            plan.receipt.initial_commit_id,
            "01920000-0000-7000-8000-000400000000"
        );
        assert_eq!(plan.global_commit.id, plan.receipt.initial_global_commit_id);
        assert_eq!(plan.main_commit.id, plan.receipt.initial_commit_id);
    }

    #[test]
    fn plan_init_seed_commit_header_tracks_schema_registrations_descriptor_and_lix_id_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.global_commit.id, plan.receipt.initial_global_commit_id);
        assert_eq!(plan.main_commit.id, plan.receipt.initial_commit_id);
        assert!(plan.global_commit.parent_ids.is_empty());
        assert_eq!(plan.main_commit.parent_ids, vec![plan.global_commit.id]);
        assert_eq!(plan.global_commit.account_id, crate::SYSTEM_ACCOUNT_ID);
        assert_eq!(plan.main_commit.account_id, crate::SYSTEM_ACCOUNT_ID);
        assert_eq!(
            plan.global_commit.created_at.to_string(),
            "2026-01-01T00:00:00.001Z"
        );

        let change_ids = plan
            .changes
            .iter()
            .map(|change| change.id.to_string())
            .collect::<Vec<_>>();
        assert_eq!(change_ids.len(), seed_schema_definitions().len() + 7);
        let first_seed_change_id = test_uuid(5);
        assert!(change_ids.contains(&first_seed_change_id));
        assert!(!change_ids.contains(&plan.global_commit.change_id.to_string()));
        assert!(!change_ids.contains(&plan.main_commit.change_id.to_string()));

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
                .pointer("/value/key")
                .and_then(JsonValue::as_str)
                == Some(REGISTERED_SCHEMA_KEY)
        }));
        assert!(registered_schema_changes.iter().any(|change| {
            snapshot(change)
                .pointer("/value/key")
                .and_then(JsonValue::as_str)
                == Some(KEY_VALUE_SCHEMA_KEY)
        }));
    }

    #[test]
    fn plan_init_seed_keeps_branch_heads_out_of_tracked_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
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
                Some(if branch.branch_id == GLOBAL_BRANCH_ID {
                    plan.receipt.initial_global_commit_id.as_str()
                } else {
                    plan.receipt.initial_commit_id.as_str()
                })
            );
        }
    }

    #[test]
    fn plan_init_seed_tracks_default_branch_pointing_to_main() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let default_branch_change = plan
            .changes
            .iter()
            .find(|change| {
                change.schema_key == KEY_VALUE_SCHEMA_KEY
                    && change.row_pk == RowPk::single(DEFAULT_BRANCH_KEY)
            })
            .expect("tracked default branch change should exist");

        let snapshot = snapshot(default_branch_change);
        assert_eq!(
            snapshot.get("key").and_then(JsonValue::as_str),
            Some(DEFAULT_BRANCH_KEY)
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
        let commit_ids = [CommitId::for_test_label(&receipt.initial_global_commit_id)];
        let commits = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await
            .expect("commit should load");
        let Some(record) = commits.into_iter().next().and_then(|(_, value)| value) else {
            panic!("initial commit should exist");
        };

        assert_eq!(record.commit_id, receipt.initial_global_commit_id);
        let commit_change_id = record.change_id();
        let membership_read = storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("membership read should open");
        let change_refs =
            crate::tracked_state::load_commit_delta_change_ids(&membership_read, record.commit_id)
                .await
                .expect("initial commit membership should load");
        assert_eq!(change_refs.len(), seed_schema_definitions().len() + 7);
        assert!(
            !change_refs.contains(&record.change_id()),
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
        {
            let read = storage
                .begin_read(crate::storage_adapter::StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            tracked_state
                .root_rebuilder(&read, &mut writes)
                .rebuild_commit_root_at(&receipt.initial_global_commit_id)
                .await
                .expect("initial commit root should rebuild from its packed delta");
            drop(read);
            storage
                .commit_write_set(
                    writes,
                    crate::storage_adapter::StorageWriteOptions::default(),
                )
                .await
                .expect("rebuilt initial commit root should commit");
        }
        let mut tracked_reader = tracked_state.reader(
            storage
                .begin_read(crate::storage_adapter::StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let rows = tracked_reader
            .scan_batch_at_commit(
                &receipt.initial_global_commit_id,
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
    async fn repository_protocol_rejects_pre_serving_base_lineage_marker() {
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
            RepositoryProtocolStatus::Malformed
        );
    }

    #[tokio::test]
    async fn repository_protocol_rejects_pre_split_commit_state_marker() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_PROTOCOL_SPACE,
            REPOSITORY_PROTOCOL_KEY,
            &b"immutable-physical-commit-state.v57"[..],
        );
        storage
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions::default(),
            )
            .await
            .expect("pre-split protocol marker should stage");
        let read = storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("protocol read should open");

        assert_eq!(
            repository_protocol_status(&read)
                .await
                .expect("protocol status should load"),
            RepositoryProtocolStatus::Malformed
        );
    }

    #[tokio::test]
    async fn repository_protocol_requires_v68_migration() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        writes.put(
            REPOSITORY_PROTOCOL_SPACE,
            REPOSITORY_PROTOCOL_KEY,
            &b"tracked-default-branch.v68"[..],
        );
        storage
            .commit_write_set(
                writes,
                crate::storage_adapter::StorageWriteOptions::default(),
            )
            .await
            .expect("v68 protocol marker should stage");
        let read = storage
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("protocol read should open");

        assert_eq!(
            repository_protocol_status(&read)
                .await
                .expect("protocol status should load"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 68 }
        );
    }

    #[test]
    fn repository_protocol_parser_distinguishes_versions() {
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v75"),
            RepositoryProtocolStatus::Current
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v74-commit-rewrite"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 74 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v73-commit-rewrite"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 73 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v72-commit-rewrite"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 72 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v74"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 74 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v73"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 73 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v69"),
            RepositoryProtocolStatus::MigrationRequired { found_version: 69 }
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v76"),
            RepositoryProtocolStatus::TooNew { found_version: 76 }
        );
        assert_eq!(
            parse_repository_protocol(b"not-a-lix-format"),
            RepositoryProtocolStatus::Malformed
        );
        assert_eq!(
            parse_repository_protocol(b"tracked-default-branch.v069"),
            RepositoryProtocolStatus::Malformed
        );
    }

    fn snapshot(change: &InitSeedChange) -> JsonValue {
        change.snapshot_content.clone()
    }

    fn untracked_snapshot(row: &InitSeedLiveRow) -> JsonValue {
        row.snapshot_content.clone()
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

    #[tokio::test]
    async fn bootstrap_accounts_are_global_rows_inherited_by_branches() {
        use crate::branch::BranchHeadControlContext;
        use crate::hot_state::TrackedHeadContext;
        use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};
        use crate::tracked_state::TrackedStateContext;

        let storage = StorageAdapter::new(Memory::new());
        let tracked_state = TrackedStateContext::new();
        let receipt = initialize(storage.clone(), &tracked_state)
            .await
            .expect("initialize should succeed");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let controls = BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .expect("scan branch-head controls");
        let tracked_head = TrackedHeadContext::new();
        let mut saw_global = false;
        for (branch_id, control) in &controls {
            let has_accounts = tracked_head
                .reader(&read)
                .has_schema_rows(branch_id, *control, ACCOUNT_SCHEMA_KEY)
                .await
                .expect("schema presence probe should succeed");
            if branch_id == GLOBAL_BRANCH_ID {
                saw_global = true;
                assert!(
                    has_accounts,
                    "GLOBAL_BRANCH_ID must physically store bootstrap accounts"
                );
            } else {
                assert!(
                    !has_accounts,
                    "branch {branch_id} must not physically store bootstrap accounts"
                );
            }
        }
        assert!(saw_global, "initialization must publish a global branch");

        let mut tracked_reader = tracked_state.reader(read);
        let rows = tracked_reader
            .scan_batch_at_commit(
                &receipt.initial_global_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec![ACCOUNT_SCHEMA_KEY.to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("scan should succeed");

        let account_rows = rows.into_rows();
        assert_eq!(
            account_rows.len(),
            2,
            "tracked state should contain exactly two bootstrap accounts"
        );
        for row in &account_rows {
            assert_eq!(
                row.commit_id, receipt.initial_global_commit_id,
                "account {:?} should be in the initial global commit",
                row.row_pk
            );
        }
    }
}
