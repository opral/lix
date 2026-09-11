//! Explicit migration pins. Like migration's ordinary temporary branch refs,
//! these survive interruption until exact outcome-backed cleanup, never opening.
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};
use crate::{LixError, changelog::CommitId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
pub(crate) const NATIVE_GLOBAL_RETENTION_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0008_000c),
    "gc.native_global_migration_retention.v1",
);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeGlobalRetention {
    version: u32,
    repository: String,
    account: String,
    attempt: String,
    binding_digest: [u8; 32],
    global_anchor: String,
    global_checkpoint: String,
    tips: BTreeMap<String, String>,
}
fn invalid() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_RETENTION_INVALID",
        "global migration pin does not match its exact authenticated native body proof",
    )
}
fn key(repository: &str, account: &str, attempt: &str) -> Result<StorageKey, LixError> {
    let mut v = Vec::with_capacity(48);
    for s in [repository, account, attempt] {
        v.extend_from_slice(
            &crate::storage_codec::id_string::uuid_bytes_from_canonical(s).ok_or_else(invalid)?,
        );
    }
    Ok(StorageKey(Bytes::from(v)))
}
impl NativeGlobalRetention {
    fn validate(&self) -> Result<(), LixError> {
        key(&self.repository, &self.account, &self.attempt)?;
        if self.version != 1 || self.tips.is_empty() || self.tips.len() > 1025 {
            return Err(invalid());
        }
        for s in std::iter::once(&self.global_anchor)
            .chain(std::iter::once(&self.global_checkpoint))
            .chain(self.tips.values())
        {
            CommitId::parse_lix(s, "global migration root")?;
        }
        for branch in self.tips.keys() {
            crate::storage_codec::id_string::uuid_bytes_from_canonical(branch)
                .ok_or_else(invalid)?;
        }
        Ok(())
    }
    pub(crate) fn roots(&self) -> Result<Vec<CommitId>, LixError> {
        self.validate()?;
        std::iter::once(&self.global_anchor)
            .chain(std::iter::once(&self.global_checkpoint))
            .chain(self.tips.values())
            .map(|s| CommitId::parse_lix(s, "migration root"))
            .collect()
    }
}
pub(crate) async fn load_global_retention(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    attempt: &str,
) -> Result<Option<(NativeGlobalRetention, Bytes)>, LixError> {
    let values = PointReadPlan::new(
        NATIVE_GLOBAL_RETENTION_SPACE,
        &[key(repository, account, attempt)?],
    )
    .materialize(read, Default::default())
    .await?
    .value;
    let Some(value) = values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(raw) = value else {
        return Err(invalid());
    };
    if raw.len() > 256 * 1024 {
        return Err(invalid());
    }
    let state: NativeGlobalRetention = serde_json::from_slice(&raw).map_err(|_| invalid())?;
    state.validate()?;
    if state.repository != repository || state.account != account || state.attempt != attempt {
        return Err(invalid());
    }
    Ok(Some((state, raw)))
}
/// Constructor-private native importer proof carries newly staged complete roots;
/// do not try loading those roots from the pre-import read snapshot.
pub(crate) async fn stage_global_body_pin(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    proof: &crate::sync::VerifiedGlobalMigrationBody,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let request = proof.request();
    let address = key(proof.repository(), proof.account(), &request.attempt_id)?;
    let loaded = load_global_retention(
        read,
        proof.repository(),
        proof.account(),
        &request.attempt_id,
    )
    .await?;
    let (mut state, prior) = match loaded {
        Some((state, raw)) => {
            if state.binding_digest != request.binding_digest()? {
                return Err(invalid());
            }
            (state, Some(raw))
        }
        None => (
            NativeGlobalRetention {
                version: 1,
                repository: proof.repository().into(),
                account: proof.account().into(),
                attempt: request.attempt_id.clone(),
                binding_digest: request.binding_digest()?,
                global_anchor: request.expected_authority_head_commit_id.clone(),
                global_checkpoint: request.checkpoint_commit_id.clone(),
                tips: BTreeMap::new(),
            },
            None,
        ),
    };
    if let Some(old) = state.tips.get(proof.branch()) {
        if old != proof.previous() && old != proof.tip() {
            return Err(invalid());
        }
    }
    state.tips.insert(proof.branch().into(), proof.tip().into());
    state.validate()?;
    let bytes = serde_json::to_vec(&state).map_err(|_| invalid())?;
    if bytes.len() > 256 * 1024 {
        return Err(invalid());
    }
    writes.put(
        NATIVE_GLOBAL_RETENTION_SPACE,
        address.clone(),
        StorageValue {
            bytes: bytes.into(),
        },
    );
    let revision = crate::storage_adapter::load_repository_mutation_revision(read).await?;
    let mut guards = proof.anchor_guards().to_vec();
    guards.push(match prior {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: NATIVE_GLOBAL_RETENTION_SPACE,
            key: address,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: NATIVE_GLOBAL_RETENTION_SPACE,
            key: address,
        },
    });
    guards.push(crate::storage_adapter::repository_mutation_revision_precondition(revision));
    guards.push(
        crate::sync::uncommitted_global_migration_guard(
            read,
            proof.repository(),
            proof.account(),
            request,
        )
        .await?,
    );
    guards.push(
        crate::sync::require_unaborted_global_migration(
            read,
            proof.repository(),
            proof.account(),
            request,
        )
        .await?,
    );
    Ok(guards)
}
pub(crate) async fn require_complete_global_migration_pin(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &crate::sync::NativeGlobalMigrationRequest,
) -> Result<StoragePrecondition, LixError> {
    let (state, raw) = load_global_retention(read, repository, account, &request.attempt_id)
        .await?
        .ok_or_else(invalid)?;
    if state.binding_digest != request.binding_digest()?
        || state.tips.get(crate::GLOBAL_BRANCH_ID) != Some(&request.captured_local_head_commit_id)
    {
        return Err(invalid());
    }
    for branch in &request.new_branches {
        if state.tips.get(&branch.branch_id) != Some(&branch.head_commit_id) {
            return Err(invalid());
        }
    }
    Ok(StoragePrecondition::KeyValueEquals {
        space: NATIVE_GLOBAL_RETENTION_SPACE,
        key: key(repository, account, &request.attempt_id)?,
        expected: raw,
    })
}
pub(super) async fn load_global_migration_roots(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<std::collections::BTreeSet<CommitId>, LixError> {
    let mut scan = read
        .begin_scan(
            NATIVE_GLOBAL_RETENTION_SPACE,
            crate::storage_adapter::StoragePrefix {
                bytes: Bytes::new(),
            }
            .to_range()?,
            Default::default(),
        )
        .await?;
    let mut roots = std::collections::BTreeSet::new();
    while let Some(entries) = scan.next_chunk().await? {
        for entry in entries {
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(invalid());
            };
            if bytes.len() > 256 * 1024 {
                return Err(invalid());
            }
            let state: NativeGlobalRetention =
                serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            state.validate()?;
            if entry.key != key(&state.repository, &state.account, &state.attempt)? {
                return Err(invalid());
            }
            roots.extend(state.roots()?);
        }
    }
    Ok(roots)
}
pub(crate) async fn stage_cleanup_global_migration_pin(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    proof: &crate::sync::AuthorizedGlobalMigrationCleanup,
) -> Result<(bool, Vec<StoragePrecondition>), LixError> {
    let request = proof.request();
    let address = key(proof.repository(), proof.account(), &request.attempt_id)?;
    let mut guards = proof.guards().to_vec();
    let loaded = load_global_retention(
        read,
        proof.repository(),
        proof.account(),
        &request.attempt_id,
    )
    .await?;
    let Some((state, raw)) = loaded else {
        return Ok((false, guards));
    };
    if state.binding_digest != request.binding_digest()? {
        return Err(invalid());
    }
    guards.push(StoragePrecondition::KeyValueEquals {
        space: NATIVE_GLOBAL_RETENTION_SPACE,
        key: address.clone(),
        expected: raw,
    });
    guards.push(
        crate::storage_adapter::repository_mutation_revision_precondition(
            crate::storage_adapter::load_repository_mutation_revision(read).await?,
        ),
    );
    writes.delete(NATIVE_GLOBAL_RETENTION_SPACE, address);
    Ok((true, guards))
}

/// Only the immutable restart owner may publish these guards with its terminal
/// abort record. GC revision plus exact pin CAS fences in-flight body imports.
pub(crate) async fn stage_abandon_global_migration_pin(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    proof: &crate::sync::AuthorizedGlobalRestart,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let repository = proof.repository();
    let account = proof.account();
    let request = proof.request();
    request.validate()?;
    let address = key(repository, account, &request.attempt_id)?;
    let loaded = load_global_retention(read, repository, account, &request.attempt_id).await?;
    let guard = match loaded {
        Some((state, expected)) => {
            if state.binding_digest != request.binding_digest()? {
                return Err(invalid());
            }
            writes.delete(NATIVE_GLOBAL_RETENTION_SPACE, address.clone());
            StoragePrecondition::KeyValueEquals {
                space: NATIVE_GLOBAL_RETENTION_SPACE,
                key: address,
                expected,
            }
        }
        None => StoragePrecondition::KeyAbsent {
            space: NATIVE_GLOBAL_RETENTION_SPACE,
            key: address,
        },
    };
    Ok(vec![
        guard,
        crate::storage_adapter::repository_mutation_revision_precondition(
            crate::storage_adapter::load_repository_mutation_revision(read).await?,
        ),
    ])
}

// gc::native_global_retention addition, reexport through gc facade.
pub(crate) async fn global_migration_pin_absence(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &crate::sync::NativeGlobalMigrationRequest,
) -> Result<Option<StoragePrecondition>, LixError> {
    request.validate()?;
    match load_global_retention(read, repository, account, &request.attempt_id).await? {
        Some((state, _)) => {
            if state.binding_digest != request.binding_digest()? {
                return Err(invalid());
            }
            Ok(None)
        }
        None => Ok(Some(StoragePrecondition::KeyAbsent {
            space: NATIVE_GLOBAL_RETENTION_SPACE,
            key: key(repository, account, &request.attempt_id)?,
        })),
    }
}
