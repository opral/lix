//! Child of sync::repository: explicit migration inspection, never opening.
use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ConversionCoordinate {
    pub head: String,
    pub checkpoint: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FullConversionBranch {
    pub branch_id: String,
    /// None plus known_deleted=false means never observed, not deleted.
    pub confirmed: Option<ConversionCoordinate>,
    pub known_deleted: bool,
    pub local: Option<ConversionCoordinate>,
    pub pending_reset: Option<serde_json::Value>,
    pub retained_rows: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FullConversionManifest {
    pub repository_id: String,
    pub account_id: String,
    pub branches: Vec<FullConversionBranch>,
    pub recoverable_uploads: bool,
    pub source_replica_state_digest: [u8; 32],
}
/// Non-serializable owner evidence: caller must bind the frozen source bank,
/// pointer claim and mutation revision when persisting its migration journal.
pub(crate) struct InspectedFullConversion {
    manifest: FullConversionManifest,
}
impl InspectedFullConversion {
    pub(crate) fn manifest(&self) -> &FullConversionManifest {
        &self.manifest
    }
}
fn unresolved(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_CONVERSION_UNRESOLVED",message).with_details(serde_json::json!({"sourcePreserved":true,"recoveryApi":"export_replica_recovery","requiresRetainedSourceAdmission":true}))
}
pub(crate) async fn inspect_full_conversion_manifest(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<InspectedFullConversion, LixError> {
    let identity = inspect_replica_rebuild_source(read, crate::init::CURRENT_FORMAT_VERSION)
        .await?
        .ok_or_else(|| unresolved("source is not an identified full replica"))?;
    let (state, raw) = load_replica_state(read).await?;
    let state = state.ok_or_else(|| unresolved("source replica receipt disappeared"))?;
    let raw = raw.ok_or_else(|| unresolved("source replica receipt bytes missing"))?;
    let controls = BranchHeadControlContext::new()
        .reader(read)
        .scan()
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut ids = state
        .authoritative_branches
        .keys()
        .chain(controls.keys())
        .chain(state.pending_resets.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    if ids.len() > 1024 {
        return Err(unresolved(
            "source branch inventory exceeds migration batch limit",
        ));
    }
    let mut branches = Vec::new();
    let current = TrackedHeadContext::new().reader(read);
    for branch_id in std::mem::take(&mut ids) {
        let (confirmed, known_deleted) = match state.authoritative_branches.get(&branch_id) {
            Some(AuthoritativeBranchCoordinate::Headed {
                head_commit_id,
                checkpoint_commit_id,
            }) => (
                Some(ConversionCoordinate {
                    head: head_commit_id.clone(),
                    checkpoint: checkpoint_commit_id.clone(),
                }),
                false,
            ),
            Some(AuthoritativeBranchCoordinate::Deleted) => (None, true),
            None => (None, false),
        };
        let (local, retained_rows) = if let Some(control) = controls.get(&branch_id) {
            let checkpoint = control
                .working_diff_checkpoint_commit_id
                .ok_or_else(|| unresolved("source branch checkpoint missing"))?;
            let retained = current
                .scan_live_batch_for_retention(
                    &branch_id,
                    *control,
                    &TrackedStateScanRequest {
                        filter: TrackedStateFilter {
                            include_tombstones: true,
                            ..Default::default()
                        },
                        read_columns: TrackedStateReadColumns {
                            columns: vec!["untracked".into()],
                        },
                        limit: None,
                    },
                    Some(true),
                )
                .await?;
            (
                Some(ConversionCoordinate {
                    head: control.head_commit_id.to_string(),
                    checkpoint: checkpoint.to_string(),
                }),
                !retained.is_empty(),
            )
        } else {
            (None, false)
        };
        branches.push(FullConversionBranch {
            pending_reset: state
                .pending_resets
                .get(&branch_id)
                .map(serde_json::to_value)
                .transpose()
                .map_err(|_| unresolved("reset encoding failed"))?,
            branch_id,
            confirmed,
            known_deleted,
            local,
            retained_rows,
        });
    }
    Ok(InspectedFullConversion {
        manifest: FullConversionManifest {
            repository_id: identity.repository_id,
            account_id: state.active_account_id,
            branches,
            recoverable_uploads: crate::session::has_recoverable_uploads(read).await?,
            source_replica_state_digest: *blake3::hash(&raw).as_bytes(),
        },
    })
}
/// Enumerate every ordinary pending branch before starting network work.
/// The caller must collect an exact inclusion proof for this complete set.
pub(crate) fn ordinary_pending_conversion_branches(
    proof: &InspectedFullConversion,
    selected: &str,
) -> Result<Vec<String>, LixError> {
    let manifest = &proof.manifest;
    if manifest.recoverable_uploads {
        return Err(unresolved(
            "unfinished local uploads require retained-source recovery",
        ));
    }
    let mut result = Vec::new();
    for branch in &manifest.branches {
        if branch.pending_reset.is_some() || branch.retained_rows {
            return Err(unresolved(
                "reset or local-only rows require retained-source recovery",
            ));
        }
        if branch.confirmed == branch.local {
            continue;
        }
        if branch.branch_id == crate::GLOBAL_BRANCH_ID {
            return Err(unresolved(
                "pending global/catalog changes require native global migration",
            ));
        }
        let (Some(base), Some(local)) = (&branch.confirmed, &branch.local) else {
            return Err(unresolved(
                "new or deleted local branches require native branch-lifecycle migration",
            ));
        };
        if base.checkpoint != local.checkpoint {
            return Err(unresolved(
                "pending checkpoint changes require native checkpoint migration",
            ));
        }
        result.push(branch.branch_id.clone());
    }
    result.sort_by_key(|id| (id == selected, id.clone()));
    Ok(result)
}
/// Builds one ordinary branch request after complete-manifest scope validation.
/// Unsupported branches remain in the manifest and permanently retained source.
pub(crate) fn pending_selected_conversion_request(
    proof: &InspectedFullConversion,
    descriptor: &crate::sync::PartialReplicaDescriptor,
    attempt_id: String,
) -> Result<crate::sync::PartialMergeRequest, LixError> {
    let manifest = &proof.manifest;
    descriptor.validate(
        &manifest.repository_id,
        Some(&descriptor.selected_branch.branch_id),
    )?;
    if manifest.recoverable_uploads {
        return Err(unresolved(
            "unfinished local uploads require retained-source recovery",
        ));
    }
    let selected = &descriptor.selected_branch.branch_id;
    ordinary_pending_conversion_branches(proof, selected)?;
    for branch in &manifest.branches {
        if branch.pending_reset.is_some() || branch.retained_rows {
            return Err(unresolved(
                "reset or local-only rows require retained-source recovery",
            ));
        }
    }
    let branch = manifest
        .branches
        .iter()
        .find(|b| &b.branch_id == selected)
        .ok_or_else(|| unresolved("selected branch missing from source"))?;
    let base = branch
        .confirmed
        .as_ref()
        .ok_or_else(|| unresolved("selected branch has no confirmed authority baseline"))?;
    let local = branch
        .local
        .as_ref()
        .ok_or_else(|| unresolved("selected branch deletion is unresolved"))?;
    let global = manifest
        .branches
        .iter()
        .find(|b| b.branch_id == crate::GLOBAL_BRANCH_ID)
        .and_then(|b| b.local.as_ref())
        .ok_or_else(|| unresolved("global branch source is unavailable"))?;
    if global.head != descriptor.global_branch.head.commit_id
        || global.checkpoint != descriptor.global_branch.checkpoint.commit_id
        || base.checkpoint != local.checkpoint
        || base.checkpoint != descriptor.selected_branch.checkpoint.commit_id
    {
        return Err(unresolved(
            "global catalog or checkpoint changed during conversion",
        ));
    }
    let request = crate::sync::PartialMergeRequest {
        attempt_id,
        branch_id: selected.clone(),
        base_commit_id: base.head.clone(),
        captured_local_head_commit_id: local.head.clone(),
        expected_authority_head_commit_id: descriptor.selected_branch.head.commit_id.clone(),
        expected_authority_checkpoint_commit_id: base.checkpoint.clone(),
        captured_local_checkpoint_commit_id: base.checkpoint.clone(),
        checkpoint_commit_id: base.checkpoint.clone(),
        global_head_commit_id: global.head.clone(),
        global_checkpoint_commit_id: global.checkpoint.clone(),
    };
    request.validate()?;
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Child of repository::full_conversion_manifest tests.
    #[tokio::test]
    async fn every_pending_ordinary_branch_is_required_by_conversion() {
        let authority = crate::open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let selected = ConversionCoordinate {
            head: descriptor.selected_branch.head.commit_id.clone(),
            checkpoint: descriptor.selected_branch.checkpoint.commit_id.clone(),
        };
        let global = ConversionCoordinate {
            head: descriptor.global_branch.head.commit_id.clone(),
            checkpoint: descriptor.global_branch.checkpoint.commit_id.clone(),
        };
        let changed = |base: &ConversionCoordinate| ConversionCoordinate {
            head: uuid::Uuid::now_v7().to_string(),
            checkpoint: base.checkpoint.clone(),
        };
        let selected_branch = FullConversionBranch {
            branch_id: descriptor.selected_branch.branch_id.clone(),
            confirmed: Some(selected.clone()),
            known_deleted: false,
            local: Some(changed(&selected)),
            pending_reset: None,
            retained_rows: false,
        };
        let global_branch = FullConversionBranch {
            branch_id: crate::GLOBAL_BRANCH_ID.into(),
            confirmed: Some(global.clone()),
            known_deleted: false,
            local: Some(global),
            pending_reset: None,
            retained_rows: false,
        };
        let another = FullConversionBranch {
            branch_id: uuid::Uuid::now_v7().to_string(),
            confirmed: Some(selected.clone()),
            known_deleted: false,
            local: Some(changed(&selected)),
            pending_reset: None,
            retained_rows: false,
        };
        let proof = InspectedFullConversion {
            manifest: FullConversionManifest {
                repository_id: authority.lix_id().into(),
                account_id: authority.active_account_id().into(),
                branches: vec![selected_branch, global_branch, another],
                recoverable_uploads: false,
                source_replica_state_digest: [1; 32],
            },
        };
        let branches =
            ordinary_pending_conversion_branches(&proof, &descriptor.selected_branch.branch_id)
                .unwrap();
        assert_eq!(branches.len(), 2);
        assert_eq!(branches.last(), Some(&descriptor.selected_branch.branch_id));
        pending_selected_conversion_request(&proof, &descriptor, uuid::Uuid::now_v7().to_string())
            .unwrap();
    }
}

// repository::full_conversion_manifest addition. Consumes authenticated proof
// from the global owner; never mutates the original full replica receipt.
pub(crate) fn pending_conversion_request_after_global(
    source: &InspectedFullConversion,
    descriptor: &crate::sync::PartialReplicaDescriptor,
    attempt_id: String,
    proof: &crate::sync::ReconciledGlobalConversion,
) -> Result<crate::sync::PartialMergeRequest, LixError> {
    let manifest = source.manifest();
    let digest = *blake3::hash(
        &serde_json::to_vec(manifest).map_err(|_| unresolved("manifest encoding failed"))?,
    )
    .as_bytes();
    if proof.manifest_digest() != digest
        || proof.repository() != manifest.repository_id
        || proof.account() != manifest.account_id
    {
        return Err(unresolved(
            "global publication proof belongs to another frozen source",
        ));
    }
    let plan =
        classify_descriptor_global_conversion(source, &descriptor.selected_branch.branch_id)?;
    let request = &proof.receipt().request;
    if plan.global_base.head != request.base_commit_id
        || plan.global_local.head != request.captured_local_head_commit_id
        || plan.global_base.checkpoint != request.checkpoint_commit_id
        || plan.new_branches != request.new_branches
    {
        return Err(unresolved(
            "global outcome omits original source coordinates",
        ));
    }
    let branch = manifest
        .branches
        .iter()
        .find(|b| b.branch_id == descriptor.selected_branch.branch_id)
        .ok_or_else(|| unresolved("selected branch missing from source manifest"))?;
    let (Some(base), Some(local)) = (&branch.confirmed, &branch.local) else {
        return Err(unresolved(
            "remaining selected merge requires an existing authority base",
        ));
    };
    if descriptor.global_branch.checkpoint.commit_id != request.checkpoint_commit_id
        || descriptor.selected_branch.checkpoint.commit_id != base.checkpoint
        || local.checkpoint != base.checkpoint
    {
        return Err(unresolved(
            "checkpoint changed while global migration reconciled pending branches",
        ));
    }
    if descriptor.lix_id != manifest.repository_id {
        return Err(unresolved("migration descriptor repository changed"));
    }
    let result = crate::sync::PartialMergeRequest {
        attempt_id,
        branch_id: branch.branch_id.clone(),
        base_commit_id: base.head.clone(),
        expected_authority_head_commit_id: descriptor.selected_branch.head.commit_id.clone(),
        captured_local_head_commit_id: local.head.clone(),
        expected_authority_checkpoint_commit_id: base.checkpoint.clone(),
        captured_local_checkpoint_commit_id: base.checkpoint.clone(),
        checkpoint_commit_id: base.checkpoint.clone(),
        global_head_commit_id: descriptor.global_branch.head.commit_id.clone(),
        global_checkpoint_commit_id: descriptor.global_branch.checkpoint.commit_id.clone(),
    };
    result.validate()?;
    Ok(result)
}
