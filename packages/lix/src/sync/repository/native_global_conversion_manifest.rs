//! Repository-owned classification before explicit migration performs network I/O.
use super::full_conversion_manifest::{ConversionCoordinate, InspectedFullConversion};
use crate::sync::native_global_migration_protocol::NativeNewBranchCoordinate;
use crate::{GLOBAL_BRANCH_ID, LixError};
use std::collections::BTreeSet;
pub(crate) struct DescriptorGlobalConversion {
    pub global_base: ConversionCoordinate,
    pub global_local: ConversionCoordinate,
    pub new_branches: Vec<NativeNewBranchCoordinate>,
    pub existing_dirty_branches: Vec<String>,
}
fn unsupported(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_CONVERSION_UNRESOLVED", message)
        .with_details(serde_json::json!({"sourcePreserved":true}))
}
pub(crate) fn classify_descriptor_global_conversion(
    source: &InspectedFullConversion,
    selected: &str,
) -> Result<DescriptorGlobalConversion, LixError> {
    let manifest = source.manifest();
    if manifest.recoverable_uploads {
        return Err(unsupported(
            "unfinished old upload must be resolved before global migration",
        ));
    }
    let global = manifest
        .branches
        .iter()
        .find(|b| b.branch_id == GLOBAL_BRANCH_ID)
        .ok_or_else(|| unsupported("source global coordinate absent"))?;
    let (Some(base), Some(local)) = (&global.confirmed, &global.local) else {
        return Err(unsupported(
            "global migration requires a known authority base",
        ));
    };
    if base.checkpoint != local.checkpoint || base.head == local.head {
        return Err(unsupported(
            "descriptor migration requires changed global head and unchanged checkpoint",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut new_branches = Vec::new();
    let mut existing_dirty_branches = Vec::new();
    for branch in &manifest.branches {
        if !ids.insert(branch.branch_id.clone())
            || branch.pending_reset.is_some()
            || branch.retained_rows
        {
            return Err(unsupported(
                "duplicate branch, reset or retained untracked rows require separate native reconciliation",
            ));
        }
        if branch.branch_id == GLOBAL_BRANCH_ID {
            continue;
        }
        match (&branch.confirmed, &branch.local) {
            (Some(b), Some(l)) => {
                if b.checkpoint != l.checkpoint {
                    return Err(unsupported(
                        "pending branch checkpoint changes require separate reconciliation",
                    ));
                }
                if b.head != l.head {
                    existing_dirty_branches.push(branch.branch_id.clone());
                }
            }
            (None, Some(l)) if !branch.known_deleted => {
                new_branches.push(NativeNewBranchCoordinate {
                    branch_id: branch.branch_id.clone(),
                    head_commit_id: l.head.clone(),
                    checkpoint_commit_id: l.checkpoint.clone(),
                })
            }
            (None, None) if branch.known_deleted => {}
            _ => {
                return Err(unsupported(
                    "branch deletion or deleted identity resurrection requires separate reconciliation",
                ));
            }
        }
    }
    if !ids.contains(selected) || new_branches.is_empty() {
        return Err(unsupported(
            "selected branch absent or no fresh branch exists",
        ));
    }
    new_branches.sort_by(|a, b| a.branch_id.cmp(&b.branch_id));
    existing_dirty_branches.sort_by(|a, b| (a == selected, a).cmp(&(b == selected, b)));
    Ok(DescriptorGlobalConversion {
        global_base: base.clone(),
        global_local: local.clone(),
        new_branches,
        existing_dirty_branches,
    })
}
