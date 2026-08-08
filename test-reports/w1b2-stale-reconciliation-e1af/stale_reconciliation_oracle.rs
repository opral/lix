//! W1b-2 standalone stale transaction/plugin/cohort model.
//!
//! Test/report-only: no Lix imports, storage access, actor invocation, or
//! runtime qualification. Future review may compile this file with
//! rustc --edition=2024 --test.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Value {
    Absent,
    Null,
    Tombstone,
    Json(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Proof {
    Valid {
        file_id: String,
        plugin_key: String,
        generation: String,
        revision: u64,
        change_id: String,
    },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted { claimed_file: String, actual_file: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RegistryProof {
    Valid {
        revision: u64,
        plugin_key: String,
        generation: String,
    },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted { claimed: String, actual: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ViewTrace {
    view_id: u64,
    begin_reads: u32,
    reader_instances: u32,
    events: Vec<(u64, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Snapshot {
    active_head: String,
    global_head: String,
    revision: u64,
    owners: BTreeMap<String, Proof>,
    registry: RegistryProof,
    changed_keys: BTreeSet<(String, String)>,
    idempotency_keys: BTreeSet<String>,
    view: ViewTrace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedWrite {
    operation_id: String,
    file_id: String,
    entity: String,
    value: Value,
    rank: (u64, String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Corruption {
    ReadView,
    OwnerProof,
    RegistryProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Conflict {
    GlobalOwnerOrSchemaChanged,
    BranchMissing,
    OwnerIdentityChanged,
    OverlapOutsideStablePlugin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Direct,
    UnrelatedOwnerSuccess,
    Idempotent,
    Reconciled { operation_id: String, value: Value },
}

fn valid_view(opening: &Snapshot, current: &Snapshot) -> Result<(), Corruption> {
    if opening.view.begin_reads != 1
        || current.view.begin_reads != 1
        || opening.view.reader_instances != 1
        || current.view.reader_instances != 1
        || opening.view.view_id != current.view.view_id
        || opening
            .view
            .events
            .iter()
            .chain(current.view.events.iter())
            .any(|(view_id, _)| *view_id != opening.view.view_id)
    {
        return Err(Corruption::ReadView);
    }
    Ok(())
}

fn valid_owner(proof: &Proof, file_id: &str) -> Result<(&str, &str), Corruption> {
    match proof {
        Proof::Valid {
            file_id: actual_file,
            plugin_key,
            generation,
            ..
        } if actual_file == file_id => Ok((plugin_key.as_str(), generation.as_str())),
        _ => Err(Corruption::OwnerProof),
    }
}

fn valid_registry(registry: &RegistryProof) -> Result<(&str, &str), Corruption> {
    match registry {
        RegistryProof::Valid {
            plugin_key,
            generation,
            ..
        } => Ok((plugin_key.as_str(), generation.as_str())),
        _ => Err(Corruption::RegistryProof),
    }
}

fn reconcile(
    opening: &Snapshot,
    current: &Snapshot,
    writes: &[PreparedWrite],
) -> Result<Result<Outcome, Conflict>, Corruption> {
    valid_view(opening, current)?;
    for write in writes {
        if current.idempotency_keys.contains(&write.operation_id) {
            return Ok(Ok(Outcome::Idempotent));
        }
    }
    if opening.active_head.is_empty() || current.active_head.is_empty() {
        return Ok(Err(Conflict::BranchMissing));
    }
    if opening.global_head != current.global_head {
        return Ok(Err(Conflict::GlobalOwnerOrSchemaChanged));
    }
    if opening.revision == current.revision {
        return Ok(Ok(Outcome::Direct));
    }

    let mut overlapping_files = BTreeSet::new();
    for write in writes {
        if current
            .changed_keys
            .contains(&(write.file_id.clone(), write.entity.clone()))
        {
            overlapping_files.insert(write.file_id.clone());
        }
    }
    if overlapping_files.is_empty() {
        return Ok(Ok(Outcome::UnrelatedOwnerSuccess));
    }

    for file_id in &overlapping_files {
        let opening_owner = opening
            .owners
            .get(file_id)
            .ok_or(Corruption::OwnerProof)?;
        let current_owner = current
            .owners
            .get(file_id)
            .ok_or(Corruption::OwnerProof)?;
        let (opening_plugin, opening_generation) = valid_owner(opening_owner, file_id)?;
        let (current_plugin, current_generation) = valid_owner(current_owner, file_id)?;
        if opening_plugin != current_plugin || opening_generation != current_generation {
            return Ok(Err(Conflict::OwnerIdentityChanged));
        }
        let (registry_plugin, registry_generation) = valid_registry(&current.registry)?;
        if registry_plugin != current_plugin || registry_generation != current_generation {
            return Ok(Err(Conflict::OwnerIdentityChanged));
        }
    }

    let write = writes.first().ok_or(Corruption::OwnerProof)?;
    Ok(Ok(Outcome::Reconciled {
        operation_id: write.operation_id.clone(),
        value: write.value.clone(),
    }))
}

fn proof(file_id: &str, plugin_key: &str, generation: &str, revision: u64) -> Proof {
    Proof::Valid {
        file_id: file_id.into(),
        plugin_key: plugin_key.into(),
        generation: generation.into(),
        revision,
        change_id: format!("change-{revision}"),
    }
}

fn registry(plugin_key: &str, generation: &str, revision: u64) -> RegistryProof {
    RegistryProof::Valid {
        revision,
        plugin_key: plugin_key.into(),
        generation: generation.into(),
    }
}

fn snapshot(revision: u64) -> Snapshot {
    Snapshot {
        active_head: "branch-head".into(),
        global_head: "global-head".into(),
        revision,
        owners: BTreeMap::from([(
            "file-a".into(),
            proof("file-a", "plugin-a", "generation-a", revision),
        )]),
        registry: registry("plugin-a", "generation-a", revision),
        changed_keys: BTreeSet::new(),
        idempotency_keys: BTreeSet::new(),
        view: ViewTrace {
            view_id: 11,
            begin_reads: 1,
            reader_instances: 1,
            events: vec![(11, "selector/catalog/owner/registry/state".into())],
        },
    }
}

fn write(operation_id: &str, file_id: &str, entity: &str, value: Value) -> PreparedWrite {
    PreparedWrite {
        operation_id: operation_id.into(),
        file_id: file_id.into(),
        entity: entity.into(),
        value,
        rank: (1, operation_id.into()),
    }
}

#[test]
fn unrelated_owner_change_succeeds_without_reconciliation() {
    let opening = snapshot(1);
    let mut current = snapshot(2);
    current.changed_keys.insert(("other-file".into(), "row".into()));
    let result = reconcile(
        &opening,
        &current,
        &[write("op-a", "file-a", "row", Value::Json("a".into()))],
    )
    .expect("valid authority");
    assert_eq!(result, Ok(Outcome::UnrelatedOwnerSuccess));
}

#[test]
fn same_owner_stale_change_is_deterministically_reconciled() {
    let opening = snapshot(1);
    let mut current = snapshot(2);
    current.changed_keys.insert(("file-a".into(), "row".into()));
    let result = reconcile(
        &opening,
        &current,
        &[write("op-a", "file-a", "row", Value::Json("a".into()))],
    )
    .expect("valid authority");
    assert_eq!(
        result,
        Ok(Outcome::Reconciled {
            operation_id: "op-a".into(),
            value: Value::Json("a".into()),
        })
    );
}

#[test]
fn owner_generation_or_registry_substitution_conflicts() {
    let opening = snapshot(1);
    let mut current = snapshot(2);
    current.changed_keys.insert(("file-a".into(), "row".into()));
    current.owners.insert(
        "file-a".into(),
        proof("file-a", "plugin-a", "generation-b", 2),
    );
    let result = reconcile(
        &opening,
        &current,
        &[write("op-a", "file-a", "row", Value::Null)],
    )
    .expect("valid authority");
    assert_eq!(result, Ok(Err(Conflict::OwnerIdentityChanged)));
}

#[test]
fn idempotency_is_a_terminal_success_without_duplicate_write() {
    let opening = snapshot(1);
    let mut current = snapshot(2);
    current.idempotency_keys.insert("op-a".into());
    let result = reconcile(
        &opening,
        &current,
        &[write("op-a", "file-a", "row", Value::Tombstone)],
    )
    .expect("valid authority");
    assert_eq!(result, Ok(Outcome::Idempotent));
}

#[test]
fn missing_malformed_wrong_kind_and_identity_proofs_fail_closed() {
    let opening = snapshot(1);
    for bad in [
        Proof::Missing,
        Proof::Malformed,
        Proof::WrongKind,
        Proof::IdentitySubstituted {
            claimed_file: "file-a".into(),
            actual_file: "file-b".into(),
        },
    ] {
        let mut current = snapshot(2);
        current.changed_keys.insert(("file-a".into(), "row".into()));
        current.owners.insert("file-a".into(), bad);
        assert_eq!(
            reconcile(
                &opening,
                &current,
                &[write("op-a", "file-a", "row", Value::Absent)],
            ),
            Err(Corruption::OwnerProof)
        );
    }
}

#[test]
fn separate_reader_or_cross_view_events_fail_closed() {
    let opening = snapshot(1);
    let mut current = snapshot(2);
    current.view.view_id = 12;
    current.changed_keys.insert(("other-file".into(), "row".into()));
    assert_eq!(
        reconcile(&opening, &current, &[]),
        Err(Corruption::ReadView)
    );
}
