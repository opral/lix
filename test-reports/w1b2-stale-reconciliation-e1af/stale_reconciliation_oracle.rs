// W1b-2 stateful stale transaction/plugin/cohort correction model.
//
// Test/report-only: it has no Lix imports, storage access, actor invocation,
// or adapter runtime. The model deliberately makes the read/identity and
// complete-plan invariants executable before any idempotency replay result.

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
        selector_id: String,
        commit_id: String,
    },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted {
        claimed_file: String,
        actual_file: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RegistryProof {
    Valid {
        revision: u64,
        change_id: String,
        plugin_key: String,
        generation: String,
        selector_id: String,
        commit_id: String,
    },
    Missing,
    Malformed,
    WrongKind,
    IdentitySubstituted {
        claimed: String,
        actual: String,
    },
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
    selector_id: String,
    commit_id: String,
    revision: u64,
    owners: BTreeMap<String, Proof>,
    registry: RegistryProof,
    changed_keys: BTreeSet<(String, String)>,
    idempotency: BTreeMap<String, String>,
    view: ViewTrace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedWrite {
    operation_id: String,
    file_id: String,
    entity: String,
    value: Value,
    rank: (u64, String),
    base_revision: u64,
    base_change_id: String,
    base_selector_id: String,
    base_commit_id: String,
}

impl PreparedWrite {
    fn fingerprint(&self) -> String {
        format!("{}|{}|{:?}", self.file_id, self.entity, self.value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Corruption {
    ReadView,
    OwnerProof,
    RegistryProof,
    IdempotencyProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Conflict {
    GlobalOwnerOrSchemaChanged,
    BranchMissing,
    OwnerIdentityChanged,
    IdempotencyMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Direct,
    UnrelatedOwnerSuccess,
    Idempotent,
    Reconciled { operation_id: String, value: Value },
}

fn valid_view(opening: &Snapshot, current: &Snapshot) -> Result<(), Corruption> {
    let total_begin_reads = opening.view.begin_reads + current.view.begin_reads;
    let total_reader_instances = opening.view.reader_instances + current.view.reader_instances;
    if opening.view.begin_reads != 1
        || current.view.begin_reads != 0
        || opening.view.reader_instances != 1
        || current.view.reader_instances != 0
        || total_begin_reads != 1
        || total_reader_instances != 1
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

fn valid_owner<'a>(
    proof: &'a Proof,
    file_id: &str,
    expected_revision: u64,
    expected_change_id: &str,
    expected_selector_id: &str,
    expected_commit_id: &str,
) -> Result<(&'a str, &'a str), Corruption> {
    match proof {
        Proof::Valid {
            file_id: actual_file,
            plugin_key,
            generation,
            revision,
            change_id,
            selector_id,
            commit_id,
        } if actual_file == file_id
            && *revision == expected_revision
            && change_id == expected_change_id
            && selector_id == expected_selector_id
            && commit_id == expected_commit_id =>
        {
            Ok((plugin_key.as_str(), generation.as_str()))
        }
        _ => Err(Corruption::OwnerProof),
    }
}

fn valid_registry<'a>(
    registry: &'a RegistryProof,
    expected_revision: u64,
    expected_change_id: &str,
    expected_selector_id: &str,
    expected_commit_id: &str,
) -> Result<(&'a str, &'a str), Corruption> {
    match registry {
        RegistryProof::Valid {
            revision,
            change_id,
            plugin_key,
            generation,
            selector_id,
            commit_id,
        } if *revision == expected_revision
            && change_id == expected_change_id
            && selector_id == expected_selector_id
            && commit_id == expected_commit_id =>
        {
            Ok((plugin_key.as_str(), generation.as_str()))
        }
        _ => Err(Corruption::RegistryProof),
    }
}

fn authenticate_write(
    opening: &Snapshot,
    current: &Snapshot,
    write: &PreparedWrite,
) -> Result<(), Corruption> {
    let expected_opening_change = format!("change-{}", opening.revision);
    let expected_current_change = format!("change-{}", current.revision);
    let expected_opening_commit = format!("commit-{}", opening.revision);
    let expected_current_commit = format!("commit-{}", current.revision);
    if opening.selector_id != "selector-1"
        || current.selector_id != opening.selector_id
        || opening.commit_id != expected_opening_commit
        || current.commit_id != expected_current_commit
    {
        return Err(Corruption::RegistryProof);
    }
    if write.base_revision != opening.revision
        || write.base_change_id != expected_opening_change
        || write.base_selector_id != opening.selector_id
        || write.base_commit_id != opening.commit_id
    {
        return Err(Corruption::OwnerProof);
    }
    let opening_owner = opening
        .owners
        .get(&write.file_id)
        .ok_or(Corruption::OwnerProof)?;
    let current_owner = current
        .owners
        .get(&write.file_id)
        .ok_or(Corruption::OwnerProof)?;
    valid_owner(
        opening_owner,
        &write.file_id,
        opening.revision,
        &expected_opening_change,
        &opening.selector_id,
        &opening.commit_id,
    )?;
    let (opening_plugin, opening_generation) = valid_registry(
        &opening.registry,
        opening.revision,
        &expected_opening_change,
        &opening.selector_id,
        &opening.commit_id,
    )?;
    let (owner_plugin, owner_generation) = valid_owner(
        opening_owner,
        &write.file_id,
        opening.revision,
        &expected_opening_change,
        &opening.selector_id,
        &opening.commit_id,
    )?;
    if opening_plugin != owner_plugin || opening_generation != owner_generation {
        return Err(Corruption::RegistryProof);
    }
    if current.selector_id != opening.selector_id {
        return Err(Corruption::RegistryProof);
    }
    valid_owner(
        current_owner,
        &write.file_id,
        current.revision,
        &expected_current_change,
        &current.selector_id,
        &current.commit_id,
    )?;
    let (current_plugin, current_generation) = valid_registry(
        &current.registry,
        current.revision,
        &expected_current_change,
        &current.selector_id,
        &current.commit_id,
    )?;
    let (owner_plugin, owner_generation) = valid_owner(
        current_owner,
        &write.file_id,
        current.revision,
        &expected_current_change,
        &current.selector_id,
        &current.commit_id,
    )?;
    if current_plugin != owner_plugin || current_generation != owner_generation {
        return Err(Corruption::RegistryProof);
    }
    Ok(())
}

fn reconcile(
    opening: &Snapshot,
    current: &Snapshot,
    writes: &[PreparedWrite],
) -> Result<Result<Outcome, Conflict>, Corruption> {
    valid_view(opening, current)?;
    if opening.active_head.is_empty() || current.active_head.is_empty() {
        return Ok(Err(Conflict::BranchMissing));
    }
    if opening.global_head != current.global_head {
        return Ok(Err(Conflict::GlobalOwnerOrSchemaChanged));
    }

    let mut ordered = writes.to_vec();
    ordered.sort_by_key(|write| write.rank.clone());
    for write in &ordered {
        authenticate_write(opening, current, write)?;
    }
    let mut idempotent_count = 0;
    let mut missing_count = 0;
    for write in &ordered {
        if let Some(existing) = current.idempotency.get(&write.operation_id) {
            if existing == &write.fingerprint() {
                idempotent_count += 1;
                continue;
            }
            return Ok(Err(Conflict::IdempotencyMismatch));
        } else {
            missing_count += 1;
        }
    }
    if idempotent_count > 0 && missing_count > 0 {
        return Ok(Err(Conflict::IdempotencyMismatch));
    }
    if idempotent_count == ordered.len() {
        return Ok(Ok(Outcome::Idempotent));
    }
    if opening.revision == current.revision {
        return Ok(Ok(Outcome::Direct));
    }

    let mut overlapping_files = BTreeSet::new();
    for write in &ordered {
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
        let opening_owner = opening.owners.get(file_id).ok_or(Corruption::OwnerProof)?;
        let current_owner = current.owners.get(file_id).ok_or(Corruption::OwnerProof)?;
        let (opening_plugin, opening_generation) = valid_owner(
            opening_owner,
            file_id,
            opening.revision,
            &format!("change-{}", opening.revision),
            &opening.selector_id,
            &opening.commit_id,
        )?;
        let (current_plugin, current_generation) = valid_owner(
            current_owner,
            file_id,
            current.revision,
            &format!("change-{}", current.revision),
            &current.selector_id,
            &current.commit_id,
        )?;
        if opening_plugin != current_plugin || opening_generation != current_generation {
            return Ok(Err(Conflict::OwnerIdentityChanged));
        }
        let (registry_plugin, registry_generation) = valid_registry(
            &current.registry,
            current.revision,
            &format!("change-{}", current.revision),
            &current.selector_id,
            &current.commit_id,
        )?;
        if registry_plugin != current_plugin || registry_generation != current_generation {
            return Ok(Err(Conflict::OwnerIdentityChanged));
        }
    }

    let write = ordered.first().ok_or(Corruption::IdempotencyProof)?;
    Ok(Ok(Outcome::Reconciled {
        operation_id: write.operation_id.clone(),
        value: write.value.clone(),
    }))
}

fn proof(
    file_id: &str,
    plugin_key: &str,
    generation: &str,
    revision: u64,
    selector_id: &str,
    commit_id: &str,
) -> Proof {
    Proof::Valid {
        file_id: file_id.into(),
        plugin_key: plugin_key.into(),
        generation: generation.into(),
        revision,
        change_id: format!("change-{revision}"),
        selector_id: selector_id.into(),
        commit_id: commit_id.into(),
    }
}

fn registry(
    plugin_key: &str,
    generation: &str,
    revision: u64,
    selector_id: &str,
    commit_id: &str,
) -> RegistryProof {
    RegistryProof::Valid {
        revision,
        change_id: format!("change-{revision}"),
        plugin_key: plugin_key.into(),
        generation: generation.into(),
        selector_id: selector_id.into(),
        commit_id: commit_id.into(),
    }
}

fn snapshot(revision: u64) -> Snapshot {
    let commit_id = format!("commit-{revision}");
    Snapshot {
        active_head: "branch-head".into(),
        global_head: "global-head".into(),
        selector_id: "selector-1".into(),
        commit_id: commit_id.clone(),
        revision,
        owners: BTreeMap::from([(
            "file-a".into(),
            proof(
                "file-a",
                "plugin-a",
                "generation-a",
                revision,
                "selector-1",
                &commit_id,
            ),
        )]),
        registry: registry(
            "plugin-a",
            "generation-a",
            revision,
            "selector-1",
            &commit_id,
        ),
        changed_keys: BTreeSet::new(),
        idempotency: BTreeMap::new(),
        view: ViewTrace {
            view_id: 11,
            begin_reads: 1,
            reader_instances: 1,
            events: vec![(11, "selector/catalog/owner/registry/state".into())],
        },
    }
}

fn current_snapshot(revision: u64) -> Snapshot {
    let mut current = snapshot(revision);
    current.view.begin_reads = 0;
    current.view.reader_instances = 0;
    current.view.events = vec![(11, "same-retained-read/current-observation".into())];
    current
}

fn write(operation_id: &str, file_id: &str, entity: &str, value: Value) -> PreparedWrite {
    write_with_rank(operation_id, file_id, entity, value, 1)
}

fn write_with_rank(
    operation_id: &str,
    file_id: &str,
    entity: &str,
    value: Value,
    rank: u64,
) -> PreparedWrite {
    PreparedWrite {
        operation_id: operation_id.into(),
        file_id: file_id.into(),
        entity: entity.into(),
        value,
        rank: (rank, operation_id.into()),
        base_revision: 1,
        base_change_id: "change-1".into(),
        base_selector_id: "selector-1".into(),
        base_commit_id: "commit-1".into(),
    }
}

#[test]
fn unrelated_owner_change_succeeds_without_reconciliation() {
    let opening = snapshot(1);
    let mut current = current_snapshot(2);
    current
        .changed_keys
        .insert(("other-file".into(), "row".into()));
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
    let mut current = current_snapshot(2);
    current.changed_keys.insert(("file-a".into(), "row".into()));
    let before = current.clone();
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
    assert_eq!(current, before);
}

#[test]
fn owner_generation_or_registry_substitution_conflicts() {
    let opening = snapshot(1);
    let mut current = current_snapshot(2);
    current.changed_keys.insert(("file-a".into(), "row".into()));
    current.owners.insert(
        "file-a".into(),
        proof(
            "file-a",
            "plugin-a",
            "generation-b",
            2,
            "selector-1",
            "commit-2",
        ),
    );
    let result = reconcile(
        &opening,
        &current,
        &[write("op-a", "file-a", "row", Value::Null)],
    );
    assert_eq!(result, Err(Corruption::RegistryProof));
}

#[test]
fn idempotency_is_exact_replay_and_mismatch_is_conflict() {
    let opening = snapshot(1);
    let write_a = write("op-a", "file-a", "row", Value::Tombstone);
    let mut current = current_snapshot(2);
    current
        .idempotency
        .insert("op-a".into(), write_a.fingerprint());
    assert_eq!(
        reconcile(&opening, &current, std::slice::from_ref(&write_a)),
        Ok(Ok(Outcome::Idempotent))
    );
    let write_b = write("op-a", "file-a", "row", Value::Json("different".into()));
    assert_eq!(
        reconcile(&opening, &current, std::slice::from_ref(&write_b)),
        Ok(Err(Conflict::IdempotencyMismatch))
    );
}

#[test]
fn multi_write_idempotency_checks_every_operation_before_replay() {
    let opening = snapshot(1);
    let low = write_with_rank("op-low", "file-a", "row-a", Value::Null, 1);
    let high = write_with_rank("op-high", "file-a", "row-b", Value::Json("high".into()), 2);
    let mut current = current_snapshot(2);
    current
        .idempotency
        .insert("op-low".into(), low.fingerprint());
    current
        .idempotency
        .insert("op-high".into(), "different-payload".into());
    assert_eq!(
        reconcile(&opening, &current, &[high.clone(), low.clone()]),
        Ok(Err(Conflict::IdempotencyMismatch))
    );

    let mut all_match = current_snapshot(2);
    all_match
        .idempotency
        .insert("op-low".into(), low.fingerprint());
    all_match
        .idempotency
        .insert("op-high".into(), high.fingerprint());
    assert_eq!(
        reconcile(&opening, &all_match, &[low, high]),
        Ok(Ok(Outcome::Idempotent))
    );
}

#[test]
fn selector_and_commit_identity_substitution_fails_before_reconciliation() {
    let opening = snapshot(1);
    let write = write("op-a", "file-a", "row", Value::Json("a".into()));
    let mut selector_forged = current_snapshot(2);
    selector_forged.selector_id = "selector-forged".into();
    assert_eq!(
        reconcile(&opening, &selector_forged, std::slice::from_ref(&write)),
        Err(Corruption::RegistryProof)
    );

    let mut commit_forged = current_snapshot(2);
    commit_forged.commit_id = "commit-forged".into();
    assert_eq!(
        reconcile(&opening, &commit_forged, std::slice::from_ref(&write)),
        Err(Corruption::RegistryProof)
    );
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
        let mut current = current_snapshot(2);
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
fn missing_malformed_wrong_kind_and_identity_registry_fails_closed() {
    let opening = snapshot(1);
    for bad in [
        RegistryProof::Missing,
        RegistryProof::Malformed,
        RegistryProof::WrongKind,
        RegistryProof::IdentitySubstituted {
            claimed: "plugin-a".into(),
            actual: "plugin-b".into(),
        },
    ] {
        let mut current = current_snapshot(2);
        current.changed_keys.insert(("file-a".into(), "row".into()));
        current.registry = bad;
        assert_eq!(
            reconcile(
                &opening,
                &current,
                &[write("op-a", "file-a", "row", Value::Json("a".into()))],
            ),
            Err(Corruption::RegistryProof)
        );
    }
}

#[test]
fn corruption_is_authenticated_before_idempotency_replay() {
    let opening = snapshot(1);
    let write_a = write("op-a", "file-a", "row", Value::Tombstone);
    let mut current = current_snapshot(2);
    current
        .idempotency
        .insert("op-a".into(), write_a.fingerprint());
    current.owners.insert("file-a".into(), Proof::Malformed);
    assert_eq!(
        reconcile(&opening, &current, &[write_a]),
        Err(Corruption::OwnerProof)
    );
}

#[test]
fn multi_write_order_is_deterministic_and_has_no_partial_publication() {
    let opening = snapshot(1);
    let mut current = current_snapshot(2);
    current.changed_keys.insert(("file-a".into(), "row".into()));
    let low = write_with_rank("op-low", "file-a", "row", Value::Null, 1);
    let high = write_with_rank("op-high", "file-a", "row", Value::Json("high".into()), 2);
    let before = current.clone();
    let forward = reconcile(&opening, &current, &[high.clone(), low.clone()]);
    let reverse = reconcile(&opening, &current, &[low, high]);
    assert_eq!(forward, reverse);
    assert_eq!(
        forward,
        Ok(Ok(Outcome::Reconciled {
            operation_id: "op-low".into(),
            value: Value::Null,
        }))
    );
    assert_eq!(current, before);
}

#[test]
fn separate_reader_or_cross_view_events_fail_closed() {
    let opening = snapshot(1);
    let mut current = current_snapshot(2);
    current.view.view_id = 12;
    current
        .changed_keys
        .insert(("other-file".into(), "row".into()));
    assert_eq!(
        reconcile(&opening, &current, &[]),
        Err(Corruption::ReadView)
    );
}
