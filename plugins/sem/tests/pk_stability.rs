#[allow(dead_code)]
mod common;

use lix_sdk::{FsWriteOptions, Value};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[tokio::test]
async fn body_edit_preserves_entity_pk() {
    let before = r#"pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}
"#;
    let after = r#"pub fn greeting(name: &str) -> String {
    format!("Hi, {name}!")
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/body-edit.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "greeting",
        &trace.after_entities,
        "function",
        "greeting",
    );
}

#[tokio::test]
async fn gap_edit_preserves_entity_pk() {
    let before = r#"// Original module comment.

pub fn value() -> i32 {
    1
}
"#;
    let after = r#"// Updated module comment.
// New gap text should not perturb the function identity.

pub fn value() -> i32 {
    1
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/gap-edit.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "value",
        &trace.after_entities,
        "function",
        "value",
    );
}

#[tokio::test]
async fn rename_preserves_entity_pk() {
    let before = r#"pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}
"#;
    let after = r#"pub fn greet(name: &str) -> String {
    format!("Hello, {name}!")
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/rename.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "greeting",
        &trace.after_entities,
        "function",
        "greet",
    );
}

#[tokio::test]
async fn rename_and_body_edit_preserves_entity_pk() {
    let before = r#"pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}
"#;
    let after = r#"pub fn greet(name: &str) -> String {
    format!("Hi, {name}!")
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/rename-body-edit.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "greeting",
        &trace.after_entities,
        "function",
        "greet",
    );
    let before = entity_by_type_and_name(&trace.before_entities, "function", "greeting");
    let after = entity_by_type_and_name(&trace.after_entities, "function", "greet");
    assert_no_pk_churn(&trace.update_changes, &before.pk, &after.pk);
}

#[tokio::test]
async fn helper_extraction_preserves_original_function_pk() {
    let before = r#"use std::fmt;

pub fn plan_file_path_write(path: &str, byte_count: usize) -> String {
    let directory = path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    format!("{directory}:{}", fmt_byte_count(byte_count))
}

fn fmt_byte_count(byte_count: usize) -> impl fmt::Display {
    byte_count
}
"#;
    let after = r#"use std::fmt;

pub fn plan_file_path_write(path: &str, byte_count: usize) -> String {
    let directory = directory_for_path(path);
    format!("{directory}:{}", fmt_byte_count(byte_count))
}

fn fmt_byte_count(byte_count: usize) -> impl fmt::Display {
    byte_count
}

fn directory_for_path(path: &str) -> &str {
    path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/helper-extraction.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "plan_file_path_write",
        &trace.after_entities,
        "function",
        "plan_file_path_write",
    );
    assert_entity_exists(&trace.after_entities, "function", "directory_for_path");
}

#[tokio::test]
async fn inline_helper_tombstones_removed_and_preserves_caller_pk() {
    let before = r#"use std::fmt;

pub fn plan_file_path_write(path: &str, byte_count: usize) -> String {
    let directory = directory_for_path(path);
    format!("{directory}:{}", fmt_byte_count(byte_count))
}

fn fmt_byte_count(byte_count: usize) -> impl fmt::Display {
    byte_count
}

fn directory_for_path(path: &str) -> &str {
    path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("")
}
"#;
    let after = r#"use std::fmt;

pub fn plan_file_path_write(path: &str, byte_count: usize) -> String {
    let directory = path.rsplit_once('/').map(|(dir, _)| dir).unwrap_or("");
    format!("{directory}:{}", fmt_byte_count(byte_count))
}

fn fmt_byte_count(byte_count: usize) -> impl fmt::Display {
    byte_count
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/inline-helper.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "plan_file_path_write",
        &trace.after_entities,
        "function",
        "plan_file_path_write",
    );
    let helper = entity_by_type_and_name(&trace.before_entities, "function", "directory_for_path");
    assert_tombstoned(&trace.update_changes, &helper.pk);
    assert_entity_missing(&trace.after_entities, "function", "directory_for_path");
}

#[tokio::test]
async fn adding_sibling_preserves_existing_entity_pk() {
    let before = r#"pub fn first() -> &'static str {
    "first"
}
"#;
    let after = r#"pub fn first() -> &'static str {
    "first"
}

pub fn second() -> &'static str {
    "second"
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/add-sibling.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "first",
        &trace.after_entities,
        "function",
        "first",
    );
    assert_entity_exists(&trace.after_entities, "function", "second");
}

#[tokio::test]
async fn deleting_sibling_preserves_remaining_entity_pk() {
    let before = r#"pub fn first() -> &'static str {
    "first"
}

pub fn second() -> &'static str {
    "second"
}
"#;
    let after = r#"pub fn first() -> &'static str {
    "first"
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/delete-sibling.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "first",
        &trace.after_entities,
        "function",
        "first",
    );
    assert_entity_missing(&trace.after_entities, "function", "second");
}

#[tokio::test]
async fn reorder_preserves_entity_pk() {
    let before = r#"pub fn first() -> &'static str {
    "first"
}

pub fn second() -> &'static str {
    "second"
}
"#;
    let after = r#"pub fn second() -> &'static str {
    "second"
}

pub fn first() -> &'static str {
    "first"
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/reorder.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "first",
        &trace.after_entities,
        "function",
        "first",
    );
    assert_same_pk(
        &trace.before_entities,
        "function",
        "second",
        &trace.after_entities,
        "function",
        "second",
    );
}

#[tokio::test]
async fn parent_rename_preserves_child_entity_pk() {
    let before = r#"pub struct Greeter {
    name: String,
}

impl Greeter {
    pub fn label(&self) -> String {
        format!("Hello, {}", self.name)
    }
}
"#;
    let after = r#"pub struct Presenter {
    name: String,
}

impl Presenter {
    pub fn label(&self) -> String {
        format!("Hello, {}", self.name)
    }
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/parent-rename.rs", before, after).await;
    assert_same_pk(
        &trace.before_entities,
        "function",
        "label",
        &trace.after_entities,
        "function",
        "label",
    );
}

#[tokio::test]
async fn replacing_entity_with_different_entity_gets_new_pk() {
    let before = r#"pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}
"#;
    let after = r#"pub fn calculate(left: i32, right: i32) -> i32 {
    left + right
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/replacement.rs", before, after).await;
    let before_entity = entity_by_type_and_name(&trace.before_entities, "function", "greeting");
    let after_entity = entity_by_type_and_name(&trace.after_entities, "function", "calculate");
    assert_ne!(
        before_entity.pk, after_entity.pk,
        "unrelated replacement should receive a new entity_pk"
    );
    assert_entity_missing(&trace.after_entities, "function", "greeting");
}

#[tokio::test]
async fn same_name_large_rewrite_gets_new_pk() {
    let before = r#"use std::fmt;

pub fn reconcile_file_data(path: &str) -> bool {
    path.ends_with(".csv")
}
"#;
    let after = r#"use std::fmt;

pub struct Row {
    value: String,
}

pub struct Plan {
    values: Vec<String>,
}

pub fn reconcile_file_data(rows: Vec<Row>) -> Result<Plan, String> {
    let mut values = Vec::new();
    for row in rows {
        values.push(row.value.trim().to_lowercase());
    }
    let _display = format!("{}", values.len());
    Ok(Plan { values })
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/large-rewrite.rs", before, after).await;
    let before = entity_by_type_and_name(&trace.before_entities, "function", "reconcile_file_data");
    let after = entity_by_type_and_name(&trace.after_entities, "function", "reconcile_file_data");
    assert_ne!(
        before.pk, after.pk,
        "same-name replacement with a different semantic shape should receive a new entity_pk"
    );
    assert_tombstoned(&trace.update_changes, &before.pk);
}

#[tokio::test]
async fn duplicate_names_do_not_cross_match() {
    let before = r#"pub mod csv {
    pub fn render() -> &'static str {
        "csv"
    }
}

pub mod json {
    pub fn render() -> &'static str {
        "json"
    }
}
"#;
    let after = r#"pub mod csv {
    pub fn render() -> &'static str {
        "csv-v2"
    }
}

pub mod json {
    pub fn render() -> &'static str {
        "json"
    }
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/duplicate-names.rs", before, after).await;
    assert_same_content_pk(
        &trace.before_entities,
        "function",
        "\"csv\"",
        &trace.after_entities,
        "function",
        "\"csv-v2\"",
    );
    assert_same_content_pk(
        &trace.before_entities,
        "function",
        "\"json\"",
        &trace.after_entities,
        "function",
        "\"json\"",
    );
}

#[tokio::test]
async fn ambiguous_rename_does_not_reuse_pk() {
    let before = r#"pub fn read_user(id: i64) -> String {
    format!("user:{id}")
}

pub fn read_account(id: i64) -> String {
    format!("account:{id}")
}
"#;
    let after = r#"pub fn read_record(id: i64) -> String {
    format!("record:{id}")
}
"#;

    let lix = shared_lix().await;
    let trace = write_before_after(&lix, "/src/ambiguous-rename.rs", before, after).await;
    let read_user = entity_by_type_and_name(&trace.before_entities, "function", "read_user");
    let read_account = entity_by_type_and_name(&trace.before_entities, "function", "read_account");
    let read_record = entity_by_type_and_name(&trace.after_entities, "function", "read_record");
    assert_ne!(
        read_user.pk, read_record.pk,
        "ambiguous rename should not reuse read_user's entity_pk"
    );
    assert_ne!(
        read_account.pk, read_record.pk,
        "ambiguous rename should not reuse read_account's entity_pk"
    );
}

async fn shared_lix() -> lix_sdk::Lix {
    common::open_lix_with_sem_plugin().await
}

async fn write_before_after(
    lix: &lix_sdk::Lix,
    path: &str,
    before: &str,
    after: &str,
) -> WriteTrace {
    lix.write_file(path, before.as_bytes().to_vec(), FsWriteOptions::default())
        .await
        .unwrap();

    let file_id = common::file_id_for_path(&lix, path).await;

    let before_entities = current_sem_entities(&lix, &file_id).await;

    let before_change_count = sem_change_count(&lix, &file_id).await;

    lix.write_file(path, after.as_bytes().to_vec(), FsWriteOptions::default())
        .await
        .unwrap();

    let after_entities = current_sem_entities(&lix, &file_id).await;

    let update_changes = sem_changes_since(&lix, &file_id, before_change_count).await;
    assert_batch_has_unique_pks(&update_changes);

    WriteTrace {
        before_entities,
        after_entities,
        update_changes,
    }
}

#[derive(Debug, Clone)]
struct WriteTrace {
    before_entities: Vec<SemEntityState>,
    after_entities: Vec<SemEntityState>,
    update_changes: Vec<SemChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SemEntityState {
    pk: String,
    snapshot: SemSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SemChange {
    pk: String,
    snapshot: Option<SemSnapshot>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct SemSnapshot {
    id: String,
    entity_type: String,
    entity_name: String,
    file_path: String,
    line: usize,
    end_line: Option<usize>,
    content: Option<String>,
}

async fn current_sem_entities(lix: &lix_sdk::Lix, file_id: &str) -> Vec<SemEntityState> {
    let rows = lix
        .execute(
            "SELECT entity_pk, snapshot_content \
             FROM lix_state \
             WHERE file_id = $1 AND schema_key = 'sem_entity' \
             ORDER BY entity_pk",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();

    let mut current = BTreeMap::<String, Option<SemSnapshot>>::new();
    for row in rows.rows() {
        let pk = single_entity_pk(row.get::<serde_json::Value>("entity_pk").unwrap());
        let snapshot = match row.value("snapshot_content").unwrap() {
            Value::Json(value) => Some(
                serde_json::from_value::<SemSnapshot>(value.clone())
                    .unwrap_or_else(|error| panic!("invalid sem snapshot for {pk}: {error}")),
            ),
            Value::Null => None,
            other => panic!("expected JSON or null snapshot_content for {pk}, got {other:?}"),
        };
        current.insert(pk, snapshot);
    }

    current
        .into_iter()
        .filter_map(|(pk, snapshot)| snapshot.map(|snapshot| SemEntityState { pk, snapshot }))
        .collect()
}

async fn sem_change_count(lix: &lix_sdk::Lix, file_id: &str) -> usize {
    let rows = lix
        .execute(
            "SELECT entity_pk \
             FROM lix_change \
             WHERE file_id = $1 AND schema_key = 'sem_entity'",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();
    rows.len()
}

async fn sem_changes_since(lix: &lix_sdk::Lix, file_id: &str, count: usize) -> Vec<SemChange> {
    let rows = lix
        .execute(
            "SELECT entity_pk, snapshot_content \
             FROM lix_change \
             WHERE file_id = $1 AND schema_key = 'sem_entity' \
             ORDER BY created_at, id",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .unwrap();

    rows.rows()
        .iter()
        .skip(count)
        .map(|row| {
            let pk = single_entity_pk(row.get::<serde_json::Value>("entity_pk").unwrap());
            let snapshot = match row.value("snapshot_content").unwrap() {
                Value::Json(value) => Some(
                    serde_json::from_value::<SemSnapshot>(value.clone())
                        .unwrap_or_else(|error| panic!("invalid sem snapshot for {pk}: {error}")),
                ),
                Value::Null => None,
                other => panic!("expected JSON or null snapshot_content for {pk}, got {other:?}"),
            };
            SemChange { pk, snapshot }
        })
        .collect()
}

fn single_entity_pk(value: serde_json::Value) -> String {
    let serde_json::Value::Array(parts) = value else {
        panic!("entity_pk should be a JSON array, got {value}");
    };
    let [part] = parts.as_slice() else {
        panic!("expected single-part entity_pk, got {parts:?}");
    };
    part.as_str()
        .unwrap_or_else(|| panic!("entity_pk part should be a string, got {part}"))
        .to_string()
}

fn assert_same_pk(
    before_entities: &[SemEntityState],
    before_type: &str,
    before_name: &str,
    after_entities: &[SemEntityState],
    after_type: &str,
    after_name: &str,
) {
    let before = entity_by_type_and_name(before_entities, before_type, before_name);
    let after = entity_by_type_and_name(after_entities, after_type, after_name);
    assert_eq!(
        before.pk, after.pk,
        "{before_type} {before_name} should keep entity_pk after becoming {after_type} {after_name}"
    );
    assert_eq!(
        after.snapshot.id, after.pk,
        "snapshot id should match entity_pk for {after_type} {after_name}"
    );
}

fn assert_same_content_pk(
    before_entities: &[SemEntityState],
    before_type: &str,
    before_content_marker: &str,
    after_entities: &[SemEntityState],
    after_type: &str,
    after_content_marker: &str,
) {
    let before = entity_by_type_and_content(before_entities, before_type, before_content_marker);
    let after = entity_by_type_and_content(after_entities, after_type, after_content_marker);
    assert_eq!(
        before.pk, after.pk,
        "{before_type} containing {before_content_marker:?} should keep entity_pk after becoming {after_type} containing {after_content_marker:?}"
    );
    assert_eq!(
        after.snapshot.id, after.pk,
        "snapshot id should match entity_pk for {after_type} containing {after_content_marker:?}"
    );
}

fn assert_no_pk_churn(changes: &[SemChange], before_pk: &str, after_pk: &str) {
    assert_eq!(
        before_pk, after_pk,
        "stable entity should keep the same entity_pk"
    );
    assert!(
        changes
            .iter()
            .all(|change| change.pk != before_pk || change.snapshot.is_some()),
        "stable entity {before_pk} should not be tombstoned during the update: {changes:#?}"
    );
}

fn assert_tombstoned(changes: &[SemChange], pk: &str) {
    assert!(
        changes
            .iter()
            .any(|change| change.pk == pk && change.snapshot.is_none()),
        "expected {pk} to be tombstoned in update changes: {changes:#?}"
    );
}

fn assert_batch_has_unique_pks(changes: &[SemChange]) {
    let mut seen = BTreeSet::new();
    for change in changes {
        assert!(
            seen.insert(change.pk.clone()),
            "one detect cycle should not emit duplicate changes for entity_pk {}: {changes:#?}",
            change.pk
        );
    }
}

fn assert_entity_exists(entities: &[SemEntityState], entity_type: &str, entity_name: &str) {
    let _ = entity_by_type_and_name(entities, entity_type, entity_name);
}

fn assert_entity_missing(entities: &[SemEntityState], entity_type: &str, entity_name: &str) {
    assert!(
        entities.iter().all(|entity| {
            entity.snapshot.entity_type != entity_type || entity.snapshot.entity_name != entity_name
        }),
        "{entity_type} {entity_name} should not be present in current sem state: {entities:#?}"
    );
}

fn entity_by_type_and_name<'a>(
    entities: &'a [SemEntityState],
    entity_type: &str,
    entity_name: &str,
) -> &'a SemEntityState {
    let matches = entities
        .iter()
        .filter(|entity| {
            entity.snapshot.entity_type == entity_type && entity.snapshot.entity_name == entity_name
        })
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected one {entity_type} {entity_name}, got {} in {entities:#?}",
        matches.len()
    );
    matches[0]
}

fn entity_by_type_and_content<'a>(
    entities: &'a [SemEntityState],
    entity_type: &str,
    content_marker: &str,
) -> &'a SemEntityState {
    let matches = entities
        .iter()
        .filter(|entity| {
            entity.snapshot.entity_type == entity_type
                && entity
                    .snapshot
                    .content
                    .as_deref()
                    .is_some_and(|content| content.contains(content_marker))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        matches.len(),
        1,
        "expected one {entity_type} containing {content_marker:?}, got {} in {entities:#?}",
        matches.len()
    );
    matches[0]
}
