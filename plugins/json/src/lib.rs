// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;

pub const SCHEMA_KEY: &str = "json_pointer";
pub const SCHEMA_PATH: &str = "schema/json_pointer.json";
pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
const MAX_ARRAY_INDEX: usize = 100_000;
const JSON_POINTER_SCHEMA_JSON: &str = include_str!("../schema/json_pointer.json");

static JSON_POINTER_SCHEMA: OnceLock<Value> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub struct JsonPlugin;
#[cfg(target_family = "wasm")]
export!(JsonPlugin);

#[derive(Debug, serde::Serialize)]
struct SnapshotContentRef<'a> {
    path: &'a str,
    value: &'a Value,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotContentWithPath {
    path: String,
    value: Value,
}

#[derive(Debug, Clone)]
struct ProjectionUpsert {
    pointer: String,
    tokens: Vec<String>,
    terminal_token: Option<TypedPathToken>,
    value: Value,
}

#[derive(Debug)]
struct ProjectionRow {
    entity_pk: Vec<String>,
    snapshot_content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionNodeKind {
    Object,
    Array,
    Scalar,
}

impl ProjectionNodeKind {
    fn from_value(value: &Value) -> Self {
        if value.is_object() {
            Self::Object
        } else if value.is_array() {
            Self::Array
        } else {
            Self::Scalar
        }
    }
}

#[derive(Debug, Clone)]
enum TypedPathToken {
    ObjectKey(String),
    ArrayIndex(usize),
}

#[derive(Debug)]
struct ProjectionTreeNode {
    value: Option<Value>,
    terminal_token: Option<TypedPathToken>,
    object_children: Vec<(String, usize)>,
    array_children: Vec<(usize, usize)>,
}

impl Plugin for JsonPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = file_from_entity_state(state)?;
        detect_changes_from_files(before, file)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        render_entity_state(empty_file(), state)
    }
}

fn detect_changes_from_files(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    let before_json = before
        .as_ref()
        .map(|file| parse_json_bytes(&file.data))
        .transpose()?;
    let after_json = parse_json_bytes(&after.data)?;

    let mut changes = Vec::new();
    diff_json(
        before_json.as_ref(),
        Some(&after_json),
        &mut Vec::new(),
        &mut changes,
    )?;

    Ok(changes)
}

fn render_entity_state(_file: File, state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    render_projection_rows(state.into_iter().filter_map(|row| {
        if row.schema_key == SCHEMA_KEY {
            Some(ProjectionRow {
                entity_pk: row.entity_pk,
                snapshot_content: row.snapshot_content,
            })
        } else {
            None
        }
    }))
}

fn render_projection_rows(
    rows: impl IntoIterator<Item = ProjectionRow>,
) -> Result<Vec<u8>, PluginError> {
    let mut seen_entity_pks = BTreeSet::new();
    let mut upserts = Vec::new();

    for row in rows {
        let pointer = single_entity_pk(row.entity_pk)?;
        if !seen_entity_pks.insert(pointer.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "duplicate entity_pk '{pointer}' for schema_key '{SCHEMA_KEY}'"
            )));
        }

        let tokens = pointer_tokens(&pointer)?;
        let value = parse_snapshot_value(&row.snapshot_content, &pointer)?;
        upserts.push(ProjectionUpsert {
            pointer,
            tokens,
            terminal_token: None,
            value,
        });
    }

    let has_root_upsert = upserts.iter().any(|entry| entry.pointer.is_empty());
    let has_non_root_rows = upserts.iter().any(|entry| !entry.pointer.is_empty());
    if has_non_root_rows && !has_root_upsert {
        return Err(PluginError::InvalidInput(
            "non-root projection rows require a root row with entity_pk ''".to_string(),
        ));
    }

    let upsert_pointers = upserts
        .iter()
        .map(|entry| entry.pointer.clone())
        .collect::<BTreeSet<_>>();
    let upsert_node_kinds = upserts
        .iter()
        .map(|entry| {
            (
                entry.pointer.clone(),
                ProjectionNodeKind::from_value(&entry.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut array_child_indices: BTreeMap<String, BTreeSet<usize>> = BTreeMap::new();
    let mut canonical_upsert_pointers = BTreeSet::new();

    for upsert in &mut upserts {
        let mut ancestor = String::new();
        let mut canonical_pointer = String::new();
        let raw_tokens = std::mem::take(&mut upsert.tokens);
        let mut terminal_token = None;
        for token in raw_tokens {
            if !upsert_pointers.contains(&ancestor) {
                return Err(PluginError::InvalidInput(format!(
                    "missing ancestor container row '{ancestor}' for entity_pk '{}'",
                    upsert.pointer
                )));
            }
            let ancestor_kind = *upsert_node_kinds
                .get(&ancestor)
                .expect("ancestor pointer existence checked above");
            let validated = validate_child_token_for_ancestor(
                ancestor_kind,
                &token,
                &ancestor,
                &upsert.pointer,
            )?;
            let canonical_token = validated.canonical_token;
            let parent_ancestor = ancestor.clone();
            push_pointer_segment(&mut ancestor, &token);
            push_pointer_segment(&mut canonical_pointer, &canonical_token);

            if let Some(index) = validated.array_index {
                array_child_indices
                    .entry(parent_ancestor)
                    .or_default()
                    .insert(index);
                terminal_token = Some(TypedPathToken::ArrayIndex(index));
            } else {
                terminal_token = Some(TypedPathToken::ObjectKey(token));
            }
        }
        upsert.terminal_token = terminal_token;

        if !canonical_upsert_pointers.insert(canonical_pointer.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "logical duplicate pointer '{canonical_pointer}' in projection rows"
            )));
        }
    }
    validate_sparse_array_children(&array_child_indices)?;
    let document = build_document_from_projection(upserts)?;

    serde_json::to_vec(&document).map_err(|error| {
        PluginError::Internal(format!("failed to serialize reconstructed JSON: {error}"))
    })
}

fn parse_json_bytes(data: &[u8]) -> Result<Value, PluginError> {
    if data.is_empty() {
        return Ok(Value::Object(Map::new()));
    }

    serde_json::from_slice::<Value>(data).map_err(|error| {
        PluginError::InvalidInput(format!("file.data must be valid JSON UTF-8 bytes: {error}"))
    })
}

fn parse_snapshot_value(raw: &str, pointer: &str) -> Result<Value, PluginError> {
    if let Ok(parsed) = serde_json::from_str::<SnapshotContentWithPath>(raw) {
        if parsed.path != pointer {
            return Err(PluginError::InvalidInput(format!(
                "snapshot path '{}' does not match entity_pk '{}'",
                parsed.path, pointer
            )));
        }
        return Ok(parsed.value);
    }

    parse_snapshot_value_slow(raw, pointer)
}

fn parse_snapshot_value_slow(raw: &str, pointer: &str) -> Result<Value, PluginError> {
    let parsed = serde_json::from_str::<Value>(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid snapshot_content for pointer '{pointer}': {error}"
        ))
    })?;

    let Value::Object(mut object) = parsed else {
        return Err(PluginError::InvalidInput(format!(
            "snapshot_content for pointer '{pointer}' must be an object with 'value'"
        )));
    };

    let raw_path = object.remove("path");
    let raw_value = object.remove("value");
    if !object.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "snapshot_content for pointer '{pointer}' contains unsupported properties"
        )));
    }

    match (raw_path, raw_value) {
        (Some(path), Some(value)) => {
            let Some(path_string) = path.as_str() else {
                return Err(PluginError::InvalidInput(format!(
                    "snapshot path for entity_pk '{pointer}' must be a string"
                )));
            };
            if path_string != pointer {
                return Err(PluginError::InvalidInput(format!(
                    "snapshot path '{path_string}' does not match entity_pk '{pointer}'"
                )));
            }
            Ok(value)
        }
        (None, Some(_)) => Err(PluginError::InvalidInput(format!(
            "snapshot_content for pointer '{pointer}' must contain 'path'"
        ))),
        (_, None) => Err(PluginError::InvalidInput(format!(
            "snapshot_content for pointer '{pointer}' must contain 'value'"
        ))),
    }
}

fn diff_json(
    before: Option<&Value>,
    after: Option<&Value>,
    path: &mut Vec<String>,
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    if before.is_none() && after.is_none() {
        return Ok(());
    }

    if after.is_none() {
        collect_deletions(
            before.expect("after is none implies before exists"),
            path,
            changes,
            true,
        );
        return Ok(());
    }

    if before.is_none() {
        collect_leaves(after.expect("checked above"), path, changes)?;
        return Ok(());
    }

    let before_value = before.expect("checked above");
    let after_value = after.expect("checked above");

    if before_value == after_value {
        return Ok(());
    }

    let before_is_container = is_container(before_value);
    let after_is_container = is_container(after_value);

    if before_is_container && after_is_container {
        if let (Some(before_items), Some(after_items)) =
            (before_value.as_array(), after_value.as_array())
        {
            let shared = before_items.len().min(after_items.len());
            for index in 0..shared {
                path.push(index.to_string());
                diff_json(
                    before_items.get(index),
                    after_items.get(index),
                    path,
                    changes,
                )?;
                path.pop();
            }

            if before_items.len() > after_items.len() {
                for index in (after_items.len()..before_items.len()).rev() {
                    path.push(index.to_string());
                    diff_json(before_items.get(index), None, path, changes)?;
                    path.pop();
                }
            } else {
                for index in before_items.len()..after_items.len() {
                    path.push(index.to_string());
                    diff_json(None, after_items.get(index), path, changes)?;
                    path.pop();
                }
            }
            return Ok(());
        }

        if let (Some(before_object), Some(after_object)) =
            (before_value.as_object(), after_value.as_object())
        {
            let mut keys = before_object.keys().cloned().collect::<Vec<_>>();
            for key in after_object.keys() {
                if !before_object.contains_key(key) {
                    keys.push(key.clone());
                }
            }

            for key in keys {
                path.push(key.clone());
                diff_json(
                    before_object.get(&key),
                    after_object.get(&key),
                    path,
                    changes,
                )?;
                path.pop();
            }
            return Ok(());
        }
    }

    if before_is_container || after_is_container {
        collect_deletions(before_value, path, changes, false);
        collect_leaves(after_value, path, changes)?;
        return Ok(());
    }

    if before_value != after_value {
        push_upsert(changes, pointer_from_segments(path), after_value.clone())?;
    }

    Ok(())
}

fn collect_deletions(
    value: &Value,
    path: &mut Vec<String>,
    changes: &mut Vec<DetectedChange>,
    include_current: bool,
) {
    match value {
        Value::Array(items) => {
            if include_current {
                push_deletion(changes, pointer_from_segments(path));
            }
            for index in (0..items.len()).rev() {
                path.push(index.to_string());
                collect_deletions(&items[index], path, changes, true);
                path.pop();
            }
        }
        Value::Object(object) => {
            if include_current {
                push_deletion(changes, pointer_from_segments(path));
            }
            for (key, item) in object {
                path.push(key.clone());
                collect_deletions(item, path, changes, true);
                path.pop();
            }
        }
        _ => {
            if include_current {
                push_deletion(changes, pointer_from_segments(path));
            }
        }
    }
}

fn collect_leaves(
    value: &Value,
    path: &mut Vec<String>,
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    match value {
        Value::Array(items) => {
            push_upsert(
                changes,
                pointer_from_segments(path),
                Value::Array(Vec::new()),
            )?;
            for (index, item) in items.iter().enumerate() {
                path.push(index.to_string());
                collect_leaves(item, path, changes)?;
                path.pop();
            }
            Ok(())
        }
        Value::Object(object) => {
            push_upsert(
                changes,
                pointer_from_segments(path),
                Value::Object(Map::new()),
            )?;
            for (key, item) in object {
                path.push(key.clone());
                collect_leaves(item, path, changes)?;
                path.pop();
            }
            Ok(())
        }
        _ => push_upsert(changes, pointer_from_segments(path), value.clone()),
    }
}

fn push_deletion(changes: &mut Vec<DetectedChange>, pointer: String) {
    changes.push(DetectedChange {
        entity_pk: vec![pointer],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: None,
        metadata: None,
    });
}

fn push_upsert(
    changes: &mut Vec<DetectedChange>,
    pointer: String,
    value: Value,
) -> Result<(), PluginError> {
    let snapshot_content = serde_json::to_string(&SnapshotContentRef {
        path: &pointer,
        value: &value,
    })
    .map_err(|error| {
        PluginError::Internal(format!(
            "failed to serialize snapshot content for '{pointer}': {error}"
        ))
    })?;

    changes.push(DetectedChange {
        entity_pk: vec![pointer],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    });

    Ok(())
}

fn is_container(value: &Value) -> bool {
    value.is_array() || value.is_object()
}

fn pointer_from_segments(segments: &[String]) -> String {
    if segments.is_empty() {
        return String::new();
    }

    let mut pointer = String::new();
    for segment in segments {
        push_pointer_segment(&mut pointer, segment);
    }
    pointer
}

fn push_pointer_segment(pointer: &mut String, token: &str) {
    pointer.push('/');
    for ch in token.chars() {
        match ch {
            '~' => pointer.push_str("~0"),
            '/' => pointer.push_str("~1"),
            _ => pointer.push(ch),
        }
    }
}

fn unescape_pointer_token(token: &str) -> Result<String, PluginError> {
    let mut output = String::with_capacity(token.len());
    let mut chars = token.chars();

    while let Some(ch) = chars.next() {
        if ch != '~' {
            output.push(ch);
            continue;
        }

        match chars.next() {
            Some('0') => output.push('~'),
            Some('1') => output.push('/'),
            Some(other) => {
                return Err(PluginError::InvalidInput(format!(
                    "invalid JSON pointer escape '~{other}' in token '{token}'"
                )));
            }
            None => {
                return Err(PluginError::InvalidInput(format!(
                    "invalid JSON pointer escape '~' in token '{token}'"
                )));
            }
        }
    }

    Ok(output)
}

fn pointer_tokens(pointer: &str) -> Result<Vec<String>, PluginError> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }

    if !pointer.starts_with('/') {
        return Err(PluginError::InvalidInput(format!(
            "entity_pk '{pointer}' must be a JSON pointer"
        )));
    }

    pointer
        .split('/')
        .skip(1)
        .map(unescape_pointer_token)
        .collect()
}

struct ValidatedChildToken {
    canonical_token: String,
    array_index: Option<usize>,
}

fn validate_child_token_for_ancestor(
    ancestor_kind: ProjectionNodeKind,
    child_token: &str,
    ancestor_pointer: &str,
    entity_pk: &str,
) -> Result<ValidatedChildToken, PluginError> {
    match ancestor_kind {
        ProjectionNodeKind::Object => Ok(ValidatedChildToken {
            canonical_token: child_token.to_string(),
            array_index: None,
        }),
        ProjectionNodeKind::Array => {
            let index = parse_projection_array_index(child_token, ancestor_pointer, entity_pk)?;
            Ok(ValidatedChildToken {
                canonical_token: index.to_string(),
                array_index: Some(index),
            })
        }
        ProjectionNodeKind::Scalar => Err(PluginError::InvalidInput(format!(
            "ancestor '{ancestor_pointer}' for entity_pk '{entity_pk}' is not a container"
        ))),
    }
}

fn validate_sparse_array_children(
    indices_by_ancestor: &BTreeMap<String, BTreeSet<usize>>,
) -> Result<(), PluginError> {
    for (ancestor, indices) in indices_by_ancestor {
        let Some(max_index) = indices.iter().next_back() else {
            continue;
        };

        for expected in 0..=*max_index {
            if !indices.contains(&expected) {
                return Err(PluginError::InvalidInput(format!(
                    "sparse array projection under '{ancestor}': missing index {expected}"
                )));
            }
        }
    }
    Ok(())
}

fn parse_projection_array_index(
    token: &str,
    ancestor_pointer: &str,
    entity_pk: &str,
) -> Result<usize, PluginError> {
    if token == "-" {
        return Err(PluginError::InvalidInput(format!(
            "entity_pk '{entity_pk}' uses non-canonical '-' array token under '{ancestor_pointer}'"
        )));
    }
    if token.is_empty() || !token.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(PluginError::InvalidInput(format!(
            "invalid array index token '{token}' under '{ancestor_pointer}'"
        )));
    }
    if token.len() > 1 && token.starts_with('0') {
        return Err(PluginError::InvalidInput(format!(
            "entity_pk '{entity_pk}' uses non-canonical array index token '{token}' under '{ancestor_pointer}'"
        )));
    }

    let index = token.parse::<usize>().map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid array index token '{token}' under '{ancestor_pointer}': {error}"
        ))
    })?;
    if index > MAX_ARRAY_INDEX {
        return Err(PluginError::InvalidInput(format!(
            "array index {index} exceeds max supported index {MAX_ARRAY_INDEX}"
        )));
    }
    Ok(index)
}

fn build_document_from_projection(upserts: Vec<ProjectionUpsert>) -> Result<Value, PluginError> {
    if upserts.is_empty() {
        return Ok(Value::Object(Map::new()));
    }

    let mut index_by_pointer = HashMap::with_capacity(upserts.len());
    let mut pointers = Vec::with_capacity(upserts.len());
    let mut nodes = Vec::with_capacity(upserts.len());
    for (index, upsert) in upserts.into_iter().enumerate() {
        index_by_pointer.insert(upsert.pointer.clone(), index);
        pointers.push(upsert.pointer);
        nodes.push(ProjectionTreeNode {
            value: Some(upsert.value),
            terminal_token: upsert.terminal_token,
            object_children: Vec::new(),
            array_children: Vec::new(),
        });
    }

    let root_index = index_by_pointer.get("").copied().ok_or_else(|| {
        PluginError::InvalidInput(
            "non-root projection rows require a root row with entity_pk ''".to_string(),
        )
    })?;

    for index in 0..pointers.len() {
        let pointer = &pointers[index];
        if pointer.is_empty() {
            continue;
        }
        let parent_pointer = parent_pointer(pointer);
        let parent_index = index_by_pointer
            .get(parent_pointer)
            .copied()
            .ok_or_else(|| {
                PluginError::InvalidInput(format!(
                    "missing ancestor container row '{parent_pointer}' for entity_pk '{pointer}'"
                ))
            })?;
        let terminal_token = nodes[index].terminal_token.take().ok_or_else(|| {
            PluginError::Internal(format!(
                "missing terminal token for non-root projection row '{pointer}'"
            ))
        })?;

        match terminal_token {
            TypedPathToken::ObjectKey(key) => {
                nodes[parent_index].object_children.push((key, index));
            }
            TypedPathToken::ArrayIndex(array_index) => {
                nodes[parent_index]
                    .array_children
                    .push((array_index, index));
            }
        }
    }

    materialize_projection_node(&mut nodes, root_index)
}

fn parent_pointer(pointer: &str) -> &str {
    pointer
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("")
}

fn materialize_projection_node(
    nodes: &mut [ProjectionTreeNode],
    index: usize,
) -> Result<Value, PluginError> {
    let (mut value, object_children, array_children) = {
        let node = nodes.get_mut(index).ok_or_else(|| {
            PluginError::Internal(format!("projection node index {index} out of bounds"))
        })?;
        (
            node.value.take().ok_or_else(|| {
                PluginError::Internal(format!("projection node {index} was materialized twice"))
            })?,
            std::mem::take(&mut node.object_children),
            std::mem::take(&mut node.array_children),
        )
    };

    match &mut value {
        Value::Object(object) => {
            if !array_children.is_empty() {
                return Err(PluginError::InvalidInput(
                    "object projection node cannot have array-index children".to_string(),
                ));
            }
            for (key, child_index) in object_children {
                let child_value = materialize_projection_node(nodes, child_index)?;
                object.insert(key, child_value);
            }
        }
        Value::Array(items) => {
            if !object_children.is_empty() {
                return Err(PluginError::InvalidInput(
                    "array projection node cannot have object-key children".to_string(),
                ));
            }
            for (array_index, child_index) in array_children {
                while items.len() <= array_index {
                    items.push(Value::Null);
                }
                items[array_index] = materialize_projection_node(nodes, child_index)?;
            }
        }
        _ => {
            if !object_children.is_empty() || !array_children.is_empty() {
                return Err(PluginError::InvalidInput(
                    "scalar projection node cannot have children".to_string(),
                ));
            }
        }
    }

    Ok(value)
}

fn file_from_entity_state(state: Vec<EntityState>) -> Result<Option<File>, PluginError> {
    if state.is_empty() {
        return Ok(None);
    }

    Ok(Some(File {
        filename: None,
        data: render_entity_state(empty_file(), state)?,
    }))
}

fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    validate_single_entity_pk(&entity_pk)?;
    Ok(entity_pk.remove(0))
}

fn validate_single_entity_pk(entity_pk: &[String]) -> Result<(), PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(())
}

fn empty_file() -> File {
    File {
        filename: None,
        data: Vec::new(),
    }
}

pub fn schema_json() -> &'static str {
    JSON_POINTER_SCHEMA_JSON
}

pub fn schema_definition() -> &'static Value {
    JSON_POINTER_SCHEMA.get_or_init(|| {
        serde_json::from_str(JSON_POINTER_SCHEMA_JSON).expect("json pointer schema must be valid")
    })
}
