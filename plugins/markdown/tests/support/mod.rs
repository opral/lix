#![allow(dead_code)]

use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{DetectedChange, File, MarkdownPlugin, NODE_SCHEMA_KEY};
use serde_json::Value;
use std::collections::BTreeMap;

pub fn file(source: &str) -> File {
    File {
        filename: Some("test.md".to_string()),
        data: source.as_bytes().to_vec(),
    }
}

pub fn state_from_source(source: &str) -> Vec<EntityState> {
    apply_changes(
        Vec::new(),
        MarkdownPlugin::detect_changes(Vec::new(), file(source)).expect("Markdown should parse"),
    )
}

pub fn apply_changes(state: Vec<EntityState>, changes: Vec<DetectedChange>) -> Vec<EntityState> {
    let mut rows = state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();
    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if let Some(snapshot_content) = change.snapshot_content {
            rows.insert(
                key,
                EntityState {
                    entity_pk: change.entity_pk,
                    schema_key: change.schema_key,
                    snapshot_content,
                    metadata: change.metadata,
                },
            );
        } else {
            rows.remove(&key);
        }
    }
    rows.into_values().collect()
}

pub fn changes(state: Vec<EntityState>, source: &str) -> Vec<DetectedChange> {
    MarkdownPlugin::detect_changes(state, file(source)).expect("Markdown should parse")
}

pub fn evolve(state: Vec<EntityState>, source: &str) -> Vec<EntityState> {
    let delta = changes(state.clone(), source);
    apply_changes(state, delta)
}

pub fn render(state: Vec<EntityState>) -> String {
    String::from_utf8(MarkdownPlugin::render(state).expect("Markdown state should render"))
        .expect("Markdown output should be UTF-8")
}

pub fn snapshot(row: &EntityState) -> Value {
    serde_json::from_str(&row.snapshot_content).expect("snapshot should be JSON")
}

pub fn change_snapshot(change: &DetectedChange) -> Value {
    serde_json::from_str(
        change
            .snapshot_content
            .as_deref()
            .expect("change should be an upsert"),
    )
    .expect("snapshot should be JSON")
}

pub fn node_rows(state: &[EntityState]) -> Vec<&EntityState> {
    state
        .iter()
        .filter(|row| row.schema_key == NODE_SCHEMA_KEY)
        .collect()
}

pub fn rows_of_kind<'a>(state: &'a [EntityState], kind: &str) -> Vec<&'a EntityState> {
    node_rows(state)
        .into_iter()
        .filter(|row| snapshot(row).get("kind").and_then(Value::as_str) == Some(kind))
        .collect()
}

pub fn ids_of_kind(state: &[EntityState], kind: &str) -> Vec<String> {
    let mut ids = rows_of_kind(state, kind)
        .into_iter()
        .map(|row| row.entity_pk[0].clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

pub fn semantic_html(source: &str) -> String {
    let options = markdown::Options {
        parse: markdown::ParseOptions::gfm(),
        compile: markdown::CompileOptions::gfm(),
    };
    markdown::to_html_with_options(source, &options)
        .expect("fixture should compile as GFM")
        .trim_end_matches(['\r', '\n'])
        .to_string()
}
