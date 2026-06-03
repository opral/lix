// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use sem_core::git::types::{FileChange, FileStatus};
use sem_core::model::change::ChangeType;
use sem_core::parser::differ::compute_semantic_diff;
use sem_core::parser::plugins::create_default_registry;
use sem_core::parser::registry::{ParserRegistry, detect_ext_from_content};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

pub const SCHEMA_KEY: &str = "sem_entity";
pub const SCHEMA_PATH: &str = "schema/sem_entity.json";
pub const MANIFEST_JSON: &str = include_str!("../manifest.json");
const SEM_ENTITY_SCHEMA_JSON: &str = include_str!("../schema/sem_entity.json");
const SYNTHETIC_STEM: &str = "file";

static SEM_ENTITY_SCHEMA: OnceLock<Value> = OnceLock::new();

#[derive(Clone, Copy, Debug)]
pub struct SemPlugin;
#[cfg(target_family = "wasm")]
export!(SemPlugin);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SemEntitySnapshot {
    id: String,
    entity_type: String,
    entity_name: String,
    file_path: String,
    line: usize,
    end_line: Option<usize>,
    content: Option<String>,
}

#[derive(Debug)]
struct RenderRow {
    entity_pk: Vec<String>,
    schema_key: String,
    snapshot_content: Option<String>,
}

impl Plugin for SemPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = file_from_entity_state(state)?;
        detect_changes_from_files(before, file)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        render_entity_state(state)
    }
}

fn registry() -> &'static ParserRegistry {
    static REGISTRY: OnceLock<ParserRegistry> = OnceLock::new();
    REGISTRY.get_or_init(create_default_registry)
}

fn detect_changes_from_files(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    let after_str = String::from_utf8(after.data).map_err(|error| {
        PluginError::InvalidInput(format!("invalid UTF-8 in file.data: {error}"))
    })?;
    let before_str = before
        .map(|file| {
            String::from_utf8(file.data).map_err(|error| {
                PluginError::InvalidInput(format!("invalid UTF-8 in previous state: {error}"))
            })
        })
        .transpose()?;

    let path = infer_synthetic_path(before_str.as_deref(), &after_str);
    let status = if before_str.is_none() {
        FileStatus::Added
    } else if after_str.is_empty() {
        FileStatus::Deleted
    } else {
        FileStatus::Modified
    };

    let file_change = FileChange {
        file_path: path.clone(),
        status,
        old_file_path: None,
        before_content: before_str,
        after_content: if after_str.is_empty() {
            None
        } else {
            Some(after_str.clone())
        },
    };
    let result = compute_semantic_diff(&[file_change], registry(), None, None);
    let after_entities = if after_str.is_empty() {
        Vec::new()
    } else {
        registry().extract_entities(&path, &after_str)
    };

    let mut line_to_end: HashMap<usize, usize> = HashMap::new();
    for entity in &after_entities {
        line_to_end
            .entry(entity.start_line)
            .and_modify(|existing| *existing = (*existing).max(entity.end_line))
            .or_insert(entity.end_line);
    }

    let after_lines = after_str.lines().collect::<Vec<_>>();
    let total_lines = after_lines.len();
    let mut changes = result
        .changes
        .into_iter()
        .filter(|change| change.entity_type != "orphan")
        .map(|change| {
            let snapshot_content = match change.change_type {
                ChangeType::Deleted => None,
                _ => {
                    let end_line = line_to_end
                        .get(&change.entity_line)
                        .copied()
                        .unwrap_or(change.entity_line);
                    let content = line_range_content(
                        &after_lines,
                        change.entity_line,
                        end_line,
                        change.after_content.unwrap_or_default(),
                    );
                    Some(serialize_snapshot(SemEntitySnapshot {
                        id: change.entity_id.clone(),
                        entity_type: change.entity_type,
                        entity_name: change.entity_name,
                        file_path: change.file_path,
                        line: change.entity_line,
                        end_line: Some(end_line),
                        content: Some(content),
                    })?)
                }
            };
            Ok(DetectedChange {
                entity_pk: vec![change.entity_id],
                schema_key: SCHEMA_KEY.to_string(),
                snapshot_content,
                metadata: None,
            })
        })
        .collect::<Result<Vec<_>, PluginError>>()?;

    append_gap_changes(&mut changes, &path, &after_lines, total_lines)?;
    Ok(changes)
}

fn append_gap_changes(
    changes: &mut Vec<DetectedChange>,
    path: &str,
    after_lines: &[&str],
    total_lines: usize,
) -> Result<(), PluginError> {
    if total_lines == 0 {
        return Ok(());
    }

    let mut covered = vec![false; total_lines + 1];
    for change in changes.iter() {
        let Some(snapshot) = change.snapshot_content.as_ref() else {
            continue;
        };
        let parsed = parse_snapshot(snapshot, &change.entity_pk)?;
        let end_line = parsed.end_line.unwrap_or(parsed.line);
        for line in parsed.line..=end_line.min(total_lines) {
            if line > 0 {
                covered[line] = true;
            }
        }
    }

    let mut gap_index = 0usize;
    let mut line = 1usize;
    while line <= total_lines {
        if covered[line] {
            line += 1;
            continue;
        }

        let start = line;
        while line <= total_lines && !covered[line] {
            line += 1;
        }
        let end = line - 1;
        let gap_id = format!("{path}::gap::{gap_index}");
        let content = (start..=end)
            .map(|line| after_lines[line - 1])
            .collect::<Vec<_>>()
            .join("\n");
        let snapshot = serialize_snapshot(SemEntitySnapshot {
            id: gap_id.clone(),
            entity_type: "gap".to_string(),
            entity_name: format!("gap-{gap_index}"),
            file_path: path.to_string(),
            line: start,
            end_line: Some(end),
            content: Some(content),
        })?;

        changes.push(DetectedChange {
            entity_pk: vec![gap_id],
            schema_key: SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot),
            metadata: None,
        });
        gap_index += 1;
    }

    Ok(())
}

fn line_range_content(
    lines: &[&str],
    start_line: usize,
    end_line: usize,
    fallback: String,
) -> String {
    let total_lines = lines.len();
    let start = start_line.max(1);
    let end = end_line.min(total_lines);
    if start <= end && start <= total_lines {
        return (start..=end)
            .map(|line| lines[line - 1])
            .collect::<Vec<_>>()
            .join("\n");
    }
    fallback
}

fn render_entity_state(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
    render_rows(state.into_iter().map(|row| RenderRow {
        entity_pk: row.entity_pk,
        schema_key: row.schema_key,
        snapshot_content: Some(row.snapshot_content),
    }))
}

fn render_rows(rows: impl IntoIterator<Item = RenderRow>) -> Result<Vec<u8>, PluginError> {
    let mut seen = HashSet::new();
    let mut snapshots = Vec::<SemEntitySnapshot>::new();

    for row in rows {
        if row.schema_key != SCHEMA_KEY {
            continue;
        }
        let entity_pk = single_entity_pk(row.entity_pk.clone())?;
        if !seen.insert(entity_pk.clone()) {
            return Err(PluginError::InvalidInput(format!(
                "duplicate sem_entity snapshot for entity_pk '{entity_pk}'"
            )));
        }
        let Some(snapshot_content) = row.snapshot_content else {
            continue;
        };
        snapshots.push(parse_snapshot(&snapshot_content, &[entity_pk])?);
    }

    if snapshots.is_empty() {
        return Ok(Vec::new());
    }

    snapshots.sort_by(|left, right| {
        left.line.cmp(&right.line).then_with(|| {
            right
                .end_line
                .unwrap_or(right.line)
                .cmp(&left.end_line.unwrap_or(left.line))
        })
    });

    let mut parts = Vec::<String>::new();
    let mut current_end = 0usize;
    for snapshot in snapshots {
        let end_line = snapshot.end_line.unwrap_or(snapshot.line);
        if snapshot.line <= current_end {
            continue;
        }
        if let Some(content) = snapshot.content {
            parts.push(content);
            current_end = end_line;
        }
    }

    let mut rendered = parts.join("\n");
    if !rendered.is_empty() {
        rendered.push('\n');
    }
    Ok(rendered.into_bytes())
}

fn parse_snapshot(raw: &str, entity_pk: &[String]) -> Result<SemEntitySnapshot, PluginError> {
    let snapshot: SemEntitySnapshot = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid sem_entity snapshot_content: {error}"))
    })?;
    validate_single_entity_pk(entity_pk)?;
    if snapshot.id != entity_pk[0] {
        return Err(PluginError::InvalidInput(format!(
            "sem_entity.id '{}' does not match entity_pk '{}'",
            snapshot.id, entity_pk[0]
        )));
    }
    Ok(snapshot)
}

fn serialize_snapshot(snapshot: SemEntitySnapshot) -> Result<String, PluginError> {
    serde_json::to_string(&snapshot)
        .map_err(|error| PluginError::Internal(format!("failed to encode sem snapshot: {error}")))
}

fn file_from_entity_state(state: Vec<EntityState>) -> Result<Option<File>, PluginError> {
    if state.is_empty() {
        return Ok(None);
    }
    Ok(Some(File {
        data: render_entity_state(state)?,
    }))
}

fn infer_synthetic_path(before: Option<&str>, after: &str) -> String {
    let content = if after.is_empty() {
        before.unwrap_or_default()
    } else {
        after
    };
    let ext =
        detect_ext_from_content(content).unwrap_or_else(|| best_extension_for_content(content));
    format!("{SYNTHETIC_STEM}{ext}")
}

fn best_extension_for_content(content: &str) -> String {
    const EXTENSIONS: &[&str] = &[
        ".ts", ".tsx", ".js", ".jsx", ".py", ".go", ".rs", ".java", ".rb", ".c", ".cpp", ".cs",
        ".php", ".kt", ".swift", ".ex", ".sh", ".tf", ".scala", ".zig", ".nix", ".dart", ".pl",
        ".ml", ".mli", ".svelte", ".vue",
    ];

    let mut best_ext = ".txt";
    let mut best_count = 0usize;
    for ext in EXTENSIONS {
        let path = format!("{SYNTHETIC_STEM}{ext}");
        let count = registry()
            .extract_entities(&path, content)
            .into_iter()
            .filter(|entity| !entity.content.trim().is_empty())
            .count();
        if count > best_count {
            best_ext = ext;
            best_count = count;
        }
    }
    best_ext.to_string()
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

pub fn detect_changes(
    before: Option<File>,
    after: File,
) -> Result<Vec<DetectedChange>, PluginError> {
    detect_changes_from_files(before, after)
}

pub fn render_changes(changes: Vec<DetectedChange>) -> Result<Vec<u8>, PluginError> {
    render_rows(changes.into_iter().map(|change| RenderRow {
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        snapshot_content: change.snapshot_content,
    }))
}

pub fn manifest_json() -> &'static str {
    MANIFEST_JSON
}

pub fn schema_json() -> &'static str {
    SEM_ENTITY_SCHEMA_JSON
}

pub fn schema_definition() -> &'static Value {
    SEM_ENTITY_SCHEMA.get_or_init(|| {
        serde_json::from_str(SEM_ENTITY_SCHEMA_JSON).expect("sem schema must parse")
    })
}
