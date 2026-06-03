// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use sem_core::model::entity::SemanticEntity;
use sem_core::parser::plugins::create_default_registry;
use sem_core::parser::registry::{ParserRegistry, detect_ext_from_content};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
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
    #[serde(default)]
    sem_id: String,
    entity_type: String,
    entity_name: String,
    file_path: String,
    line: usize,
    end_line: Option<usize>,
    content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    structural_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct PreviousSnapshot {
    pk: String,
    snapshot: SemEntitySnapshot,
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
        let previous = previous_snapshots(&state)?;
        let before = file_from_entity_state(state)?;
        detect_changes_from_files(previous, before, file)
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
    previous: Vec<PreviousSnapshot>,
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
    let after_entities = if after_str.is_empty() {
        Vec::new()
    } else {
        registry().extract_entities(&path, &after_str)
    };

    let after_lines = after_str.lines().collect::<Vec<_>>();
    let total_lines = after_lines.len();
    let mut current = after_entities
        .into_iter()
        .filter(|entity| entity.entity_type != "orphan")
        .map(|entity| snapshot_from_entity(entity, &after_lines))
        .collect::<Vec<_>>();
    append_gap_snapshots(&mut current, &path, &after_lines, total_lines);
    append_source_snapshot(&mut current, &path, &after_str, total_lines);

    let (assigned, tombstones) = assign_stable_pks(&previous, current);
    let mut changes = assigned
        .into_iter()
        .map(|(pk, mut snapshot)| {
            snapshot.id = pk.clone();
            Ok(DetectedChange {
                entity_pk: vec![pk],
                schema_key: SCHEMA_KEY.to_string(),
                snapshot_content: Some(serialize_snapshot(snapshot)?),
                metadata: None,
            })
        })
        .collect::<Result<Vec<_>, PluginError>>()?;
    changes.extend(tombstones.into_iter().map(|pk| DetectedChange {
        entity_pk: vec![pk],
        schema_key: SCHEMA_KEY.to_string(),
        snapshot_content: None,
        metadata: None,
    }));
    Ok(changes)
}

fn snapshot_from_entity(entity: SemanticEntity, after_lines: &[&str]) -> SemEntitySnapshot {
    let content = line_range_content(
        after_lines,
        entity.start_line,
        entity.end_line,
        entity.content.clone(),
    );
    SemEntitySnapshot {
        id: entity.id.clone(),
        sem_id: entity.id,
        entity_type: entity.entity_type,
        entity_name: entity.name,
        file_path: entity.file_path,
        line: entity.start_line,
        end_line: Some(entity.end_line),
        content: Some(content),
        content_hash: Some(entity.content_hash),
        structural_hash: entity.structural_hash,
    }
}

fn append_gap_snapshots(
    snapshots: &mut Vec<SemEntitySnapshot>,
    path: &str,
    after_lines: &[&str],
    total_lines: usize,
) {
    if total_lines == 0 {
        return;
    }

    let mut covered = vec![false; total_lines + 1];
    for snapshot in snapshots.iter() {
        let end_line = snapshot.end_line.unwrap_or(snapshot.line);
        for line in snapshot.line..=end_line.min(total_lines) {
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
        let content = (start..=end)
            .map(|line| after_lines[line - 1])
            .collect::<Vec<_>>()
            .join("\n");
        let gap_id = format!("{path}::gap::{gap_index}");
        snapshots.push(SemEntitySnapshot {
            id: gap_id.clone(),
            sem_id: gap_id,
            entity_type: "gap".to_string(),
            entity_name: format!("gap-{gap_index}"),
            file_path: path.to_string(),
            line: start,
            end_line: Some(end),
            content: Some(content),
            content_hash: None,
            structural_hash: None,
        });
        gap_index += 1;
    }
}

fn append_source_snapshot(
    snapshots: &mut Vec<SemEntitySnapshot>,
    path: &str,
    source: &str,
    total_lines: usize,
) {
    if source.is_empty() {
        return;
    }

    let source_id = format!("{path}::source");
    snapshots.push(SemEntitySnapshot {
        id: source_id.clone(),
        sem_id: source_id,
        entity_type: "source".to_string(),
        entity_name: "source".to_string(),
        file_path: path.to_string(),
        line: 1,
        end_line: Some(total_lines.max(1)),
        content: Some(source.trim_end_matches('\n').to_string()),
        content_hash: None,
        structural_hash: None,
    });
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

fn previous_snapshots(state: &[EntityState]) -> Result<Vec<PreviousSnapshot>, PluginError> {
    state
        .iter()
        .filter(|row| row.schema_key == SCHEMA_KEY)
        .map(|row| {
            let pk = single_entity_pk(row.entity_pk.clone())?;
            let snapshot = parse_snapshot(&row.snapshot_content, std::slice::from_ref(&pk))?;
            Ok(PreviousSnapshot { pk, snapshot })
        })
        .collect()
}

fn assign_stable_pks(
    previous: &[PreviousSnapshot],
    current: Vec<SemEntitySnapshot>,
) -> (Vec<(String, SemEntitySnapshot)>, Vec<String>) {
    let mut matched_previous = HashSet::<usize>::new();
    let mut assigned = Vec::<(String, SemEntitySnapshot)>::new();

    for snapshot in current {
        let previous_index = find_previous_match(previous, &matched_previous, &snapshot);
        let pk = if let Some(index) = previous_index {
            matched_previous.insert(index);
            previous[index].pk.clone()
        } else {
            mint_entity_pk(&snapshot)
        };
        assigned.push((pk, snapshot));
    }

    let assigned_pks = assigned
        .iter()
        .map(|(pk, _)| pk.as_str())
        .collect::<HashSet<_>>();
    let tombstones = previous
        .iter()
        .enumerate()
        .filter(|(index, previous)| {
            !matched_previous.contains(index) && !assigned_pks.contains(previous.pk.as_str())
        })
        .map(|(_, previous)| previous.pk.clone())
        .collect();

    (assigned, tombstones)
}

fn find_previous_match(
    previous: &[PreviousSnapshot],
    matched_previous: &HashSet<usize>,
    current: &SemEntitySnapshot,
) -> Option<usize> {
    for (index, candidate) in previous.iter().enumerate() {
        if matched_previous.contains(&index) {
            continue;
        }
        if candidate.snapshot.sem_id == current.sem_id
            && same_entity_kind(&candidate.snapshot, current)
            && plausible_same_sem_id_match(&candidate.snapshot, current)
        {
            return Some(index);
        }
    }

    for (index, candidate) in previous.iter().enumerate() {
        if matched_previous.contains(&index) {
            continue;
        }
        if same_entity_kind(&candidate.snapshot, current)
            && hashes_match(
                candidate.snapshot.content_hash.as_deref(),
                current.content_hash.as_deref(),
            )
        {
            return Some(index);
        }
    }

    for (index, candidate) in previous.iter().enumerate() {
        if matched_previous.contains(&index) {
            continue;
        }
        if same_entity_kind(&candidate.snapshot, current)
            && hashes_match(
                candidate.snapshot.structural_hash.as_deref(),
                current.structural_hash.as_deref(),
            )
        {
            return Some(index);
        }
    }

    unique_fuzzy_match(previous, matched_previous, current)
}

fn same_entity_kind(left: &SemEntitySnapshot, right: &SemEntitySnapshot) -> bool {
    left.entity_type == right.entity_type
}

fn hashes_match(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if !left.is_empty() && left == right)
}

fn plausible_same_sem_id_match(left: &SemEntitySnapshot, right: &SemEntitySnapshot) -> bool {
    if left.entity_type == "gap" {
        return true;
    }
    if hashes_match(left.content_hash.as_deref(), right.content_hash.as_deref())
        || hashes_match(
            left.structural_hash.as_deref(),
            right.structural_hash.as_deref(),
        )
    {
        return true;
    }
    content_similarity(left, right) >= 0.45
}

fn unique_fuzzy_match(
    previous: &[PreviousSnapshot],
    matched_previous: &HashSet<usize>,
    current: &SemEntitySnapshot,
) -> Option<usize> {
    const THRESHOLD: f64 = 0.45;
    const MIN_MARGIN: f64 = 0.05;

    let mut best: Option<(usize, f64)> = None;
    let mut second_best = 0.0;
    for (index, candidate) in previous.iter().enumerate() {
        if matched_previous.contains(&index) || !same_entity_kind(&candidate.snapshot, current) {
            continue;
        }

        let score = content_similarity(&candidate.snapshot, current);
        match best {
            Some((_, best_score)) if score > best_score => {
                second_best = best_score;
                best = Some((index, score));
            }
            Some(_) => {
                second_best = second_best.max(score);
            }
            None => {
                best = Some((index, score));
            }
        }
    }

    let (best_index, best_score) = best?;
    if best_score >= THRESHOLD && best_score - second_best >= MIN_MARGIN {
        Some(best_index)
    } else {
        None
    }
}

fn content_similarity(left: &SemEntitySnapshot, right: &SemEntitySnapshot) -> f64 {
    let Some(left_content) = left.content.as_deref() else {
        return 0.0;
    };
    let Some(right_content) = right.content.as_deref() else {
        return 0.0;
    };
    let left_tokens = left_content.split_whitespace().collect::<HashSet<_>>();
    let right_tokens = right_content.split_whitespace().collect::<HashSet<_>>();
    if left_tokens.is_empty() || right_tokens.is_empty() {
        return 0.0;
    }
    let intersection = left_tokens.intersection(&right_tokens).count();
    let union = left_tokens.union(&right_tokens).count();
    intersection as f64 / union as f64
}

fn mint_entity_pk(snapshot: &SemEntitySnapshot) -> String {
    let content_hash = snapshot
        .content_hash
        .as_deref()
        .unwrap_or("no-content-hash");
    format!("sem::{}::{content_hash}", snapshot.sem_id)
}

fn parse_snapshot(raw: &str, entity_pk: &[String]) -> Result<SemEntitySnapshot, PluginError> {
    let mut snapshot: SemEntitySnapshot = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid sem_entity snapshot_content: {error}"))
    })?;
    validate_single_entity_pk(entity_pk)?;
    if snapshot.sem_id.is_empty() {
        snapshot.sem_id.clone_from(&snapshot.id);
    }
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
    detect_changes_from_files(Vec::new(), before, after)
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
