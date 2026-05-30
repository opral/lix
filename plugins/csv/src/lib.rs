#[expect(clippy::same_length_and_capacity)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

mod csv;
mod order_key;
pub mod schemas;

use crate::csv::{
    CsvDialect, TableSnapshot, parse_file, parse_table_snapshot, render_projection,
    table_upsert_change,
};
use crate::exports::lix::plugin::api::{
    ActiveStateRow, DetectStateContext, EntityChange, File, Guest as Plugin, PluginError,
};
use crate::order_key::FractionalIndex;
use itertools::Itertools;
use rand::Rng;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::str;
use std::{cmp, iter};

pub const ROOT_ENTITY_PK: &str = "root";
pub const TABLE_SCHEMA_KEY: &str = schemas::TABLE_SCHEMA_KEY;
pub const ROW_SCHEMA_KEY: &str = schemas::ROW_SCHEMA_KEY;

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

pub use crate::exports::lix::plugin::api::{
    ActiveStateRow as PluginActiveStateRow, DetectStateContext as PluginDetectStateContext,
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

#[derive(Clone, Copy, Debug)]
pub struct CsvPlugin;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projection {
    rows_by_id: BTreeMap<String, RowSnapshot>,
    dialect: CsvDialect,
    table_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    order_key: FractionalIndex,
    cells: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowSnapshot {
    order_key: FractionalIndex,
    cells: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Equal,
    Replace,
    Insert,
    Delete,
}

impl Plugin for CsvPlugin {
    fn detect_changes(
        state: DetectStateContext,
        file: File,
    ) -> Result<Vec<EntityChange>, PluginError> {
        let before = projection_from_entity_changes(
            empty_file(),
            entity_changes_from_active_state(state.active_state),
        )?;
        detect_changes_from_projection(&before, &file)
    }

    fn render(state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
        let projection = projection_from_entity_changes(
            empty_file(),
            entity_changes_from_active_state(state.active_state),
        )?;
        render_projection(&projection)
    }
}

fn detect_changes_from_projection(
    before: &Projection,
    after: &File,
) -> Result<Vec<EntityChange>, PluginError> {
    let base = before.to_rows();
    let (file_rows, after_dialect) = parse_file(after)?;
    let ops = diff_by(&base, &file_rows, |row, file_row| row.cells == *file_row).collect_vec();
    let mut changes = Vec::new();
    let mut rng = rand::rng();
    let mut base_index = 0;
    let mut file_index = 0;
    let mut previous_order_key = None;

    for op in ops {
        match op {
            Op::Equal => {
                previous_order_key = Some(base[base_index].order_key);
                base_index += 1;
                file_index += 1;
            }
            Op::Replace => {
                let row = &base[base_index];
                changes.push(row_upsert_change(
                    &row.id,
                    row.order_key,
                    &file_rows[file_index],
                )?);
                previous_order_key = Some(row.order_key);
                base_index += 1;
                file_index += 1;
            }
            Op::Delete => {
                changes.push(EntityChange {
                    entity_pk: base[base_index].id.clone(),
                    schema_key: ROW_SCHEMA_KEY.to_string(),
                    snapshot_content: None,
                });
                base_index += 1;
            }
            Op::Insert => {
                let next_order_key = base.get(base_index).map(|row| row.order_key);
                let order_key = FractionalIndex::between(previous_order_key, next_order_key)?;
                let id = RowId::random(&mut rng).to_entity_pk();
                changes.push(row_upsert_change(&id, order_key, &file_rows[file_index])?);
                previous_order_key = Some(order_key);
                file_index += 1;
            }
        }
    }

    if before.dialect != after_dialect
        || (!before.table_present
            && (!file_rows.is_empty() || after_dialect != CsvDialect::default()))
    {
        changes.push(table_upsert_change(after_dialect)?);
    }

    Ok(changes)
}

fn entity_changes_from_active_state(rows: Vec<ActiveStateRow>) -> Vec<EntityChange> {
    rows.into_iter()
        .filter_map(|row| {
            Some(EntityChange {
                entity_pk: row.entity_pk,
                schema_key: row.schema_key?,
                snapshot_content: row.snapshot_content,
            })
        })
        .collect()
}

fn projection_from_entity_changes(
    file: File,
    changes: Vec<EntityChange>,
) -> Result<Projection, PluginError> {
    let mut projection = projection_from_file_with_index_ids(&file)?;
    let mut table_snapshot = None::<TableSnapshot>;
    let mut table_seen = false;
    let mut seen_row_change_ids = BTreeSet::<String>::new();

    for change in changes {
        match change.schema_key.as_str() {
            TABLE_SCHEMA_KEY => {
                if change.entity_pk != ROOT_ENTITY_PK {
                    return Err(PluginError::InvalidInput(format!(
                        "unsupported entity_pk '{}' for schema_key '{}', expected '{}'",
                        change.entity_pk, TABLE_SCHEMA_KEY, ROOT_ENTITY_PK
                    )));
                }
                if table_seen {
                    return Err(PluginError::InvalidInput(format!(
                        "duplicate entity_pk '{ROOT_ENTITY_PK}' for schema_key '{TABLE_SCHEMA_KEY}'"
                    )));
                }
                table_seen = true;
                let snapshot_present = change.snapshot_content.is_some();
                table_snapshot = Some(match change.snapshot_content {
                    Some(raw) => parse_table_snapshot(&raw)?,
                    None => TableSnapshot {
                        dialect: CsvDialect::default(),
                    },
                });
                projection.table_present = snapshot_present;
            }
            ROW_SCHEMA_KEY => {
                if !seen_row_change_ids.insert(change.entity_pk.clone()) {
                    return Err(PluginError::InvalidInput(format!(
                        "duplicate entity_pk '{}' for schema_key '{}'",
                        change.entity_pk, ROW_SCHEMA_KEY
                    )));
                }

                match change.snapshot_content {
                    Some(raw) => {
                        let row = parse_row_snapshot(&raw, &change.entity_pk)?;
                        projection.rows_by_id.insert(change.entity_pk, row);
                    }
                    None => {
                        projection.rows_by_id.remove(&change.entity_pk);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(table) = table_snapshot {
        projection.dialect = table.dialect;
    }

    Ok(projection)
}

fn projection_from_file_with_index_ids(file: &File) -> Result<Projection, PluginError> {
    let (rows, dialect) = parse_file(file)?;
    let len = rows.len();
    let rows_by_id = rows
        .into_iter()
        .enumerate()
        .map(|(offset, cells)| {
            (
                format!("row:{offset}"),
                RowSnapshot {
                    order_key: FractionalIndex::evenly_spaced(offset, len),
                    cells,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(Projection {
        rows_by_id,
        dialect,
        table_present: false,
    })
}

impl Projection {
    fn to_rows(&self) -> Vec<Row> {
        let mut rows = self
            .rows_by_id
            .iter()
            .map(|(id, row)| Row {
                id: id.clone(),
                order_key: row.order_key,
                cells: row.cells.clone(),
            })
            .collect_vec();
        rows.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));
        rows
    }
}

fn row_upsert_change(
    id: &str,
    order_key: FractionalIndex,
    cells: &[String],
) -> Result<EntityChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": id,
        "order_key": order_key.to_snapshot_string(),
        "cells": cells,
    }))
    .map_err(|error| PluginError::Internal(format!("failed to serialize CSV row: {error}")))?;

    Ok(EntityChange {
        entity_pk: id.to_string(),
        schema_key: ROW_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
    })
}

fn parse_row_snapshot(raw: &str, entity_pk: &str) -> Result<RowSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!(
            "invalid csv row snapshot_content for entity_pk '{entity_pk}': {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv row snapshot_content for entity_pk '{entity_pk}' must be an object"
        ))
    })?;
    reject_unknown_fields(object.keys(), &["id", "order_key", "cells"], "csv row")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv row snapshot for entity_pk '{entity_pk}' must contain string 'id'"
        ))
    })?;
    if id != entity_pk {
        return Err(PluginError::InvalidInput(format!(
            "csv row snapshot id '{id}' does not match entity_pk '{entity_pk}'"
        )));
    }
    if id.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "csv row snapshot id for entity_pk '{entity_pk}' must not be empty"
        )));
    }

    let order_key = parse_order_key_snapshot(object.get("order_key"), entity_pk)?;

    let cell_values = object
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            PluginError::InvalidInput(format!(
                "csv row snapshot for entity_pk '{entity_pk}' must contain array 'cells'"
            ))
        })?;
    let cells = cell_values
        .iter()
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                PluginError::InvalidInput(format!(
                    "csv row cells for entity_pk '{entity_pk}' must contain strings"
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RowSnapshot { order_key, cells })
}

fn reject_unknown_fields<'a>(
    keys: impl Iterator<Item = &'a String>,
    allowed: &[&str],
    label: &str,
) -> Result<(), PluginError> {
    for key in keys {
        if !allowed.contains(&key.as_str()) {
            return Err(PluginError::InvalidInput(format!(
                "{label} snapshot contains unsupported field '{key}'"
            )));
        }
    }
    Ok(())
}

fn parse_order_key_snapshot(
    value: Option<&Value>,
    entity_pk: &str,
) -> Result<FractionalIndex, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv row snapshot for entity_pk '{entity_pk}' must contain string 'order_key'"
        ))
    })?;

    FractionalIndex::from_snapshot_string(raw).map_err(|message| {
        PluginError::InvalidInput(format!(
            "invalid csv row order_key for entity_pk '{entity_pk}': {message}"
        ))
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct RowId([u8; 8]);

impl RowId {
    fn random(rng: &mut impl Rng) -> Self {
        let mut bytes = [0_u8; 8];
        rng.fill(&mut bytes);
        Self(bytes)
    }

    fn to_entity_pk(self) -> String {
        format!("row:{}", bytes_to_hex(&self.0))
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(hex_char(byte >> 4));
        output.push(hex_char(byte & 0x0f));
    }
    output
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '?',
    }
}

fn diff_by<'a, T, U>(
    a: &'a [T],
    b: &'a [U],
    mut eq: impl FnMut(&T, &U) -> bool + 'a,
) -> impl Iterator<Item = Op> + 'a {
    let prefix = a.iter().zip(b.iter()).take_while(|(a, b)| eq(a, b)).count();

    let a_rest = &a[prefix..];
    let b_rest = &b[prefix..];
    let suffix = a_rest
        .iter()
        .rev()
        .zip(b_rest.iter().rev())
        .take_while(|(a, b)| eq(a, b))
        .count()
        .min(a_rest.len())
        .min(b_rest.len());

    let a_mid = a.len() - prefix - suffix;
    let b_mid = b.len() - prefix - suffix;
    let replace = cmp::min(a_mid, b_mid);

    iter::empty()
        .chain((0..prefix).map(|_| Op::Equal))
        .chain(
            a[prefix..prefix + replace]
                .iter()
                .zip_eq(&b[prefix..prefix + replace])
                .map(move |(a, b)| if eq(a, b) { Op::Equal } else { Op::Replace }),
        )
        .chain((replace..a_mid).map(|_| Op::Delete))
        .chain((replace..b_mid).map(|_| Op::Insert))
        .chain((0..suffix).map(|_| Op::Equal))
}

fn empty_file() -> File {
    File {
        id: String::new(),
        path: String::new(),
        data: Vec::new(),
    }
}

#[cfg(target_family = "wasm")]
export!(CsvPlugin);
