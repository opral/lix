mod bindings {
    #![expect(clippy::same_length_and_capacity)]
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

mod csv;
mod diff;
mod order_key;
pub mod schemas;

use crate::csv::{
    CsvDialect, parse_file, parse_table_snapshot, render_projection, table_upsert_change,
};
use crate::diff::{Op, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::order_key::OrderKey;
use itertools::Itertools;
use rand::Rng;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::str;

pub const ROOT_ENTITY_PK: &str = "root";
pub const TABLE_SCHEMA_KEY: &str = schemas::TABLE_SCHEMA_KEY;
pub const ROW_SCHEMA_KEY: &str = schemas::ROW_SCHEMA_KEY;

pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

#[derive(Clone, Copy, Debug)]
pub struct CsvPlugin;
#[cfg(target_family = "wasm")]
export!(CsvPlugin);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projection {
    rows_by_id: BTreeMap<String, RowSnapshot>,
    dialect: CsvDialect,
    table_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    id: String,
    order_key: OrderKey,
    cells: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowSnapshot {
    order_key: OrderKey,
    cells: Vec<String>,
}

impl Plugin for CsvPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = Projection::from_entity_state(state.into_iter())?;
        let (file_rows, after_dialect) = parse_file(&file)?;
        detect_changes_for_rows(&before, &file_rows, after_dialect)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let projection = Projection::from_entity_state(state.into_iter())?;
        render_projection(&projection)
    }
}

fn detect_changes_for_rows(
    before: &Projection,
    file_rows: &[Vec<String>],
    after_dialect: CsvDialect,
) -> Result<Vec<DetectedChange>, PluginError> {
    let base = before.to_rows();
    let op_runs = imara_diff_runs(base.iter().map(|row| &row.cells), file_rows.iter());
    let mut changes = Vec::new();
    let mut rng = rand::rng();
    let mut base_index = 0;
    let mut file_index = 0;
    let mut previous_order_key = None;

    for run in op_runs {
        match run.op {
            Op::Equal => {
                for _ in 0..run.len {
                    previous_order_key = Some(base[base_index].order_key);
                    base_index += 1;
                    file_index += 1;
                }
            }
            Op::Replace => {
                for _ in 0..run.len {
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
            }
            Op::Delete => {
                for _ in 0..run.len {
                    changes.push(DetectedChange {
                        entity_pk: vec![base[base_index].id.clone()],
                        schema_key: ROW_SCHEMA_KEY.to_string(),
                        snapshot_content: None,
                        metadata: None,
                    });
                    base_index += 1;
                }
            }
            Op::Insert => {
                let next_order_key = base.get(base_index).map(|row| row.order_key);
                let order_keys =
                    OrderKey::evenly_between(previous_order_key, next_order_key, run.len);
                for order_key in order_keys {
                    let id = RowId::random(&mut rng).to_entity_pk();
                    changes.push(row_upsert_change(&id, order_key, &file_rows[file_index])?);
                    previous_order_key = Some(order_key);
                    file_index += 1;
                }
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

fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(entity_pk.remove(0))
}

impl Projection {
    fn from_entity_state(changes: impl Iterator<Item = EntityState>) -> Result<Self, PluginError> {
        let mut rows_by_id = BTreeMap::new();
        let mut dialect = None;

        for change in changes {
            match change.schema_key.as_str() {
                TABLE_SCHEMA_KEY => {
                    let entity_pk = single_entity_pk(change.entity_pk)?;
                    if entity_pk != ROOT_ENTITY_PK {
                        return Err(PluginError::InvalidInput(format!(
                            "unsupported entity_pk '{entity_pk}' for schema_key '{TABLE_SCHEMA_KEY}', expected '{ROOT_ENTITY_PK}'"
                        )));
                    }
                    if dialect.is_some() {
                        return Err(PluginError::InvalidInput(format!(
                            "duplicate entity_pk '{ROOT_ENTITY_PK}' for schema_key '{TABLE_SCHEMA_KEY}'"
                        )));
                    }
                    dialect = Some(parse_table_snapshot(&change.snapshot_content)?.dialect);
                }
                ROW_SCHEMA_KEY => {
                    let entity_pk = single_entity_pk(change.entity_pk)?;
                    match rows_by_id.entry(entity_pk) {
                        Entry::Occupied(entry) => {
                            return Err(PluginError::InvalidInput(format!(
                                "duplicate entity_pk '{}' for schema_key '{ROW_SCHEMA_KEY}'",
                                entry.key()
                            )));
                        }
                        Entry::Vacant(entry) => {
                            let row = parse_row_snapshot(&change.snapshot_content, entry.key())?;
                            entry.insert(row);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Self {
            rows_by_id,
            dialect: dialect.unwrap_or_default(),
            table_present: dialect.is_some(),
        })
    }

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
    order_key: OrderKey,
    cells: &[String],
) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": id,
        "order_key": order_key.to_snapshot_string(),
        "cells": cells,
    }))
    .map_err(|error| PluginError::Internal(format!("failed to serialize CSV row: {error}")))?;

    Ok(DetectedChange {
        entity_pk: vec![id.to_string()],
        schema_key: ROW_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
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
) -> Result<OrderKey, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv row snapshot for entity_pk '{entity_pk}' must contain string 'order_key'"
        ))
    })?;

    OrderKey::from_snapshot_string(raw).map_err(|message| {
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
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use std::collections::BTreeMap;

    #[test]
    fn fuzz_detect_changes_round_trips_rows() {
        let mut rng = SmallRng::seed_from_u64(0);

        for _ in 0..100_000 {
            let base_rows = random_csv(&mut rng);
            let file_rows = random_csv(&mut rng);
            let before = projection_from_rows(base_rows.clone());

            let changes =
                detect_changes_for_rows(&before, &file_rows, CsvDialect::default()).unwrap();

            let rows_are_equal = base_rows == file_rows;
            assert!(
                changes.is_empty() == rows_are_equal,
                "changes emptiness did not match row equality: changes={}, rows_are_equal={rows_are_equal}",
                changes.len()
            );

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_rows = applied
                .to_rows()
                .into_iter()
                .map(|row| row.cells)
                .collect::<Vec<_>>();
            assert_eq!(applied_rows, file_rows);
        }
    }

    fn random_csv(rng: &mut (impl Rng + ?Sized)) -> Vec<Vec<String>> {
        let random_cell_alphabet_len: u8 = rng.random_range(1..=6);
        let width = rng.random_range(1..=10);
        let height = rng.random_range(0..=10);

        (0..height)
            .map(|_| {
                (0..width)
                    .map(|_| {
                        let offset = rng.random_range(0..random_cell_alphabet_len);
                        char::from(b'a' + offset).to_string()
                    })
                    .collect()
            })
            .collect()
    }

    fn projection_from_rows(rows: Vec<Vec<String>>) -> Projection {
        let order_keys = OrderKey::evenly_between(None, None, rows.len());
        let rows_by_id = rows
            .into_iter()
            .zip(order_keys)
            .enumerate()
            .map(|(offset, (cells, order_key))| {
                (format!("row:{offset}"), RowSnapshot { order_key, cells })
            })
            .collect::<BTreeMap<_, _>>();

        Projection {
            rows_by_id,
            dialect: CsvDialect::default(),
            table_present: true,
        }
    }

    fn apply_entity_change(
        projection: &mut Projection,
        change: DetectedChange,
    ) -> Result<(), PluginError> {
        match change.schema_key.as_str() {
            TABLE_SCHEMA_KEY => {
                if let Some(raw) = change.snapshot_content {
                    projection.dialect = parse_table_snapshot(&raw)?.dialect;
                    projection.table_present = true;
                } else {
                    projection.dialect = CsvDialect::default();
                    projection.table_present = false;
                }
            }
            ROW_SCHEMA_KEY => {
                let entity_pk = single_entity_pk(change.entity_pk)?;
                if let Some(raw) = change.snapshot_content {
                    let row = parse_row_snapshot(&raw, &entity_pk)?;
                    projection.rows_by_id.insert(entity_pk, row);
                } else {
                    projection.rows_by_id.remove(&entity_pk);
                }
            }
            _ => {}
        }

        Ok(())
    }
}
