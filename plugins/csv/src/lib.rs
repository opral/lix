// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod csv;
mod diff;
pub mod schemas;

use crate::csv::{
    CsvDialect, parse_file, parse_table_snapshot, render_projection, table_upsert_change,
};
use crate::diff::{DiffRun, imara_diff_runs};
pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use itertools::Itertools;
use lix_order_key::OrderKey;
use serde_json::Value;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::str;
use uuid::Uuid;

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReplaceRun {
    old: Range<usize>,
    new: Range<usize>,
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
    let (old_for_new, new_for_old) = match_diff_rows(
        &base,
        file_rows,
        imara_diff_runs(base.iter().map(|row| &row.cells), file_rows.iter()),
    );
    let inserted_ids = inserted_row_ids(&old_for_new);
    let mut changes = Vec::new();

    for base_index in (0..base.len()).filter(|index| new_for_old[*index].is_none()) {
        changes.push(DetectedChange {
            entity_pk: vec![base[base_index].id.clone()],
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: None,
            metadata: None,
        });
    }
    detect_row_upsert_changes(&base, file_rows, &old_for_new, &inserted_ids, &mut changes)?;

    if before.dialect != after_dialect
        || (!before.table_present
            && (!file_rows.is_empty() || after_dialect != CsvDialect::default()))
    {
        changes.push(table_upsert_change(after_dialect)?);
    }

    Ok(changes)
}

fn match_diff_rows(
    base: &[Row],
    file_rows: &[Vec<String>],
    diff_runs: impl IntoIterator<Item = DiffRun>,
) -> (Vec<Option<usize>>, Vec<Option<usize>>) {
    let mut old_for_new = vec![None; file_rows.len()];
    let mut new_for_old = vec![None; base.len()];
    let mut replace_runs = Vec::new();
    let mut base_index = 0usize;
    let mut file_index = 0usize;

    for run in diff_runs {
        match run {
            DiffRun::Equal { len } => {
                for (old_index, new_index) in
                    (base_index..base_index + len).zip(file_index..file_index + len)
                {
                    old_for_new[new_index] = Some(old_index);
                    new_for_old[old_index] = Some(new_index);
                }
                base_index += len;
                file_index += len;
            }
            DiffRun::Replace { old, new } => {
                replace_runs.push(ReplaceRun {
                    old: base_index..base_index + old,
                    new: file_index..file_index + new,
                });
                base_index += old;
                file_index += new;
            }
        }
    }

    let old_replace_len = replace_runs.iter().map(|run| run.old.len()).sum();
    let mut old_rows_by_cells = HashMap::<&[String], Vec<usize>>::with_capacity(old_replace_len);
    for run in replace_runs.iter().rev() {
        for old_index in run.old.clone().rev() {
            old_rows_by_cells
                .entry(base[old_index].cells.as_slice())
                .or_default()
                .push(old_index);
        }
    }

    for run in &replace_runs {
        for new_index in run.new.clone() {
            let Some(old_indices) = old_rows_by_cells.get_mut(file_rows[new_index].as_slice())
            else {
                continue;
            };
            let Some(old_index) = old_indices.pop() else {
                continue;
            };
            old_for_new[new_index] = Some(old_index);
            new_for_old[old_index] = Some(new_index);
        }
    }

    for run in &replace_runs {
        let mut old_index = run.old.start;
        let mut new_index = run.new.start;
        loop {
            while old_index < run.old.end && new_for_old[old_index].is_some() {
                old_index += 1;
            }
            while new_index < run.new.end && old_for_new[new_index].is_some() {
                new_index += 1;
            }
            if old_index == run.old.end || new_index == run.new.end {
                break;
            }
            old_for_new[new_index] = Some(old_index);
            new_for_old[old_index] = Some(new_index);
            old_index += 1;
            new_index += 1;
        }
    }

    (old_for_new, new_for_old)
}

fn inserted_row_ids(old_for_new: &[Option<usize>]) -> Vec<Option<String>> {
    old_for_new
        .iter()
        .map(|old_index| match old_index {
            Some(_) => None,
            None => Some(Uuid::now_v7().to_string()),
        })
        .collect()
}

fn row_id_for_new<'a>(
    base: &'a [Row],
    old_for_new: &[Option<usize>],
    inserted_ids: &'a [Option<String>],
    new_index: usize,
) -> &'a str {
    match old_for_new[new_index] {
        Some(old_index) => &base[old_index].id,
        None => inserted_ids[new_index]
            .as_deref()
            .expect("inserted row id should exist"),
    }
}

fn detect_row_upsert_changes(
    base: &[Row],
    file_rows: &[Vec<String>],
    old_for_new: &[Option<usize>],
    inserted_ids: &[Option<String>],
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    let keep_order_key = kept_order_key_indices(base, old_for_new);
    let mut previous_order_key = None::<OrderKey>;
    let mut pending = Vec::new();

    for new_index in 0..file_rows.len() {
        if keep_order_key[new_index] {
            let old_index =
                old_for_new[new_index].expect("kept order key should belong to an existing row");
            let order_key = &base[old_index].order_key;
            flush_generated_row_upserts(
                &mut pending,
                &mut previous_order_key,
                Some(order_key),
                base,
                file_rows,
                old_for_new,
                inserted_ids,
                changes,
            )?;
            if base[old_index].cells.as_slice() != file_rows[new_index].as_slice() {
                changes.push(row_upsert_change(
                    row_id_for_new(base, old_for_new, inserted_ids, new_index),
                    order_key,
                    &file_rows[new_index],
                )?);
            }
            previous_order_key = Some(order_key.clone());
        } else {
            pending.push(new_index);
        }
    }

    flush_generated_row_upserts(
        &mut pending,
        &mut previous_order_key,
        None,
        base,
        file_rows,
        old_for_new,
        inserted_ids,
        changes,
    )
}

fn kept_order_key_indices(base: &[Row], old_for_new: &[Option<usize>]) -> Vec<bool> {
    let mut keep = vec![false; old_for_new.len()];
    if order_keys_are_strictly_increasing(base, old_for_new) {
        for (new_index, old_index) in old_for_new.iter().enumerate() {
            keep[new_index] = old_index.is_some();
        }
        return keep;
    }

    let mut pile_tops = Vec::<usize>::new();
    let mut predecessors = vec![None; old_for_new.len()];

    for (new_index, old_index) in old_for_new.iter().copied().enumerate() {
        let Some(old_index) = old_index else {
            continue;
        };
        let order_key = &base[old_index].order_key;
        let pile = pile_tops
            .partition_point(|top_index| old_order_key(base, old_for_new, *top_index) < order_key);
        if pile != 0 {
            predecessors[new_index] = Some(pile_tops[pile - 1]);
        }
        if pile == pile_tops.len() {
            pile_tops.push(new_index);
        } else if old_order_key(base, old_for_new, pile_tops[pile]) > order_key {
            pile_tops[pile] = new_index;
        }
    }

    let Some(mut current) = pile_tops.last().copied() else {
        return keep;
    };
    loop {
        keep[current] = true;
        let Some(previous) = predecessors[current] else {
            break;
        };
        current = previous;
    }
    keep
}

fn order_keys_are_strictly_increasing(base: &[Row], old_for_new: &[Option<usize>]) -> bool {
    let mut previous_order_key = None::<&OrderKey>;

    for old_index in old_for_new.iter().copied().flatten() {
        let order_key = &base[old_index].order_key;
        if previous_order_key.is_some_and(|previous| previous >= order_key) {
            return false;
        }
        previous_order_key = Some(order_key);
    }

    true
}

fn old_order_key<'a>(
    base: &'a [Row],
    old_for_new: &[Option<usize>],
    new_index: usize,
) -> &'a OrderKey {
    let old_index = old_for_new[new_index].expect("old order key should belong to existing row");
    &base[old_index].order_key
}

fn flush_generated_row_upserts(
    pending: &mut Vec<usize>,
    previous_order_key: &mut Option<OrderKey>,
    next_order_key: Option<&OrderKey>,
    base: &[Row],
    file_rows: &[Vec<String>],
    old_for_new: &[Option<usize>],
    inserted_ids: &[Option<String>],
    changes: &mut Vec<DetectedChange>,
) -> Result<(), PluginError> {
    if pending.is_empty() {
        return Ok(());
    }

    let ids = pending
        .iter()
        .map(|new_index| row_id_for_new(base, old_for_new, inserted_ids, *new_index).to_string())
        .collect_vec();
    let order_keys = OrderKey::evenly_between(previous_order_key.as_ref(), next_order_key, &ids)
        .map_err(PluginError::Internal)?;

    for (new_index, order_key) in pending.drain(..).zip(order_keys) {
        changes.push(row_upsert_change(
            row_id_for_new(base, old_for_new, inserted_ids, new_index),
            &order_key,
            &file_rows[new_index],
        )?);
        *previous_order_key = Some(order_key);
    }

    Ok(())
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
                order_key: row.order_key.clone(),
                cells: row.cells.clone(),
            })
            .collect_vec();
        rows.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));
        rows
    }
}

fn row_upsert_change(
    id: &str,
    order_key: &OrderKey,
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
        let ids = (0..rows.len())
            .map(|offset| format!("row:{offset}"))
            .collect::<Vec<_>>();
        let order_keys = OrderKey::evenly_between(None, None, &ids).unwrap();
        let rows_by_id = rows
            .into_iter()
            .zip(ids.into_iter().zip(order_keys))
            .map(|(cells, (id, order_key))| (id, RowSnapshot { order_key, cells }))
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
