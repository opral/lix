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
use lix_order_key::OrderKey;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
    rows: Vec<Row>,
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
    let base = before.rows.as_slice();
    let (old_for_new, new_for_old) = match_diff_rows(
        base,
        file_rows,
        imara_diff_runs(base.iter().map(|row| &row.cells), file_rows.iter()),
    );
    let inserted_ids = old_for_new
        .iter()
        .map(|old_index| match old_index {
            Some(_) => None,
            None => Some(Uuid::now_v7().to_string()),
        })
        .collect::<Vec<_>>();
    let mut changes = Vec::new();

    for base_index in (0..base.len()).filter(|index| new_for_old[*index].is_none()) {
        changes.push(DetectedChange {
            entity_pk: vec![base[base_index].id.clone()],
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: None,
            metadata: None,
        });
    }
    detect_row_upsert_changes(base, file_rows, &old_for_new, &inserted_ids, &mut changes)?;

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
    if old_for_new
        .iter()
        .copied()
        .flatten()
        .map(|old_index| &base[old_index].order_key)
        .is_sorted_by(|previous, current| previous < current)
    {
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

    let order_keys =
        OrderKey::evenly_between(previous_order_key.as_ref(), next_order_key, pending.len())
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
        let mut rows = Vec::new();
        let mut row_ids = HashSet::new();
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
                    if !row_ids.insert(entity_pk.clone()) {
                        return Err(PluginError::InvalidInput(format!(
                            "duplicate entity_pk '{entity_pk}' for schema_key '{ROW_SCHEMA_KEY}'"
                        )));
                    }
                    let snapshot = parse_row_snapshot(&change.snapshot_content, &entity_pk)?;
                    rows.push(Row {
                        id: entity_pk,
                        order_key: snapshot.order_key,
                        cells: snapshot.cells,
                    });
                }
                _ => {}
            }
        }

        rows.sort_by(|a, b| a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id)));

        Ok(Self {
            rows,
            dialect: dialect.unwrap_or_default(),
            table_present: dialect.is_some(),
        })
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

    #[test]
    fn projection_rejects_duplicate_row_ids() {
        let row = EntityState {
            entity_pk: vec!["row:duplicate".to_string()],
            schema_key: ROW_SCHEMA_KEY.to_string(),
            snapshot_content: serde_json::json!({
                "id": "row:duplicate",
                "order_key": "80",
                "cells": ["value"],
            })
            .to_string(),
            metadata: None,
        };

        let error = Projection::from_entity_state(vec![row.clone(), row].into_iter())
            .expect_err("duplicate row ids must be rejected");

        assert!(matches!(
            error,
            PluginError::InvalidInput(message)
                if message == "duplicate entity_pk 'row:duplicate' for schema_key 'csv_row'"
        ));
    }

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
                .rows
                .into_iter()
                .map(|row| row.cells)
                .collect::<Vec<_>>();
            assert_eq!(applied_rows, file_rows);
        }
    }

    #[test]
    fn fuzz_detect_changes_reorders_rows_without_changing_ids() {
        let mut rng = SmallRng::seed_from_u64(1);

        for _ in 0..100_000 {
            let base_rows = random_csv(&mut rng);
            let before = projection_from_rows(base_rows.clone());
            let mut reordered_rows = base_rows.clone();
            shuffle(&mut reordered_rows, &mut rng);

            let changes =
                detect_changes_for_rows(&before, &reordered_rows, CsvDialect::default()).unwrap();

            for change in &changes {
                assert_eq!(
                    change.schema_key, ROW_SCHEMA_KEY,
                    "reordering rows should not change the table snapshot"
                );

                let entity_pk = single_entity_pk(change.entity_pk.clone()).unwrap();
                let before_row = before
                    .rows
                    .iter()
                    .find(|row| row.id == entity_pk)
                    .expect("reordering rows should only update existing row ids");
                let snapshot_content = change
                    .snapshot_content
                    .as_deref()
                    .expect("reordering rows should not delete existing row entities");
                let after_row = parse_row_snapshot(snapshot_content, &entity_pk).unwrap();

                assert_eq!(
                    after_row.cells, before_row.cells,
                    "reordering rows should only update order keys"
                );
                assert_ne!(
                    after_row.order_key, before_row.order_key,
                    "reordering rows should not emit unchanged row snapshots"
                );
            }

            let mut applied = before;
            for change in changes {
                apply_entity_change(&mut applied, change).unwrap();
            }

            let applied_rows = applied
                .rows
                .into_iter()
                .map(|row| row.cells)
                .collect::<Vec<_>>();
            assert_eq!(applied_rows, reordered_rows);
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

    fn shuffle<T>(items: &mut [T], rng: &mut (impl Rng + ?Sized)) {
        for index in (1..items.len()).rev() {
            items.swap(index, rng.random_range(0..=index));
        }
    }

    fn projection_from_rows(rows: Vec<Vec<String>>) -> Projection {
        let ids = (0..rows.len())
            .map(|offset| format!("row:{offset}"))
            .collect::<Vec<_>>();
        let order_keys = OrderKey::evenly_between(None, None, ids.len()).unwrap();
        let rows = rows
            .into_iter()
            .zip(ids.into_iter().zip(order_keys))
            .map(|(cells, (id, order_key))| Row {
                id,
                order_key,
                cells,
            })
            .collect();

        Projection {
            rows,
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
                    let snapshot = parse_row_snapshot(&raw, &entity_pk)?;
                    let row = Row {
                        id: entity_pk.clone(),
                        order_key: snapshot.order_key,
                        cells: snapshot.cells,
                    };
                    if let Some(existing) =
                        projection.rows.iter_mut().find(|row| row.id == entity_pk)
                    {
                        *existing = row;
                    } else {
                        projection.rows.push(row);
                    }
                    projection.rows.sort_by(|a, b| {
                        a.order_key.cmp(&b.order_key).then_with(|| a.id.cmp(&b.id))
                    });
                } else {
                    projection.rows.retain(|row| row.id != entity_pk);
                }
            }
            _ => {}
        }

        Ok(())
    }
}
