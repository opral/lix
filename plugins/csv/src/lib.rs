#[expect(clippy::same_length_and_capacity)]
mod bindings {
    wit_bindgen::generate!({
        path: "../../packages/engine/wit",
        world: "plugin",
    });
}
pub use bindings::*;

use crate::exports::lix::plugin::api::{
    ActiveStateRow, DetectStateContext, EntityChange, File, Guest as Plugin, PluginError,
};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use csv::{ByteRecord, QuoteStyle, ReaderBuilder, Terminator, WriterBuilder};
use csv_nose::{Quote, Sniffer};
use encoding_rs::{CoderResult, Encoding};
use itertools::Itertools;
use rand::Rng;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::str;
use std::{cmp, iter};

pub mod schemas;

pub const ROOT_ENTITY_PK: &str = "root";
pub const DOCUMENT_SCHEMA_KEY: &str = schemas::DOCUMENT_SCHEMA_KEY;
pub const ROW_SCHEMA_KEY: &str = schemas::ROW_SCHEMA_KEY;

const MANIFEST_JSON: &str = include_str!("../manifest.json");

pub use crate::exports::lix::plugin::api::{
    ActiveStateRow as PluginActiveStateRow, DetectStateContext as PluginDetectStateContext,
    EntityChange as PluginEntityChange, File as PluginFile, PluginError as PluginApiError,
};

struct CsvPlugin;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Projection {
    rows_by_id: BTreeMap<String, RowSnapshot>,
    dialect: CsvDialect,
    document_present: bool,
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
struct CsvDialect {
    delimiter: u8,
    quote: Quote,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: Quote::Some(b'"'),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DocumentSnapshot {
    dialect: CsvDialect,
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
        let before = projection_from_active_state(state.active_state)?;
        detect_changes_from_projection(&before, &file)
    }

    fn render(state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
        render_state_context(state)
    }
}

pub fn detect_changes(before: Option<File>, after: File) -> Result<Vec<EntityChange>, PluginError> {
    let state_context = project_state_context_from_before(before)?;
    <CsvPlugin as Plugin>::detect_changes(state_context, after)
}

pub fn detect_changes_with_state_context(
    before: Option<File>,
    after: File,
    state_context: Option<PluginDetectStateContext>,
) -> Result<Vec<EntityChange>, PluginError> {
    let state_context = match state_context {
        Some(state_context) => state_context,
        None => project_state_context_from_before(before)?,
    };
    <CsvPlugin as Plugin>::detect_changes(state_context, after)
}

pub fn render(state_context: PluginDetectStateContext) -> Result<Vec<u8>, PluginError> {
    <CsvPlugin as Plugin>::render(state_context)
}

pub fn render_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    render_entity_changes(file, changes)
}

pub fn apply_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    render_changes(file, changes)
}

pub fn manifest_json() -> &'static str {
    MANIFEST_JSON
}

fn empty_state_context() -> PluginDetectStateContext {
    PluginDetectStateContext {
        active_state: Vec::new(),
    }
}

fn project_state_context_from_before(
    before: Option<File>,
) -> Result<PluginDetectStateContext, PluginError> {
    let Some(before_file) = before else {
        return Ok(empty_state_context());
    };

    let projection = projection_from_file_with_index_ids(&before_file)?;
    Ok(PluginDetectStateContext {
        active_state: projection
            .into_entity_changes()?
            .into_iter()
            .map(|row| PluginActiveStateRow {
                entity_pk: row.entity_pk,
                schema_key: Some(row.schema_key),
                snapshot_content: row.snapshot_content,
                file_id: None,
                plugin_key: None,
                branch_id: None,
                change_id: None,
                metadata: None,
                created_at: None,
                updated_at: None,
            })
            .collect(),
    })
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
                let order_key = fractional_index_between(previous_order_key, next_order_key)?;
                let id = RowId::random(&mut rng).to_entity_pk();
                changes.push(row_upsert_change(&id, order_key, &file_rows[file_index])?);
                previous_order_key = Some(order_key);
                file_index += 1;
            }
        }
    }

    if before.dialect != after_dialect
        || (!before.document_present
            && (!file_rows.is_empty() || after_dialect != CsvDialect::default()))
    {
        changes.push(document_upsert_change(after_dialect)?);
    }

    Ok(changes)
}

fn render_state_context(state: DetectStateContext) -> Result<Vec<u8>, PluginError> {
    render_active_state_rows(state.active_state)
}

fn render_active_state_rows(rows: Vec<ActiveStateRow>) -> Result<Vec<u8>, PluginError> {
    render_entity_changes(empty_file(), entity_changes_from_active_state(rows))
}

fn render_entity_changes(file: File, changes: Vec<EntityChange>) -> Result<Vec<u8>, PluginError> {
    let projection = projection_from_entity_changes(file, changes)?;
    render_projection(&projection)
}

fn projection_from_active_state(rows: Vec<ActiveStateRow>) -> Result<Projection, PluginError> {
    projection_from_entity_changes(empty_file(), entity_changes_from_active_state(rows))
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
    let mut document_snapshot = None::<DocumentSnapshot>;
    let mut document_seen = false;
    let mut seen_row_change_ids = BTreeSet::<String>::new();

    for change in changes {
        match change.schema_key.as_str() {
            DOCUMENT_SCHEMA_KEY => {
                if change.entity_pk != ROOT_ENTITY_PK {
                    return Err(PluginError::InvalidInput(format!(
                        "unsupported entity_pk '{}' for schema_key '{}', expected '{}'",
                        change.entity_pk, DOCUMENT_SCHEMA_KEY, ROOT_ENTITY_PK
                    )));
                }
                if document_seen {
                    return Err(PluginError::InvalidInput(format!(
                        "duplicate entity_pk '{ROOT_ENTITY_PK}' for schema_key '{DOCUMENT_SCHEMA_KEY}'"
                    )));
                }
                document_seen = true;
                let snapshot_present = change.snapshot_content.is_some();
                document_snapshot = Some(match change.snapshot_content {
                    Some(raw) => parse_document_snapshot(&raw)?,
                    None => DocumentSnapshot {
                        dialect: CsvDialect::default(),
                    },
                });
                projection.document_present = snapshot_present;
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

    if let Some(document) = document_snapshot {
        projection.dialect = document.dialect;
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
                    order_key: evenly_spaced_fractional_index(offset, len),
                    cells,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(Projection {
        rows_by_id,
        dialect,
        document_present: false,
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

    fn into_entity_changes(self) -> Result<Vec<EntityChange>, PluginError> {
        let mut changes = self
            .rows_by_id
            .into_iter()
            .map(|(id, row)| row_upsert_change(&id, row.order_key, &row.cells))
            .collect::<Result<Vec<_>, _>>()?;
        changes.push(document_upsert_change(self.dialect)?);
        Ok(changes)
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

fn document_upsert_change(dialect: CsvDialect) -> Result<EntityChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": ROOT_ENTITY_PK,
        "dialect": dialect_snapshot_content(dialect),
    }))
    .map_err(|error| PluginError::Internal(format!("failed to serialize CSV document: {error}")))?;

    Ok(EntityChange {
        entity_pk: ROOT_ENTITY_PK.to_string(),
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
    })
}

fn dialect_snapshot_content(dialect: CsvDialect) -> Value {
    serde_json::json!({
        "delimiter": byte_to_latin1_string(dialect.delimiter),
        "quote": match dialect.quote {
            Quote::None => Value::Null,
            Quote::Some(quote) => Value::from(byte_to_latin1_string(quote)),
        },
    })
}

fn parse_document_snapshot(raw: &str) -> Result<DocumentSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid csv document snapshot_content: {error}"))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput("csv document snapshot_content must be an object".to_string())
    })?;
    reject_unknown_fields(object.keys(), &["id", "dialect"], "csv document")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput("csv document snapshot must contain string 'id'".to_string())
    })?;
    if id != ROOT_ENTITY_PK {
        return Err(PluginError::InvalidInput(format!(
            "csv document snapshot id '{id}' does not match expected '{ROOT_ENTITY_PK}'"
        )));
    }

    let dialect = parse_dialect_snapshot(object.get("dialect").ok_or_else(|| {
        PluginError::InvalidInput("csv document snapshot must contain object 'dialect'".to_string())
    })?)?;

    Ok(DocumentSnapshot { dialect })
}

fn parse_dialect_snapshot(value: &Value) -> Result<CsvDialect, PluginError> {
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput("csv document dialect must be an object".to_string())
    })?;
    reject_unknown_fields(
        object.keys(),
        &["delimiter", "quote"],
        "csv document dialect",
    )?;

    let delimiter = parse_dialect_byte_string(object.get("delimiter"), "delimiter")?;
    let quote = match object.get("quote") {
        Some(Value::Null) => Quote::None,
        Some(value) => Quote::Some(parse_dialect_byte_string(Some(value), "quote")?),
        None => {
            return Err(PluginError::InvalidInput(
                "csv document dialect must contain 'quote'".to_string(),
            ));
        }
    };

    Ok(CsvDialect { delimiter, quote })
}

fn byte_to_latin1_string(byte: u8) -> String {
    char::from(byte).to_string()
}

fn parse_dialect_byte_string(value: Option<&Value>, field: &str) -> Result<u8, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv document dialect field '{field}' must be a single-byte string"
        ))
    })?;
    let mut chars = raw.chars();
    let Some(ch) = chars.next() else {
        return Err(PluginError::InvalidInput(format!(
            "csv document dialect field '{field}' must not be empty"
        )));
    };
    if chars.next().is_some() {
        return Err(PluginError::InvalidInput(format!(
            "csv document dialect field '{field}' must contain exactly one character"
        )));
    }
    u8::try_from(u32::from(ch)).map_err(|_| {
        PluginError::InvalidInput(format!(
            "csv document dialect field '{field}' must be in the range U+0000 through U+00FF"
        ))
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

fn parse_file(file: &File) -> Result<(Vec<Vec<String>>, CsvDialect), PluginError> {
    let decoded = decode(&file.data)?;
    let dialect = dialect_for_filename(Some(file.path.as_str()), &decoded);
    let rows = parse_rows(&decoded, dialect)?;
    Ok((rows, dialect))
}

fn decode(csv: &[u8]) -> Result<Cow<'_, str>, PluginError> {
    let (buf, encoding) = buffer_with_encoding(csv);
    if encoding == encoding_rs::UTF_8 {
        return Ok(String::from_utf8_lossy(buf));
    }
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let capacity = decoder.max_utf8_buffer_length(buf.len()).ok_or_else(|| {
        PluginError::Internal("CSV input is too large to decode as UTF-8".to_string())
    })?;
    let mut decoded = String::with_capacity(capacity);
    let (result, read, _replaced) = decoder.decode_to_string(buf, &mut decoded, true);
    if result != CoderResult::InputEmpty || read != buf.len() {
        return Err(PluginError::InvalidInput(
            "failed to decode complete CSV input".to_string(),
        ));
    }
    Ok(Cow::Owned(decoded))
}

fn parse_rows(csv: &str, dialect: CsvDialect) -> Result<Vec<Vec<String>>, PluginError> {
    let mut reader_builder = ReaderBuilder::new();
    reader_builder
        .flexible(true)
        .has_headers(false)
        .delimiter(dialect.delimiter);
    match dialect.quote {
        Quote::None => {
            reader_builder.quoting(false);
        }
        Quote::Some(quote) => {
            reader_builder.quoting(true).quote(quote);
        }
    }
    let mut reader = reader_builder.from_reader(csv.as_bytes());

    let mut rows = Vec::new();
    let mut record = ByteRecord::new();
    while reader.read_byte_record(&mut record).map_err(|error| {
        PluginError::InvalidInput(format!("failed to parse CSV input as records: {error}"))
    })? {
        let mut row = Vec::with_capacity(record.len());
        for field in &record {
            let field = str::from_utf8(field).map_err(|error| {
                PluginError::InvalidInput(format!("CSV field is not valid UTF-8: {error}"))
            })?;
            row.push(field.to_string());
        }
        rows.push(row);
    }
    Ok(rows)
}

fn buffer_with_encoding(buf: &[u8]) -> (&[u8], &'static Encoding) {
    if let Some((encoding, skip)) = Encoding::for_bom(buf) {
        (&buf[skip..], encoding)
    } else {
        let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
        detector.feed(buf, true);
        (buf, detector.guess(None, Utf8Detection::Allow))
    }
}

fn dialect_for_filename(filename: Option<&str>, decoded: &str) -> CsvDialect {
    let sniffer = Sniffer::new();
    if let Ok(metadata) = sniffer.sniff_bytes(decoded.as_bytes()) {
        return CsvDialect {
            delimiter: metadata.dialect.delimiter,
            quote: metadata.dialect.quote,
        };
    }

    CsvDialect {
        delimiter: fallback_delimiter(filename, decoded),
        quote: Quote::Some(b'"'),
    }
}

fn fallback_delimiter(filename: Option<&str>, decoded: &str) -> u8 {
    match filename.and_then(|f| Path::new(f).extension().and_then(|ext| ext.to_str())) {
        Some(extension) if extension.eq_ignore_ascii_case("tsv") => b'\t',
        Some(extension) if extension.eq_ignore_ascii_case("csv") => b',',
        _ => {
            let buf = decoded.as_bytes();
            let sample = &buf[..buf.len().min(8 * 1024)];
            let comma_count = bytecount::count(sample, b',');
            let tab_count = bytecount::count(sample, b'\t');
            if tab_count > comma_count { b'\t' } else { b',' }
        }
    }
}

fn render_projection(projection: &Projection) -> Result<Vec<u8>, PluginError> {
    let mut writer_builder = WriterBuilder::new();
    writer_builder
        .has_headers(false)
        .delimiter(projection.dialect.delimiter)
        .terminator(Terminator::Any(b'\n'));
    match projection.dialect.quote {
        Quote::None => {
            writer_builder.quote_style(QuoteStyle::Never);
        }
        Quote::Some(quote) => {
            writer_builder.quote(quote);
        }
    }
    let mut writer = writer_builder.from_writer(Vec::new());

    for row in projection.to_rows() {
        writer.write_record(&row.cells).map_err(|error| {
            PluginError::Internal(format!("failed to render CSV row '{}': {error}", row.id))
        })?;
    }

    writer.into_inner().map_err(|error| {
        PluginError::Internal(format!("failed to finish rendering CSV: {}", error.error()))
    })
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct FractionalIndex(u128);

impl FractionalIndex {
    fn to_snapshot_string(self) -> String {
        format!("{:032x}", self.0)
    }

    fn from_snapshot_string(raw: &str) -> Result<Self, String> {
        if raw.len() != 32 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("must be a 32-character hexadecimal string".to_string());
        }
        if raw.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err("must use lowercase hexadecimal digits".to_string());
        }

        let value = u128::from_str_radix(raw, 16)
            .map_err(|error| format!("must parse as a u128 hexadecimal value: {error}"))?;
        if value == 0 || value == u128::MAX {
            return Err("must be between the reserved lower and upper sentinels".to_string());
        }

        Ok(Self(value))
    }
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

fn evenly_spaced_fractional_index(offset: usize, len: usize) -> FractionalIndex {
    let step = u128::MAX / (len as u128 + 1);
    FractionalIndex(step * (offset as u128 + 1))
}

fn fractional_index_between(
    previous: Option<FractionalIndex>,
    next: Option<FractionalIndex>,
) -> Result<FractionalIndex, PluginError> {
    let lower = previous.map_or(0, |index| index.0);
    let upper = next.map_or(u128::MAX, |index| index.0);
    if lower > upper {
        return Err(PluginError::InvalidInput(format!(
            "fractional index bounds are out of order: previous={previous:?}, next={next:?}"
        )));
    }
    if lower == upper {
        return Err(PluginError::InvalidInput(format!(
            "cannot generate fractional index between identical indexes: {previous:?}"
        )));
    }

    let gap = upper - lower;
    if gap <= 1 {
        return Err(PluginError::InvalidInput(format!(
            "fractional index space exhausted between previous={previous:?} and next={next:?}"
        )));
    }

    Ok(FractionalIndex(lower + gap / 2))
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
