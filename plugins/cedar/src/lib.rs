//! Canonical Cedar files to transparent, read-only Lix rows.
#![cfg_attr(
    not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")),
    allow(dead_code)
)]

use cedar_policy::{Entities, PolicySet, Schema};
use lix::plugin as sdk;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

struct CedarPlugin;

const SCHEMA_KEY: &str = "cedar_permission_source";

#[derive(Debug, Deserialize, Serialize)]
struct SourceRow {
    id: String,
    kind: String,
    path: String,
    source: String,
}

impl sdk::FileProjection for CedarPlugin {
    fn parse(input: sdk::ParseInput<'_>, output: &mut sdk::RowOutput<'_, '_>) -> sdk::Result<()> {
        emit_source(input.file_id, input.path, input.file.read_all()?, output)
    }

    fn parse_changes(
        input: sdk::ParseChangesInput<'_>,
        output: &mut sdk::RowChangeOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let mut bytes = input.before.read_all()?;
        let mut edits = input.file_edits.iter().cloned().collect::<Vec<_>>();
        edits.sort_unstable_by_key(|edit| std::cmp::Reverse(edit.offset));
        for edit in edits {
            let start = usize::try_from(edit.offset)
                .map_err(|_| sdk::Error::limit_exceeded("Cedar edit offset is too large"))?;
            let delete_len = usize::try_from(edit.delete_len)
                .map_err(|_| sdk::Error::limit_exceeded("Cedar edit length is too large"))?;
            let end = start
                .checked_add(delete_len)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| sdk::Error::invalid_input("Cedar edit range is invalid"))?;
            bytes.splice(start..end, edit.insert.iter().copied());
        }
        let mut replacement = output.replace_all_rows()?;
        emit_source(input.file_id, input.after_path, bytes, &mut replacement)
    }

    fn serialize(
        mut input: sdk::SerializeInput<'_>,
        output: &mut sdk::FileOutput<'_, '_>,
    ) -> sdk::Result<()> {
        let row = input
            .rows
            .next()?
            .ok_or_else(|| sdk::Error::invalid_input("Cedar source row is missing"))?;
        if input.rows.next()?.is_some() {
            return Err(sdk::Error::invalid_input(
                "Cedar source projection must contain exactly one row",
            ));
        }
        let source: SourceRow = serde_json::from_slice(&row.snapshot)
            .map_err(|error| sdk::Error::invalid_input(format!("invalid Cedar row: {error}")))?;
        validate_source(&source.path, &source.source)?;
        output.write(source.source.as_bytes())
    }

    fn serialize_changes(
        _input: sdk::SerializeChangesInput<'_>,
        _output: &mut sdk::FileEditOutput<'_, '_>,
    ) -> sdk::Result<()> {
        Err(sdk::Error::invalid_input(
            "cedar_permission_source rows are read-only; edit /.lix/permissions files",
        ))
    }
}

fn emit_source(
    file_id: &str,
    path: &str,
    bytes: Vec<u8>,
    output: &mut sdk::RowOutput<'_, '_>,
) -> sdk::Result<()> {
    let source = String::from_utf8(bytes).map_err(|error| {
        sdk::Error::invalid_input(format!("Cedar source is not UTF-8: {error}"))
    })?;
    let kind = validate_source(path, &source)?;
    let snapshot = serde_json::to_vec(&SourceRow {
        id: file_id.to_owned(),
        kind: kind.to_owned(),
        path: path.to_owned(),
        source,
    })
    .map_err(|error| sdk::Error::internal(format!("could not encode Cedar row: {error}")))?;
    output.upsert(SCHEMA_KEY, &[file_id.to_owned()], &snapshot)
}

fn validate_source(path: &str, source: &str) -> sdk::Result<&'static str> {
    if path.ends_with(".cedarschema") {
        let (_schema, warnings) = Schema::from_cedarschema_str(source).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid Cedar schema in {path}: {error}"))
        })?;
        let _warnings = warnings.collect::<Vec<_>>();
        Ok("schema")
    } else if path.ends_with(".cedar.json") {
        Entities::from_json_str(source, None).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid Cedar entities in {path}: {error}"))
        })?;
        Ok("entities")
    } else if path.ends_with(".cedar") {
        PolicySet::from_str(source).map_err(|error| {
            sdk::Error::invalid_input(format!("invalid Cedar policies in {path}: {error}"))
        })?;
        Ok("policy")
    } else {
        Err(sdk::Error::invalid_input(format!(
            "unsupported permission file '{path}'"
        )))
    }
}

lix::plugin::export_capabilities! {
    file_projection: CedarPlugin,
}

#[cfg(test)]
mod tests {
    use super::validate_source;

    #[test]
    fn validates_each_canonical_source_kind() {
        assert_eq!(
            validate_source("/.lix/permissions/schema.cedarschema", "entity Account;").unwrap(),
            "schema"
        );
        assert_eq!(
            validate_source(
                "/.lix/permissions/publications.cedar",
                "permit(principal, action, resource);"
            )
            .unwrap(),
            "policy"
        );
        assert_eq!(
            validate_source("/.lix/permissions/entities.cedar.json", "[]").unwrap(),
            "entities"
        );
    }
}
