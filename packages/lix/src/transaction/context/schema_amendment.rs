//! Defaults added by an accepted schema amendment are ordinary row
//! changes, materialized once in the schema transaction rather than on reads.
use super::*;

pub(super) fn schemas_with_defaults(
    write: &TransactionWrite,
) -> Result<Vec<(String, JsonValue)>, LixError> {
    let rows = match write {
        TransactionWrite::Rows { rows, .. }
        | TransactionWrite::RowsWithFileContent { rows, .. } => rows,
    };
    // Certified SQL batches contain one homogeneous non-control schema; keep
    // the ordinary insert/update path constant-time at this optional hook.
    if rows
        .certified_preparation()
        .is_some_and(|proof| proof.fileless_typed_sql_rows)
        && rows
            .get(0)
            .is_some_and(|row| row.schema_key != REGISTERED_SCHEMA_KEY)
    {
        return Ok(Vec::new());
    }
    let mut schemas = Vec::new();
    for row in rows
        .iter()
        .filter(|row| row.schema_key == REGISTERED_SCHEMA_KEY)
    {
        let snapshot = if let Some(snapshot) = row.snapshot_json() {
            snapshot.value().clone()
        } else if let Some(typed) = row.decoded_snapshot() {
            typed.to_json_value()?
        } else {
            continue;
        };
        let Some(schema) = snapshot.get("value") else {
            continue;
        };
        let Some(key) = schema.get("key").and_then(JsonValue::as_str) else {
            continue;
        };
        if schema
            .get("columns")
            .and_then(JsonValue::as_array)
            .is_some_and(|columns| {
                columns.iter().any(|column| {
                    column.get("default_expression").is_some()
                        || column.get("default_value").is_some()
                })
            })
        {
            schemas.push((key.to_owned(), schema.clone()));
        }
    }
    Ok(schemas)
}

impl<StorageImpl> Transaction<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(super) async fn materialize_schema_defaults(
        &mut self,
        schemas: &[(String, JsonValue)],
    ) -> Result<(), LixError> {
        // A global schema can affect many branch/durability catalogs. Resolve
        // each row against the staged schema state, retaining local overrides.
        self.schema_resolver.clear_cached_catalogs();
        let mut updates = RawWriteBatch::default();
        for (schema_key, amended_schema) in schemas {
            let rows = self
                .scan_visible_hot_state_batch(&HotStateScanRequest {
                    filter: HotStateFilter {
                        schema_keys: vec![schema_key.clone()],
                        ..Default::default()
                    },
                    projection: HotStateProjection {
                        columns: vec!["snapshot_content".into(), "metadata".into()],
                    },
                    ..Default::default()
                })
                .await?;
            let staged = self.staged_writes.staging_overlay()?;
            let read = self.opening_read();
            let hot_state = self
                .hot_state
                .transaction_reader(read, Arc::clone(&self.branch_head_control_cache));
            for row in rows.iter() {
                let domain = Domain::for_live_row_ref(row).schema_catalog_domain();
                let catalog = self
                    .schema_resolver
                    .catalog_for_row_normalization(&hot_state, &staged, &domain)
                    .await?;
                let Some((_, plan)) = catalog.snapshot().plan_for_key(schema_key) else {
                    continue;
                };
                if plan.schema.as_ref() != amended_schema {
                    continue;
                }
                let Some(typed) = row.materialize_decoded_snapshot()? else {
                    continue;
                };
                if !plan.compiled_schema.defaults_would_apply(&typed.row) {
                    continue;
                }
                let mut value = typed.row.clone();
                let functions = self.functions.clone();
                let timestamp_functions = functions.clone();
                plan.compiled_schema
                    .apply_defaults(
                        &mut value,
                        || functions.call_uuid_v7(),
                        || {
                            let timestamp = *self
                                .current_timestamp
                                .get_or_insert_with(|| timestamp_functions.call_timestamp());
                            i64::try_from(timestamp.milliseconds_since_unix_epoch())
                                .expect("Lix timestamp fits i64")
                                * 1_000
                        },
                    )
                    .map_err(|error| {
                        LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string())
                    })?;
                plan.compiled_schema
                    .materialize_missing_nullable_columns(&mut value);
                let typed = Arc::new(WasmTypedRow::from_row(plan, value)?);
                let metadata = row
                    .metadata()
                    .map(|metadata| {
                        TransactionJson::from_value(
                            serde_json::from_str(metadata.as_str()).map_err(|error| {
                                LixError::new(LixError::CODE_SCHEMA_VALIDATION, error.to_string())
                            })?,
                            "schema amendment row metadata",
                        )
                    })
                    .transpose()?;
                updates.push_typed_parts(
                    Some(row.row_pk().clone()),
                    schema_key.as_str().into(),
                    row.file_id().map(Into::into),
                    Some(typed),
                    metadata,
                    None,
                    Some(row.created_at().to_string().into()),
                    None,
                    row.global(),
                    None,
                    None,
                    row.untracked(),
                    row.branch_id().into(),
                );
            }
        }
        if !updates.is_empty() {
            // Retain normal file reconciliation, row identity/constraint checks,
            // change history and commit publication for the generated values.
            Box::pin(
                self.stage_write_without_schema_default_backfill(TransactionWrite::Rows {
                    mode: TransactionWriteMode::Replace,
                    rows: updates,
                }),
            )
            .await?;
        }
        Ok(())
    }
}
