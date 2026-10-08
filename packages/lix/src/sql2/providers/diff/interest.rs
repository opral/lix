//! Native endpoint-input preparation. No SQL plan or user function is replayed.
use super::*;

pub(crate) async fn prepare_native_diff_interest<R>(
    store: R,
    relation: &str,
    from: &str,
    to: &str,
    request: &TrackedStateDiffRequest,
    projected_columns: &[String],
    native_diff_budget: Option<crate::tracked_state::NativeDiffIdentityBudget>,
    collect_selected_head_keys: bool,
) -> Result<PreparedNativeDiffInputs, crate::LixError>
where
    R: StorageAdapterRead + Clone,
{
    if from == to {
        return Ok(PreparedNativeDiffInputs {
            selected_head_keys: Vec::new(),
            visible_after_change_ids: Vec::new(),
        });
    }
    let prepare = async {
        let from_descriptor = commit_state_descriptor(&store, from).await?;
        let to_descriptor = commit_state_descriptor(&store, to).await?;
        let mut tracked = TrackedStateContext::new().reader(store.clone());
        if let Some(budget) = native_diff_budget {
            tracked = tracked.with_native_diff_identity_budget(budget);
        }
        // Native side preparation inspects projection names only. This schema
        // never reaches Arrow output, validation, or expression evaluation.
        let projection = Schema::new(
            projected_columns
                .iter()
                .map(|name| Field::new(name, DataType::Null, true))
                .collect::<Vec<_>>(),
        );
        let provenance = projected_columns
            .iter()
            .any(|name| matches!(name.as_str(), "from_lixcol_global" | "to_lixcol_global"));
        let (diff, from_global, to_global) = effective_diff(
            &mut tracked,
            from,
            to,
            &from_descriptor,
            &to_descriptor,
            request,
            None,
            provenance,
            relation == "lix_file",
        )
        .await?;
        if request.retain_payloads {
            diff.validate_live_payloads()
                .map_err(lix_error_to_datafusion_error)?;
        }
        // Preserve the exact keys produced by the bounded native diff so the
        // caller can close one combined point frontier against the selected
        // current head. The authority's shared diff identity budget caps this
        // set; candidate warming does not request a duplicate key batch.
        let selected_head_keys = if collect_selected_head_keys {
            diff.entries
                .iter()
                .map(|entry| TrackedStateKey {
                    schema_key: entry.identity.schema_key().to_owned(),
                    file_id: entry.identity.file_id().map(str::to_owned),
                    row_pk: entry.identity.row_pk().clone(),
                })
                .collect()
        } else {
            Vec::new()
        };
        let visible_after_change_ids = if collect_selected_head_keys {
            diff.entries
                .iter()
                .filter_map(|entry| entry.visible_after().map(|row| row.change_id))
                .collect()
        } else {
            Vec::new()
        };
        match relation {
            "lix_file" => {
                file_diff_rows(
                    &mut tracked,
                    diff,
                    &request.filter.file_ids,
                    &projection,
                    from,
                    to,
                    &from_descriptor,
                    &to_descriptor,
                )
                .await?;
            }
            "lix_directory" => {
                directory_diff_rows(
                    &mut tracked,
                    diff,
                    &request.filter.row_pks,
                    &projection,
                    from,
                    to,
                    &from_descriptor,
                    &to_descriptor,
                    &from_global,
                    &to_global,
                )
                .await?;
            }
            _ => {
                schema_diff_rows(diff, relation, &projection, &from_global, &to_global)?;
            }
        }
        Ok::<PreparedNativeDiffInputs, DataFusionError>(PreparedNativeDiffInputs {
            selected_head_keys,
            visible_after_change_ids,
        })
    }
    .await;
    prepare.map_err(datafusion_error_to_lix_error)
}
