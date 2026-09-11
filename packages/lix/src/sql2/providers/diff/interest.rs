//! Native endpoint-input preparation. No SQL plan or user function is replayed.
use super::*;
pub(crate) async fn prepare_native_diff_interest<R>(
    store: R,
    relation: &str,
    from: &str,
    to: &str,
    request: &TrackedStateDiffRequest,
    projected_columns: &[String],
) -> Result<(), crate::LixError>
where
    R: StorageAdapterRead + Clone,
{
    if from == to {
        return Ok(());
    }
    let prepare = async {
        let from_descriptor = commit_state_descriptor(&store, from).await?;
        let to_descriptor = commit_state_descriptor(&store, to).await?;
        let mut tracked = TrackedStateContext::new().reader(store);
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
        )
        .await?;
        if request.retain_payloads {
            diff.validate_live_payloads()
                .map_err(lix_error_to_datafusion_error)?;
        }
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
                schema_diff_rows(diff, &projection, &from_global, &to_global)?;
            }
        }
        Ok::<(), DataFusionError>(())
    }
    .await;
    prepare.map_err(datafusion_error_to_lix_error)
}
