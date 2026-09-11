//! Native file-content preparation shared with partial working-set refresh.
use super::*;
use crate::hot_state::{FilePathInterest, FilePathInterestComparison, LogicalReadInterest};
fn retained_path(predicate: &FilePathPredicate) -> FilePathInterest {
    match predicate {
        FilePathPredicate::All => FilePathInterest::All,
        FilePathPredicate::Comparison { operation, value } => FilePathInterest::Comparison {
            operation: match operation {
                FilePathComparison::Equal => FilePathInterestComparison::Equal,
                FilePathComparison::LessThan => FilePathInterestComparison::LessThan,
                FilePathComparison::LessThanOrEqual => FilePathInterestComparison::LessThanOrEqual,
                FilePathComparison::GreaterThan => FilePathInterestComparison::GreaterThan,
                FilePathComparison::GreaterThanOrEqual => {
                    FilePathInterestComparison::GreaterThanOrEqual
                }
            },
            value: value.clone(),
        },
        FilePathPredicate::In(values) => FilePathInterest::In {
            values: values.iter().cloned().collect(),
        },
        FilePathPredicate::LowercaseContains(value) => FilePathInterest::LowercaseContains {
            value: value.clone(),
        },
        FilePathPredicate::And(left, right) => FilePathInterest::And {
            left: Box::new(retained_path(left)),
            right: Box::new(retained_path(right)),
        },
        FilePathPredicate::Or(left, right) => FilePathInterest::Or {
            left: Box::new(retained_path(left)),
            right: Box::new(retained_path(right)),
        },
    }
}
fn native_path(predicate: &FilePathInterest) -> FilePathPredicate {
    match predicate {
        FilePathInterest::All => FilePathPredicate::All,
        FilePathInterest::Comparison { operation, value } => FilePathPredicate::Comparison {
            operation: match operation {
                FilePathInterestComparison::Equal => FilePathComparison::Equal,
                FilePathInterestComparison::LessThan => FilePathComparison::LessThan,
                FilePathInterestComparison::LessThanOrEqual => FilePathComparison::LessThanOrEqual,
                FilePathInterestComparison::GreaterThan => FilePathComparison::GreaterThan,
                FilePathInterestComparison::GreaterThanOrEqual => {
                    FilePathComparison::GreaterThanOrEqual
                }
            },
            value: value.clone(),
        },
        FilePathInterest::In { values } => FilePathPredicate::In(values.iter().cloned().collect()),
        FilePathInterest::LowercaseContains { value } => {
            FilePathPredicate::LowercaseContains(value.clone())
        }
        FilePathInterest::And { left, right } => {
            FilePathPredicate::And(Box::new(native_path(left)), Box::new(native_path(right)))
        }
        FilePathInterest::Or { left, right } => {
            FilePathPredicate::Or(Box::new(native_path(left)), Box::new(native_path(right)))
        }
    }
}
fn retained_ids(ids: &FileIdConstraint) -> Option<Vec<String>> {
    match ids {
        FileIdConstraint::All => None,
        FileIdConstraint::None => Some(vec![]),
        FileIdConstraint::Ids(ids) => Some(ids.iter().cloned().collect()),
    }
}
pub(super) fn retain_content(
    hot: &dyn HotStateReader,
    request: &HotStateScanRequest,
    file_ids: &FileIdConstraint,
    directory_ids: &FileIdConstraint,
    root_directory: bool,
    path: &FilePathPredicate,
    indexed: bool,
    range: Option<&Range<u64>>,
) -> Result<(), LixError> {
    if let Some(registry) = hot.read_interest_registry() {
        registry.register(LogicalReadInterest::FileContent {
            request: request.clone(),
            file_ids: retained_ids(file_ids),
            directory_ids: retained_ids(directory_ids),
            root_directory,
            indexed,
            path_predicate: retained_path(path),
            byte_range: range.map(|range| (range.start, range.end)),
        })?;
    }
    Ok(())
}
/// Hydrate the native content inputs, including plugin render inputs, at the
/// candidate file identities. No user SQL/functions or view acknowledgments run.
pub(crate) async fn prepare_native_file_content_interest(
    hot_state: Arc<dyn HotStateReader>,
    filesystem_path_index: Arc<dyn FilesystemPathIndexReader>,
    blob_reader: Arc<dyn BlobDataReader>,
    plugin_host: PluginRuntimeHost,
    request: &HotStateScanRequest,
    file_ids: Option<&[String]>,
    directory_ids: Option<&[String]>,
    root_directory: bool,
    indexed: bool,
    path: &FilePathInterest,
    byte_range: Option<(u64, u64)>,
) -> Result<(), LixError> {
    if file_ids.is_some_and(<[String]>::is_empty)
        || directory_ids.is_some_and(<[String]>::is_empty)
        || request.limit == Some(0)
    {
        return Ok(());
    }
    let range = byte_range.map(|(start, end)| start..end);
    if range.as_ref().is_some_and(|range| range.start > range.end) {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "retained file byte range is reversed",
        ));
    }
    let path = native_path(path);
    let prepared = if indexed {
        let index = filesystem_path_index
            .path_index(
                &FilesystemPathIndexRequest::new(request.filter.branch_ids.clone())
                    .with_blob_refs(true)
                    .with_cached_blob_data(range.is_none()),
            )
            .await?;
        let ids = file_ids.map(|ids| ids.iter().cloned().collect::<BTreeSet<_>>());
        let dirs = directory_ids.map(|ids| ids.iter().cloned().collect::<BTreeSet<_>>());
        let matches = if root_directory {
            indexed_file_root_matches(
                index,
                &ids.as_ref().map_or(FileIdConstraint::All, |ids| {
                    FileIdConstraint::Ids(ids.clone())
                }),
                &path,
            )
        } else if let Some(dirs) = &dirs {
            indexed_file_directory_matches(index, dirs, ids.as_ref(), &path)
        } else if let Some(ids) = &ids {
            indexed_file_id_matches(index, ids, &path)
        } else {
            indexed_file_matches(index, &path)
        };
        // Native file projection currently loads its candidate content batch before
        // SQL residual filters/output LIMIT. Retain request.limit as a recipe fact,
        // never reinterpret it as complete coverage or truncate a file's state rows.
        let rows = scan_indexed_file_batch(&matches, true)?;
        prepare_indexed_lix_file_rows(&matches, rows)?
    } else {
        let ids = file_ids.map_or(FileIdConstraint::All, |ids| {
            FileIdConstraint::Ids(ids.iter().cloned().collect())
        });
        let rows = scan_lix_file_live_batch(Arc::clone(&hot_state), request, &ids).await?;
        prepare_lix_file_rows(rows, &path)?
    };
    let render = if prepared.needs_plugin_render(true) {
        plugin_render_context_for_lix_file_scan_cached(
            hot_state,
            request,
            plugin_host,
            &prepared,
            false,
            None,
        )
        .await?
    } else {
        None
    };
    exact_path_data_rows_from_prepared(&blob_reader, render, prepared, range.as_ref()).await?;
    Ok(())
}
