use super::*;

impl<S: StorageAdapterRead> HotStateStoreReader<S> {
    /// Reads a canonical single-file HOT range incrementally. Only accepted
    /// visible rows consume the limit; tombstones and retention predicates may
    /// require further pages. Layered generations use the merge reader instead.
    pub(crate) async fn try_scan_limited_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
        requested_untracked: Option<bool>,
    ) -> Result<Option<MaterializedHotStateBatch>, LixError> {
        let Some(limit) = request.limit else {
            return Ok(None);
        };
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        let generation = control.tracked_generation;
        let scope = hot_scope_prefix(branch_id, generation);
        if load_root_current_base_commit(&self.store, branch_id, generation)
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let packed = PointReadPlan::new(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            &[StorageKey(Bytes::copy_from_slice(&scope))],
        )
        .materialize(&self.store, StorageGetOptions::default())
        .await?
        .value;
        if packed.into_iter().any(|value| value.is_some()) {
            return Ok(None);
        }
        let mut filter = request.filter.clone();
        if !hot_filter_has_one_fixed_file_bucket(&filter) {
            if hot_schema_has_file_members(&self.store, branch_id, generation, &filter.schema_keys)
                .await?
            {
                return Ok(None);
            }
            if !filter.file_ids.is_empty()
                && !filter
                    .file_ids
                    .iter()
                    .any(|file| matches!(file, NullableKeyFilter::Null | NullableKeyFilter::Any))
            {
                return Ok(Some(MaterializedHotStateBatch::default()));
            }
            filter.file_ids = vec![NullableKeyFilter::Null];
        }
        let collection = load_hot_collection_visibility_control(
            &self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        let replaced = (collection.active_generation != generation).then_some(collection);
        if replaced.is_some_and(|control| control.live_count == 0) {
            return Ok(Some(MaterializedHotStateBatch::default()));
        }
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let mut result = MaterializedHotStateBatchBuilder::with_capacity(
            limit.min(crate::storage_adapter::MAX_SCAN_PAGE_ROWS),
        );
        let prefixes = hot_file_scan_prefixes(branch_id, generation, &filter)
            .unwrap_or_else(|| hot_row_scan_prefixes(&scope, &filter));
        for prefix in prefixes {
            let range = StoragePrefix {
                bytes: Bytes::from(prefix),
            }
            .to_range()?;
            let mut cursor = self
                .store
                .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
                .await?;
            while result.len() < limit {
                let (page, more) = cursor
                    .next_page(
                        (limit - result.len()).min(crate::storage_adapter::MAX_SCAN_PAGE_ROWS),
                    )
                    .await?
                    .into_parts();
                let mut entries = Vec::with_capacity(page.len());
                for entry in page {
                    let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                    if identity.matches_filter(&filter) {
                        entries.push((identity, full_value_bytes(entry.value)?));
                    }
                }
                let mut entries = HotScanEntries::Decoded(entries);
                if let Some(control) = replaced {
                    filter_hot_scan_entries_by_collection_generation(&mut entries, control)?;
                }
                let rows = materialize_hot_scan_entries(
                    &self.store,
                    entries,
                    projection,
                    branch_id,
                    control.working_diff_checkpoint_commit_id,
                )
                .await?;
                for row in rows.iter() {
                    if (request.filter.include_tombstones || !row.deleted())
                        && requested_untracked.is_none_or(|untracked| row.untracked() == untracked)
                    {
                        result.push_ref(row, None);
                    }
                }
                if !more {
                    break;
                }
            }
            if result.len() == limit {
                break;
            }
        }
        Ok(Some(result.finish()))
    }
}
