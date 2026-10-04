use super::*;

impl<S: StorageAdapterRead> HotStateStoreReader<S> {
    /// Scans one small global schema by identity, admitting a fixed number of
    /// physical HOT entries and bytes before returning any candidates. Packed
    /// and root-backed collections decline so callers can take their normal
    /// authoritative fallback.
    pub(crate) async fn try_scan_bounded_live_row_pks(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
        max_live_rows: usize,
        max_physical_entries: usize,
        max_physical_bytes: usize,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        let Some(limit) = request.limit else {
            return Ok(None);
        };
        let [schema_key] = request.filter.schema_keys.as_slice() else {
            return Ok(None);
        };
        if limit > max_live_rows.saturating_add(1) {
            return Ok(None);
        }
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
                return Ok(Some(Vec::new()));
            }
            filter.file_ids = vec![NullableKeyFilter::Null];
        }
        if hot_schema_has_collection_fence(&self.store, branch_id, generation, schema_key).await? {
            return Ok(None);
        }
        let collection = load_hot_collection_control(
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
            return Ok(Some(Vec::new()));
        }

        let prefixes = hot_file_scan_prefixes(branch_id, generation, &filter)
            .unwrap_or_else(|| hot_row_scan_prefixes(&scope, &filter));
        let mut row_pks = Vec::new();
        let mut physical_entries = 0_usize;
        let mut physical_bytes = 0_usize;
        for prefix in prefixes {
            let Some(range) = hot_file_row_pk_range(prefix, &filter)? else {
                continue;
            };
            let mut cursor = self
                .store
                .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
                .await?;
            loop {
                let (page, more) = cursor.next_page(1).await?.into_parts();
                let Some(entry) = page.into_iter().next() else {
                    if !more {
                        break;
                    }
                    continue;
                };
                let entry_bytes = entry
                    .key
                    .0
                    .len()
                    .checked_add(match &entry.value {
                        StorageProjectedValue::FullValue(value) => value.len(),
                        StorageProjectedValue::KeyOnly => 0,
                    })
                    .ok_or_else(|| head_value_error("bounded scan byte count overflow"))?;
                physical_entries = physical_entries
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("bounded scan entry count overflow"))?;
                physical_bytes = physical_bytes
                    .checked_add(entry_bytes)
                    .ok_or_else(|| head_value_error("bounded scan byte count overflow"))?;
                if physical_entries > max_physical_entries || physical_bytes > max_physical_bytes {
                    return Ok(None);
                }

                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(&filter) {
                    let value = full_value_bytes(entry.value)?;
                    let value = decode_head_value(&value)?;
                    let survives_replacement = replaced.is_none_or(|control| {
                        survives_collection_generation_fence(
                            value.untracked,
                            value.commit_id,
                            control.active_generation,
                            false,
                        )
                    });
                    if survives_replacement && !value.deleted {
                        if row_pks.len() >= max_live_rows {
                            return Ok(None);
                        }
                        row_pks.push(identity.row_pk.clone());
                    }
                }
                if !more {
                    break;
                }
            }
        }
        Ok(Some(row_pks))
    }

    pub(crate) async fn try_scan_bounded_live_identities(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
        max_physical_entries: usize,
        max_physical_bytes: usize,
    ) -> Result<Option<BoundedLiveIdentityScan>, LixError> {
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
                return Ok(Some(BoundedLiveIdentityScan::default()));
            }
            filter.file_ids = vec![NullableKeyFilter::Null];
        }
        if hot_schema_has_collection_fence(&self.store, branch_id, generation, schema_key).await? {
            return Ok(None);
        }
        let collection = load_hot_collection_control(
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
            return Ok(Some(BoundedLiveIdentityScan::default()));
        }

        let prefixes = hot_file_scan_prefixes(branch_id, generation, &filter)
            .unwrap_or_else(|| hot_row_scan_prefixes(&scope, &filter));
        let mut scan = BoundedLiveIdentityScan::default();
        for prefix in prefixes {
            let range = StoragePrefix {
                bytes: Bytes::from(prefix),
            }
            .to_range()?;
            let mut cursor = self
                .store
                .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
                .await?;
            loop {
                // One physical value is admitted at a time. Decode only its
                // identity and fixed header; never construct a row batch that
                // can retain payload-bearing durable predecessors.
                let (page, more) = cursor.next_page(1).await?.into_parts();
                let Some(entry) = page.into_iter().next() else {
                    if !more {
                        break;
                    }
                    continue;
                };
                let entry_bytes = entry
                    .key
                    .0
                    .len()
                    .checked_add(match &entry.value {
                        StorageProjectedValue::FullValue(value) => value.len(),
                        StorageProjectedValue::KeyOnly => 0,
                    })
                    .ok_or_else(|| head_value_error("bounded scan byte count overflow"))?;
                scan.physical_entries = scan
                    .physical_entries
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("bounded scan entry count overflow"))?;
                scan.physical_bytes = scan
                    .physical_bytes
                    .checked_add(entry_bytes)
                    .ok_or_else(|| head_value_error("bounded scan byte count overflow"))?;
                if scan.physical_entries > max_physical_entries
                    || scan.physical_bytes > max_physical_bytes
                {
                    return Ok(None);
                }

                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(&filter) {
                    let value = full_value_bytes(entry.value)?;
                    let value = decode_head_value(&value)?;
                    let survives_replacement = replaced.is_none_or(|control| {
                        survives_collection_generation_fence(
                            value.untracked,
                            value.commit_id,
                            control.active_generation,
                            false,
                        )
                    });
                    if survives_replacement && !value.deleted {
                        if scan.identities.len() >= limit {
                            return Ok(None);
                        }
                        // RowPk string/byte parts are slices of this key's
                        // Bytes allocation. This total is therefore a
                        // conservative measure of retained identity storage.
                        scan.identity_bytes =
                            scan.identity_bytes
                                .checked_add(identity.key.len())
                                .ok_or_else(|| head_value_error("identity byte count overflow"))?;
                        let HotScanIdentity {
                            key,
                            row_pk,
                            file_id,
                            ..
                        } = identity;
                        scan.identities
                            .push((row_pk, file_id.map(|file| file.into_string(&key))));
                    }
                }
                if !more {
                    break;
                }
            }
        }
        Ok(Some(scan))
    }

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
        self.try_scan_limited_live_batch_inner(branch_id, control, request, requested_untracked)
            .await
    }

    async fn try_scan_limited_live_batch_inner(
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
        if hot_schema_has_collection_fence(&self.store, branch_id, generation, schema_key).await? {
            return Ok(None);
        }
        let collection = load_hot_collection_control(
            &self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        if collection.active_generation != generation {
            return Ok(None);
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
                let page_limit =
                    (limit - result.len()).min(crate::storage_adapter::MAX_SCAN_PAGE_ROWS);
                let (page, more) = cursor.next_page(page_limit).await?.into_parts();
                let mut entries = Vec::with_capacity(page.len());
                for entry in page {
                    let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                    if identity.matches_filter(&filter) {
                        entries.push((identity, full_value_bytes(entry.value)?));
                    }
                }
                let rows = materialize_hot_scan_entries(
                    &self.store,
                    HotScanEntries::Decoded(entries),
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
