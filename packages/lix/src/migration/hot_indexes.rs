//! v81 -> v82 replaces the derived declared-column index from current rows.
//! No historical commit, pending upload, or authority receipt is rewritten.
use std::collections::BTreeSet;
use std::time::Duration;

use crate::LixError;
use crate::hot_state::{HotStateReadDomain, HotStateReader, HotStateScanRequest};
use crate::storage_adapter::{Storage, StorageAdapter};

use super::{MigrationOptions, publish::PublicationPlan};

#[allow(dead_code)]
#[derive(Debug, Default)]
struct HotIndexMigrationProfile {
    planning_read: Duration,
    branch_controls: Duration,
    catalog_load: Duration,
    catalog_spec_derivation: Duration,
    row_scans: Duration,
    index_extraction: Duration,
    index_staging: Duration,
    publication_plan_copy: Duration,
    catalog_calls: usize,
    scan_calls: usize,
    scanned_rows: usize,
    migrated_rows: usize,
    index_entries: usize,
    staged_records: usize,
    staged_bytes: usize,
}

impl HotIndexMigrationProfile {
    #[cfg(feature = "storage-benches")]
    fn micros(duration: Duration) -> u128 {
        duration.as_micros()
    }

    #[cfg(feature = "storage-benches")]
    fn report(&self) -> String {
        format!(
            concat!(
                "planning_read_us={} branch_controls_us={} catalog_load_us={} ",
                "catalog_spec_derivation_us={} row_scans_us={} index_extraction_us={} ",
                "index_staging_us={} publication_plan_copy_us={} catalog_calls={} ",
                "scan_calls={} scanned_rows={} migrated_rows={} index_entries={} ",
                "staged_records={} staged_bytes={}"
            ),
            Self::micros(self.planning_read),
            Self::micros(self.branch_controls),
            Self::micros(self.catalog_load),
            Self::micros(self.catalog_spec_derivation),
            Self::micros(self.row_scans),
            Self::micros(self.index_extraction),
            Self::micros(self.index_staging),
            Self::micros(self.publication_plan_copy),
            self.catalog_calls,
            self.scan_calls,
            self.scanned_rows,
            self.migrated_rows,
            self.index_entries,
            self.staged_records,
            self.staged_bytes,
        )
    }
}

#[cfg(feature = "storage-benches")]
macro_rules! migration_profile_start {
    () => {
        std::time::Instant::now()
    };
}

#[cfg(not(feature = "storage-benches"))]
macro_rules! migration_profile_start {
    () => {
        ()
    };
}

#[cfg(feature = "storage-benches")]
macro_rules! migration_profile_record_duration {
    ($profile:expr, $started:expr, $field:ident) => {
        if let Some(profile) = $profile.as_deref_mut() {
            profile.$field += $started.elapsed();
        }
    };
}

#[cfg(not(feature = "storage-benches"))]
macro_rules! migration_profile_record_duration {
    ($profile:expr, $started:expr, $field:ident) => {};
}

#[cfg(feature = "storage-benches")]
macro_rules! migration_profile_add {
    ($profile:expr, $field:ident, $value:expr) => {
        if let Some(profile) = $profile.as_deref_mut() {
            profile.$field += $value;
        }
    };
}

#[cfg(not(feature = "storage-benches"))]
macro_rules! migration_profile_add {
    ($profile:expr, $field:ident, $value:expr) => {};
}

pub(super) async fn append_plan<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    plan: &mut PublicationPlan,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    append_plan_inner(adapter, options, plan, None).await
}

async fn append_plan_inner<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    plan: &mut PublicationPlan,
    mut _profile: Option<&mut HotIndexMigrationProfile>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _started = migration_profile_start!();
    let read = super::MigrationPlanningRead::new(adapter).await?;
    migration_profile_record_duration!(_profile, _started, planning_read);
    // A sparse receipt authenticates only resident ranges. Retire the old
    // derived plane without fabricating collection completeness or hydrating
    // remote history during an offline storage upgrade.
    if crate::sync::prepare_owned_partial_metadata_upgrade(&read)
        .await?
        .is_some()
    {
        plan.replace_mutable_space(crate::hot_state::INDEX_SPACE, Vec::new())?;
        read.finish()?;
        return Ok(());
    }
    let _started = migration_profile_start!();
    let mut controls = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .scan()
        .await?;
    migration_profile_record_duration!(_profile, _started, branch_controls);
    // Count authoritative global rows before allowing for their projected
    // copies in local bounded scans.
    controls.sort_by_key(|(branch, _)| branch != crate::GLOBAL_BRANCH_ID);
    let context = crate::hot_state::HotStateContext::new(
        crate::tracked_state::TrackedStateContext::new(),
        crate::commit_graph::CommitGraphContext::new(),
    );
    let reader = context.reader(&read);
    let catalogs = crate::catalog::CatalogContext::new();
    let mut writes = adapter.new_write_set();
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    let mut global_rows = 0usize;
    for (branch_id, control) in controls {
        let mut entries = Vec::new();
        let mut witnessed = BTreeSet::new();
        for untracked in [false, true] {
            let _started = migration_profile_start!();
            let catalog = catalogs
                .compiled_catalog_for_domain(
                    &reader,
                    &crate::domain::Domain::schema_catalog(branch_id.clone(), untracked),
                )
                .await?;
            migration_profile_add!(_profile, catalog_calls, 1);
            migration_profile_record_duration!(_profile, _started, catalog_load);
            let mut schema_keys = Vec::new();
            let _started = migration_profile_start!();
            for schema in catalog.plans() {
                let spec = crate::sql2::derive_schema_surface_spec_from_schema(&schema.schema)?;
                if !spec.indexed_columns.is_empty() || !spec.indexed_groups.is_empty() {
                    schema_keys.push(spec.schema_key.clone());
                }
                for column in &spec.indexed_columns {
                    witnessed.insert((spec.schema_key.clone(), column.ordinal));
                }
                for (ordinal, _) in &spec.indexed_groups {
                    witnessed.insert((spec.schema_key.clone(), *ordinal));
                }
            }
            migration_profile_record_duration!(_profile, _started, catalog_spec_derivation);
            if schema_keys.is_empty() {
                continue;
            }
            let mut request = HotStateScanRequest::default();
            request.filter.schema_keys = schema_keys;
            request.filter.branch_ids = vec![branch_id.clone()];
            request.filter.untracked = Some(untracked);
            request.limit = Some(
                options
                    .max_changes
                    .saturating_sub(row_count)
                    .saturating_add(global_rows)
                    .saturating_add(1),
            );
            let _started = migration_profile_start!();
            let rows = reader
                .scan_domain_batch(
                    &request,
                    if untracked {
                        HotStateReadDomain::Untracked
                    } else {
                        HotStateReadDomain::Tracked
                    },
                )
                .await?;
            migration_profile_add!(_profile, scan_calls, 1);
            migration_profile_add!(_profile, scanned_rows, rows.len());
            migration_profile_record_duration!(_profile, _started, row_scans);
            let _started = migration_profile_start!();
            for row in rows.iter() {
                if row.branch_id() != branch_id
                    || row.untracked() != untracked
                    || (row.global() && branch_id != crate::GLOBAL_BRANCH_ID)
                {
                    continue;
                }
                row_count = row_count.saturating_add(1);
                if branch_id == crate::GLOBAL_BRANCH_ID {
                    global_rows += 1;
                }
                row_bytes = row_bytes.saturating_add(
                    row.snapshot_json_value()?
                        .map_or(0, |value| value.to_string().len()),
                );
                if row_count > options.max_changes || row_bytes > options.max_preflight_bytes {
                    return Err(LixError::new(
                        "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                        "hot index rebuild exceeds configured row or byte bound",
                    ));
                }
                let (_, schema) = catalog.plan_for_key(row.schema_key()).ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_MIGRATION_FAILED",
                        format!(
                            "hot index rebuild cannot resolve schema '{}'",
                            row.schema_key()
                        ),
                    )
                })?;
                entries.extend(crate::transaction::hot_index_entries_for_migration(
                    schema, row,
                )?);
            }
            migration_profile_add!(
                _profile,
                migrated_rows,
                rows.iter()
                    .filter(|row| {
                        row.branch_id() == branch_id
                            && row.untracked() == untracked
                            && (!row.global() || branch_id == crate::GLOBAL_BRANCH_ID)
                    })
                    .count()
            );
            migration_profile_add!(_profile, index_entries, entries.len());
            migration_profile_record_duration!(_profile, _started, index_extraction);
        }
        let _started = migration_profile_start!();
        crate::hot_state::stage_hot_index_entries_rebuild(
            &read,
            &mut writes,
            &branch_id,
            control.tracked_generation,
            &entries,
            &witnessed,
        )
        .await?;
        migration_profile_record_duration!(_profile, _started, index_staging);
    }
    let _started = migration_profile_start!();
    let staged = writes
        .staged_values_in_space(crate::hot_state::INDEX_SPACE)
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect::<Vec<_>>();
    migration_profile_add!(_profile, staged_records, staged.len());
    migration_profile_add!(
        _profile,
        staged_bytes,
        staged
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum::<usize>()
    );
    migration_profile_record_duration!(_profile, _started, publication_plan_copy);
    plan.replace_mutable_space(
        crate::hot_state::INDEX_SPACE,
        staged,
    )?;
    drop(reader);
    read.finish()?;
    Ok(())
}

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    read.finish()?;
    let mut plan = PublicationPlan::bounded(options.max_changes, options.max_preflight_bytes);
    Box::pin(append_plan(adapter, options, &mut plan)).await?;
    let (source, target) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V81,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V82,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_V81,
            crate::init::REPOSITORY_PROTOCOL_V82,
        )
    };
    super::publish::publish(adapter, revision, source, target, plan).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageSession;

    #[tokio::test]
    async fn v81_rebuild_preserves_rows_and_repairs_missing_indexes() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "migration_index",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "label", "type": "text", "nullable": false}
            ], "primary_key": ["id"], "unique": [["label"]]
        });
        lix.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1)",
            &[crate::Value::Jsonb(schema.into())],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO migration_index(id,label) VALUES ('a','before')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        super::super::epoch::stage_repository_format_for_test(&storage, false, 81)
            .await
            .unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let read = super::super::MigrationPlanningRead::new(&adapter)
            .await
            .unwrap();
        let revision = crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap();
        read.finish().unwrap();
        let mut empty = PublicationPlan::bounded(0, 0);
        empty
            .replace_mutable_space(crate::hot_state::INDEX_SPACE, Vec::new())
            .unwrap();
        super::super::publish::publish(
            &adapter,
            revision,
            crate::init::REPOSITORY_PROTOCOL_V81,
            crate::init::REPOSITORY_PROTOCOL_V81,
            empty,
        )
        .await
        .unwrap();
        let report = crate::migration::migrate_repository(storage.clone())
            .await
            .unwrap();
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.preservation_basis, "v82-hot-index-plan-v1");
        assert_ne!(report.before_content_digest, report.after_content_digest);
        assert_eq!(report.expected_content_digest, report.after_content_digest);
        let lix = crate::open_lix().with_storage(storage).await.unwrap();
        assert_eq!(
            lix.execute("SELECT id FROM migration_index WHERE label = 'before'", &[])
                .await
                .unwrap()
                .rows()
                .len(),
            1
        );
        lix.execute(
            "UPDATE migration_index SET label = 'after' WHERE id = 'a'",
            &[],
        )
        .await
        .unwrap();
        assert!(
            lix.execute("SELECT id FROM migration_index WHERE label = 'before'", &[])
                .await
                .unwrap()
                .rows()
                .is_empty()
        );
        assert_eq!(
            lix.execute("SELECT id FROM migration_index WHERE label = 'after'", &[])
                .await
                .unwrap()
                .rows()
                .len(),
            1
        );
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn v81_rebuild_covers_tracked_and_untracked_rows_and_rolls_back_limits() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "migration_lane_index",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "label", "type": "text", "nullable": false}
            ], "primary_key": ["id"], "unique": [["label"]]
        });
        lix.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1)",
            &[crate::Value::Jsonb(schema.into())],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO migration_lane_index(id,label) VALUES ('tracked','tracked')",
            &[],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO migration_lane_index(id,label,lixcol_untracked) VALUES ('local','local',true)",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        super::super::epoch::stage_repository_format_for_test(&storage, false, 81)
            .await
            .unwrap();

        let limited = crate::migration::migrate_repository_with_options(
            storage.clone(),
            MigrationOptions {
                max_changes: 1,
                ..MigrationOptions::default()
            },
        )
        .await
        .expect_err("the v82 rebuild must refuse a plan over the row bound");
        assert_eq!(limited.code, "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED");
        let inspection = crate::migration::inspect_repository(storage.clone())
            .await
            .unwrap();
        assert_eq!(inspection.format, Some(81));

        let report = crate::migration::migrate_repository(storage.clone())
            .await
            .unwrap();
        assert!(report.semantic_preservation_verified);
        let lix = crate::open_lix().with_storage(storage).await.unwrap();
        for (label, expected) in [("tracked", false), ("local", true)] {
            let result = lix
                .execute(
                    "SELECT id FROM migration_lane_index WHERE label = $1 AND lixcol_untracked = $2",
                    &[crate::Value::Text(label.into()), crate::Value::Boolean(expected)],
                )
                .await
                .unwrap();
            let rows = result.rows();
            assert_eq!(rows.len(), 1, "rebuilt index must serve {label} row");
        }
        lix.close().await.unwrap();
    }
    #[tokio::test]
    async fn migrated_timestamp_composite_references_restrict_and_cascade() {
        for action in ["no_action", "cascade"] {
            let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
            let lix = crate::open_lix()
                .with_storage(storage.clone())
                .await
                .unwrap();
            for (key, parent) in [("migration_parent", true), ("migration_child", false)] {
                let mut schema = serde_json::json!({
                    "$schema": "https://lix.dev/schema-v1.json", "key": key,
                    "columns": [
                        {"name":"id","type":"text","nullable":false},
                        {"name":"moment","type":"timestamptz","nullable":false},
                        {"name":"label","type":"text","nullable":false}
                    ], "primary_key":["id"]
                });
                if parent {
                    schema["unique"] = serde_json::json!([["moment", "label"]]);
                } else {
                    schema["foreign_keys"] = serde_json::json!([{
                        "columns":["moment","label"],
                        "references":{"schema_key":"migration_parent","columns":["moment","label"]},
                        "on_delete":action
                    }]);
                }
                lix.execute(
                    "INSERT INTO lix_registered_schema(value) VALUES ($1)",
                    &[crate::Value::Jsonb(schema.into())],
                )
                .await
                .unwrap();
                lix.execute(&format!("INSERT INTO {key}(id,moment,label) VALUES ('row',CAST('2026-01-02T03:04:05Z' AS TIMESTAMPTZ),'same')"), &[]).await.unwrap();
            }
            lix.close().await.unwrap();
            super::super::epoch::stage_repository_format_for_test(&storage, false, 81)
                .await
                .unwrap();
            let lix = crate::open_lix().with_storage(storage).await.unwrap();
            let deleted = lix
                .execute("DELETE FROM migration_parent WHERE id='row'", &[])
                .await;
            if action == "cascade" {
                deleted.unwrap();
                assert!(
                    lix.execute("SELECT id FROM migration_child", &[])
                        .await
                        .unwrap()
                        .rows()
                        .is_empty()
                );
            } else {
                assert!(
                    deleted.is_err(),
                    "migrated timestamp FK must still restrict deletion"
                );
                assert_eq!(
                    lix.execute("SELECT id FROM migration_child", &[])
                        .await
                        .unwrap()
                        .rows()
                        .len(),
                    1
                );
            }
            lix.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn rebuild_indexes_global_rows_only_in_their_authoritative_branch() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"migration_global_index",
            "columns":[{"name":"id","type":"text","nullable":false},{"name":"label","type":"text","nullable":false}],
            "primary_key":["id"], "unique":[["label"]]
        });
        for global in [false, true] {
            lix.execute(
                "INSERT INTO lix_registered_schema(value,lixcol_global) VALUES ($1,$2)",
                &[
                    crate::Value::Jsonb(schema.clone().into()),
                    crate::Value::Boolean(global),
                ],
            )
            .await
            .unwrap();
        }
        lix.execute("INSERT INTO migration_global_index(id,label,lixcol_global) VALUES ('global','global-label',true)", &[]).await.unwrap();
        for index in 0..3 {
            lix.create_branch(crate::CreateBranchOptions {
                id: None,
                name: format!("migration-global-{index}"),
                from_commit_id: None,
            })
            .await
            .unwrap();
        }
        lix.close().await.unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let mut plan = PublicationPlan::default();
        append_plan(&adapter, MigrationOptions::default(), &mut plan)
            .await
            .unwrap();
        let (mut overlay, _) = plan.into_preservation_overlay();
        let records = overlay.remove(&crate::hot_state::INDEX_SPACE.id.0).unwrap();
        let read = super::super::MigrationPlanningRead::new(&adapter)
            .await
            .unwrap();
        let controls = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .unwrap();
        for (branch, control) in controls {
            let prefix =
                crate::hot_state::hot_generation_scope_prefix(&branch, control.tracked_generation);
            let entries = records
                .keys()
                .filter(|key| {
                    key.starts_with(&prefix)
                        && key
                            .windows(b"migration_global_index".len())
                            .any(|part| part == b"migration_global_index")
                        && !crate::hot_state::hot_index_key_is_witness(key)
                })
                .count();
            assert_eq!(
                entries,
                if branch == crate::GLOBAL_BRANCH_ID {
                    2
                } else {
                    0
                },
                "global row duplicated in branch {branch}"
            );
        }
    }

    /// Repeatable migration profile for a generated in-memory repository.
    ///
    /// Run with:
    /// `cargo test -p lix --features storage-benches migration::hot_indexes::tests::profile_v81_to_v82 --lib -- --ignored --nocapture`
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    #[ignore = "explicit migration profiling probe"]
    async fn profile_v81_to_v82() {
        use std::time::Instant;

        let row_count = std::env::var("LIX_MIGRATION_PROFILE_ROWS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000);

        let storage = profile_fixture(row_count).await;
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let mut plan = PublicationPlan::default();
        let started = Instant::now();
        let ((result, profile), storage_work) =
            crate::storage_bench::measure_checkpoint_foreground(async {
                let mut profile = HotIndexMigrationProfile::default();
                let result = append_plan_inner(
                    &adapter,
                    MigrationOptions::default(),
                    &mut plan,
                    Some(&mut profile),
                )
                .await;
                (result, profile)
            })
            .await;
        result.unwrap();
        println!(
            "v81_to_v82_plan rows={row_count} elapsed_us={} {}",
            started.elapsed().as_micros(),
            profile.report()
        );
        println!("v81_to_v82_plan_storage {storage_work:?}");

        let storage = profile_fixture(row_count).await;
        let started = Instant::now();
        let (result, storage_work) = crate::storage_bench::measure_checkpoint_foreground(
            crate::migration::migrate_repository(storage.clone()),
        )
        .await;
        result.unwrap();
        println!(
            "v81_to_v82_repository_migration rows={row_count} elapsed_us={} storage={storage_work:?}",
            started.elapsed().as_micros()
        );

        let lix = crate::open_lix()
            .with_storage(storage)
            .await
            .unwrap();
        let result = lix
            .execute("SELECT id FROM migration_profile", &[])
            .await
            .unwrap();
        assert_eq!(result.rows().len(), row_count);
        lix.close().await.unwrap();
    }

    #[cfg(feature = "storage-benches")]
    async fn profile_fixture(row_count: usize) -> StorageSession<crate::Memory> {
        use std::fmt::Write as _;

        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "migration_profile",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "label", "type": "text", "nullable": false}
            ],
            "primary_key": ["id"],
            "unique": [["label"]]
        });
        lix.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1)",
            &[crate::Value::Jsonb(schema.into())],
        )
        .await
        .unwrap();
        let mut insert = String::from("INSERT INTO migration_profile(id,label) VALUES ");
        for index in 0..row_count {
            if index != 0 {
                insert.push(',');
            }
            write!(insert, "('row-{index:08}','label-{index:08}')").unwrap();
        }
        lix.execute(&insert, &[]).await.unwrap();
        lix.close().await.unwrap();
        super::super::epoch::stage_repository_format_for_test(&storage, false, 81)
            .await
            .unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let read = super::super::MigrationPlanningRead::new(&adapter)
            .await
            .unwrap();
        let revision = crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap();
        read.finish().unwrap();
        let mut empty = PublicationPlan::bounded(0, 0);
        empty
            .replace_mutable_space(crate::hot_state::INDEX_SPACE, Vec::new())
            .unwrap();
        super::super::publish::publish(
            &adapter,
            revision,
            crate::init::REPOSITORY_PROTOCOL_V81,
            crate::init::REPOSITORY_PROTOCOL_V81,
            empty,
        )
        .await
        .unwrap();
        storage
    }
}
