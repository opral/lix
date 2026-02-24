use crate::sql as legacy_sql;

use super::super::effects::DetectedFileDomainChange;

pub(crate) fn to_legacy_detected_file_domain_changes_by_statement(
    changes_by_statement: &[Vec<DetectedFileDomainChange>],
) -> Vec<Vec<legacy_sql::DetectedFileDomainChange>> {
    changes_by_statement
        .iter()
        .map(|changes| to_legacy_detected_file_domain_changes(changes))
        .collect()
}

pub(crate) fn from_legacy_detected_file_domain_changes(
    changes: Vec<legacy_sql::DetectedFileDomainChange>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .into_iter()
        .map(from_legacy_detected_file_domain_change)
        .collect()
}

pub(super) fn to_legacy_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<legacy_sql::DetectedFileDomainChange> {
    changes
        .iter()
        .cloned()
        .map(to_legacy_detected_file_domain_change)
        .collect()
}

fn to_legacy_detected_file_domain_change(
    change: DetectedFileDomainChange,
) -> legacy_sql::DetectedFileDomainChange {
    legacy_sql::DetectedFileDomainChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        version_id: change.version_id,
        plugin_key: change.plugin_key,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        writer_key: change.writer_key,
    }
}

fn from_legacy_detected_file_domain_change(
    change: legacy_sql::DetectedFileDomainChange,
) -> DetectedFileDomainChange {
    DetectedFileDomainChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        version_id: change.version_id,
        plugin_key: change.plugin_key,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        writer_key: change.writer_key,
    }
}
