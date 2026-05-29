use crate::untracked_state::{MaterializedUntrackedStateRow, UntrackedStateRow};
use crate::{LixError, parse_row_metadata};

pub(crate) fn materialize_row(
    row: UntrackedStateRow,
    projection: &UntrackedMaterializationProjection,
) -> Result<MaterializedUntrackedStateRow, LixError> {
    let deleted = row.snapshot_content.is_none();
    let created_at = row.created_at().to_string();
    let updated_at = row.updated_at().to_string();
    let snapshot_content = if projection.snapshot_content {
        row.snapshot_content
    } else {
        None
    };
    let metadata = if projection.metadata {
        load_optional_metadata(row.metadata)?
    } else {
        None
    };
    Ok(MaterializedUntrackedStateRow {
        entity_pk: row.entity_pk,
        schema_key: row.schema_key,
        file_id: row.file_id,
        snapshot_content,
        metadata,
        deleted,
        created_at,
        updated_at,
        global: row.global,
        branch_id: row.branch_id,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UntrackedMaterializationProjection {
    pub(crate) snapshot_content: bool,
    pub(crate) metadata: bool,
}

impl UntrackedMaterializationProjection {
    pub(crate) fn full() -> Self {
        Self {
            snapshot_content: true,
            metadata: true,
        }
    }

    pub(crate) fn from_columns(columns: &[String]) -> Self {
        if columns.is_empty() {
            return Self::full();
        }
        Self {
            snapshot_content: columns.iter().any(|column| column == "snapshot_content"),
            metadata: columns.iter().any(|column| column == "metadata"),
        }
    }
}

fn load_optional_metadata(metadata: Option<String>) -> Result<Option<String>, LixError> {
    let Some(json) = metadata else {
        return Ok(None);
    };
    parse_row_metadata(&json, "untracked_state metadata").map(Some)
}
