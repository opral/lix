#[derive(Debug, Clone, Copy)]
pub(crate) struct LixTableSpec {
    pub(crate) name: &'static str,
    pub(crate) columns: &'static [&'static str],
}

const LIX_WORKING_CHANGES_COLUMNS: &[&str] = &[
    "entity_id",
    "schema_key",
    "file_id",
    "before_change_id",
    "after_change_id",
    "before_commit_id",
    "after_commit_id",
    "status",
];

const LIX_CHANGE_COLUMNS: &[&str] = &[
    "id",
    "entity_id",
    "schema_key",
    "schema_version",
    "file_id",
    "plugin_key",
    "metadata",
    "created_at",
    "snapshot_content",
];

const LIX_FILE_COLUMNS: &[&str] = &[
    "id",
    "directory_id",
    "name",
    "extension",
    "path",
    "data",
    "metadata",
    "hidden",
    "lixcol_entity_id",
    "lixcol_schema_key",
    "lixcol_file_id",
    "lixcol_plugin_key",
    "lixcol_schema_version",
    "lixcol_inherited_from_version_id",
    "lixcol_change_id",
    "lixcol_created_at",
    "lixcol_updated_at",
    "lixcol_commit_id",
    "lixcol_writer_key",
    "lixcol_untracked",
    "lixcol_metadata",
];

const LIX_DIRECTORY_COLUMNS: &[&str] = &[
    "id",
    "parent_id",
    "name",
    "path",
    "metadata",
    "hidden",
    "lixcol_entity_id",
    "lixcol_schema_key",
    "lixcol_file_id",
    "lixcol_plugin_key",
    "lixcol_schema_version",
    "lixcol_inherited_from_version_id",
    "lixcol_change_id",
    "lixcol_created_at",
    "lixcol_updated_at",
    "lixcol_commit_id",
    "lixcol_writer_key",
    "lixcol_untracked",
    "lixcol_metadata",
];

const PUBLIC_LIX_TABLE_REGISTRY: &[LixTableSpec] = &[
    LixTableSpec {
        name: "lix_state",
        columns: &[],
    },
    LixTableSpec {
        name: "lix_state_by_version",
        columns: &[],
    },
    LixTableSpec {
        name: "lix_state_history",
        columns: &[],
    },
    LixTableSpec {
        name: "lix_change",
        columns: LIX_CHANGE_COLUMNS,
    },
    LixTableSpec {
        name: "lix_working_changes",
        columns: LIX_WORKING_CHANGES_COLUMNS,
    },
    LixTableSpec {
        name: "lix_file",
        columns: LIX_FILE_COLUMNS,
    },
    LixTableSpec {
        name: "lix_file_by_version",
        columns: LIX_FILE_COLUMNS,
    },
    LixTableSpec {
        name: "lix_file_history",
        columns: LIX_FILE_COLUMNS,
    },
    LixTableSpec {
        name: "lix_directory",
        columns: LIX_DIRECTORY_COLUMNS,
    },
    LixTableSpec {
        name: "lix_directory_by_version",
        columns: LIX_DIRECTORY_COLUMNS,
    },
    LixTableSpec {
        name: "lix_directory_history",
        columns: LIX_DIRECTORY_COLUMNS,
    },
    LixTableSpec {
        name: "lix_version",
        columns: &[],
    },
    LixTableSpec {
        name: "lix_active_version",
        columns: &[],
    },
    LixTableSpec {
        name: "lix_stored_schema",
        columns: &[],
    },
];

pub(crate) fn public_lix_table_names() -> Vec<&'static str> {
    PUBLIC_LIX_TABLE_REGISTRY
        .iter()
        .map(|spec| spec.name)
        .collect()
}

pub(crate) fn columns_for_public_lix_table(table_name: &str) -> Option<&'static [&'static str]> {
    PUBLIC_LIX_TABLE_REGISTRY
        .iter()
        .find(|spec| spec.name.eq_ignore_ascii_case(table_name))
        .and_then(|spec| {
            if spec.columns.is_empty() {
                None
            } else {
                Some(spec.columns)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::PUBLIC_LIX_TABLE_REGISTRY;
    use std::collections::HashSet;

    #[test]
    fn public_lix_table_registry_names_are_unique() {
        let mut seen = HashSet::new();
        for spec in PUBLIC_LIX_TABLE_REGISTRY {
            assert!(
                seen.insert(spec.name),
                "duplicate public Lix table in registry: {}",
                spec.name
            );
        }
    }
}
