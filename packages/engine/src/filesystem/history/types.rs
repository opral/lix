#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryContentMode {
    #[default]
    MetadataOnly,
    IncludeData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryLineageScope {
    #[default]
    ActiveVersion,
    Standard,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum FileHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct FileHistoryRequest {
    pub(crate) lineage_scope: FileHistoryLineageScope,
    pub(crate) root_scope: FileHistoryRootScope,
    pub(crate) version_scope: FileHistoryVersionScope,
    pub(crate) file_ids: Vec<String>,
    pub(crate) content_mode: FileHistoryContentMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileHistoryRow {
    pub(crate) id: String,
    pub(crate) path: Option<String>,
    pub(crate) data: Option<Vec<u8>>,
    pub(crate) metadata: Option<String>,
    pub(crate) hidden: Option<bool>,
    pub(crate) lixcol_entity_id: String,
    pub(crate) lixcol_schema_key: String,
    pub(crate) lixcol_file_id: String,
    pub(crate) lixcol_version_id: String,
    pub(crate) lixcol_plugin_key: String,
    pub(crate) lixcol_schema_version: String,
    pub(crate) lixcol_change_id: String,
    pub(crate) lixcol_metadata: Option<String>,
    pub(crate) lixcol_commit_id: String,
    pub(crate) lixcol_commit_created_at: String,
    pub(crate) lixcol_root_commit_id: String,
    pub(crate) lixcol_depth: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DirectoryHistoryRequest {
    pub(crate) lineage_scope: FileHistoryLineageScope,
    pub(crate) root_scope: FileHistoryRootScope,
    pub(crate) version_scope: FileHistoryVersionScope,
    pub(crate) directory_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectoryHistoryRow {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) path: Option<String>,
    pub(crate) hidden: Option<bool>,
    pub(crate) lixcol_entity_id: String,
    pub(crate) lixcol_schema_key: String,
    pub(crate) lixcol_file_id: String,
    pub(crate) lixcol_version_id: String,
    pub(crate) lixcol_plugin_key: String,
    pub(crate) lixcol_schema_version: String,
    pub(crate) lixcol_change_id: String,
    pub(crate) lixcol_metadata: Option<String>,
    pub(crate) lixcol_commit_id: String,
    pub(crate) lixcol_commit_created_at: String,
    pub(crate) lixcol_root_commit_id: String,
    pub(crate) lixcol_depth: i64,
}
