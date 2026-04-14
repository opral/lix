#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqlPreparationPendingStorage {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqlPreparationPendingRow {
    pub(crate) snapshot_content: Option<String>,
    pub(crate) tombstone: bool,
}

pub(crate) trait SqlPreparationPendingOverlay {
    fn visible_registered_schema_entries(&self) -> Vec<(String, Option<String>)>;

    fn visible_registered_schema_rows(
        &self,
        storage: SqlPreparationPendingStorage,
    ) -> Vec<SqlPreparationPendingRow>;
}
