use std::collections::{BTreeMap, BTreeSet};

use crate::canonical::journal::CanonicalCommitOutput;
use crate::canonical::read::VersionInfo;
use crate::change_view::TrackedDomainChangeView;
use crate::{
    CanonicalJson, CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId,
    FileId, VersionId,
};

use super::receipt::UpdatedVersionRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProposedDomainChange {
    pub(crate) entity_id: EntityId,
    pub(crate) schema_key: CanonicalSchemaKey,
    pub(crate) schema_version: Option<CanonicalSchemaVersion>,
    pub(crate) file_id: Option<FileId>,
    pub(crate) plugin_key: Option<CanonicalPluginKey>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) version_id: VersionId,
    pub(crate) writer_key: Option<String>,
}

impl TrackedDomainChangeView for ProposedDomainChange {
    fn entity_id(&self) -> &str {
        self.entity_id.as_str()
    }

    fn schema_key(&self) -> &str {
        self.schema_key.as_str()
    }

    fn schema_version(&self) -> Option<&str> {
        self.schema_version.as_ref().map(|value| value.as_str())
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id.as_ref().map(|value| value.as_str())
    }

    fn plugin_key(&self) -> Option<&str> {
        self.plugin_key.as_ref().map(|value| value.as_str())
    }

    fn snapshot_content(&self) -> Option<&str> {
        self.snapshot_content.as_deref()
    }

    fn version_id(&self) -> &str {
        self.version_id.as_str()
    }

    fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainChangeInput {
    pub id: String,
    pub entity_id: EntityId,
    pub schema_key: CanonicalSchemaKey,
    pub schema_version: CanonicalSchemaVersion,
    pub file_id: FileId,
    pub plugin_key: CanonicalPluginKey,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub version_id: VersionId,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitArgs {
    pub timestamp: String,
    pub active_accounts: Vec<String>,
    pub changes: Vec<DomainChangeInput>,
    pub versions: BTreeMap<String, VersionInfo>,
    pub force_commit_versions: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitResult {
    pub canonical_output: CanonicalCommitOutput,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
}
