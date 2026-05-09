use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;

/// Physical append/locality unit for commit metadata and derived commit SQL
/// surfaces.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Commit {
    pub(crate) id: String,
    pub(crate) change_id: String,
    pub(crate) change_set_id: String,
    pub(crate) parent_ids: Vec<String>,
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) created_at: String,
    pub(crate) change_pack_count: u32,
    pub(crate) membership_pack_count: u32,
}

impl Commit {
    pub(crate) fn as_borrowed(&self) -> StoredCommitBorrowed<'_> {
        StoredCommitBorrowed {
            id: &self.id,
            change_id: &self.change_id,
            change_set_id: &self.change_set_id,
            parent_ids: &self.parent_ids,
            author_account_ids: &self.author_account_ids,
            created_at: &self.created_at,
            change_pack_count: self.change_pack_count,
            membership_pack_count: self.membership_pack_count,
        }
    }
}

/// Borrowed write-boundary view of stored [`Commit`] bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoredCommitBorrowed<'a> {
    pub(crate) id: &'a str,
    pub(crate) change_id: &'a str,
    pub(crate) change_set_id: &'a str,
    pub(crate) parent_ids: &'a [String],
    pub(crate) author_account_ids: &'a [String],
    pub(crate) created_at: &'a str,
    pub(crate) change_pack_count: u32,
    pub(crate) membership_pack_count: u32,
}

/// Borrowed logical commit supplied by commit producers before physical packing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitDraftBorrowed<'a> {
    pub(crate) id: &'a str,
    pub(crate) change_id: &'a str,
    pub(crate) change_set_id: &'a str,
    pub(crate) parent_ids: &'a [String],
    pub(crate) author_account_ids: &'a [String],
    pub(crate) created_at: &'a str,
}

/// Logical entity mutation fact stored in a commit change pack.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Change {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
}

/// Read-boundary view of a commit-store change with JSON refs resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializedChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

impl Change {
    pub(crate) fn as_borrowed(&self) -> ChangeBorrowed<'_> {
        ChangeBorrowed {
            id: &self.id,
            entity_id: &self.entity_id,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_ref: self.snapshot_ref.as_ref(),
            metadata_ref: self.metadata_ref.as_ref(),
            created_at: &self.created_at,
        }
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn as_ref(&self) -> ChangeBorrowed<'_> {
        self.as_borrowed()
    }
}

/// Borrowed write-boundary view of [`Change`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChangeBorrowed<'a> {
    pub(crate) id: &'a str,
    pub(crate) entity_id: &'a EntityIdentity,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: &'a str,
}

/// Logical scan request for the `lix_change` SQL surface over commit_store.
#[derive(Debug, Clone, Default)]
pub(crate) struct ChangeScanRequest {
    pub(crate) limit: Option<usize>,
}

/// Commit-local physical pack of newly authored change payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChangePack {
    pub(crate) commit_id: String,
    pub(crate) pack_id: u32,
    pub(crate) changes: Vec<Change>,
}

impl ChangePack {
    pub(crate) fn as_view(&self) -> ChangePackView<'_> {
        ChangePackView {
            commit_id: &self.commit_id,
            pack_id: self.pack_id,
            changes: &self.changes,
        }
    }
}

/// Borrowed read view for a decoded [`ChangePack`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChangePackView<'a> {
    pub(crate) commit_id: &'a str,
    pub(crate) pack_id: u32,
    pub(crate) changes: &'a [Change],
}

/// Storage location of an existing change payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChangeLocator {
    pub(crate) source_commit_id: String,
    pub(crate) source_pack_id: u32,
    pub(crate) source_ordinal: u32,
    pub(crate) change_id: String,
}

impl ChangeLocator {
    pub(crate) fn as_borrowed(&self) -> ChangeLocatorBorrowed<'_> {
        ChangeLocatorBorrowed {
            source_commit_id: &self.source_commit_id,
            source_pack_id: self.source_pack_id,
            source_ordinal: self.source_ordinal,
            change_id: &self.change_id,
        }
    }
}

/// Borrowed write-boundary view of [`ChangeLocator`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChangeLocatorBorrowed<'a> {
    pub(crate) source_commit_id: &'a str,
    pub(crate) source_pack_id: u32,
    pub(crate) source_ordinal: u32,
    pub(crate) change_id: &'a str,
}

/// Exact lookup entry for a derived-surface-visible change id.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ChangeIndexEntry {
    CommitHeader {
        commit_id: String,
        change_id: String,
    },
    PackedChange {
        locator: ChangeLocator,
    },
}

impl ChangeIndexEntry {
    pub(crate) fn as_borrowed(&self) -> ChangeIndexEntryBorrowed<'_> {
        match self {
            ChangeIndexEntry::CommitHeader {
                commit_id,
                change_id,
            } => ChangeIndexEntryBorrowed::CommitHeader {
                commit_id,
                change_id,
            },
            ChangeIndexEntry::PackedChange { locator } => ChangeIndexEntryBorrowed::PackedChange {
                locator: locator.as_borrowed(),
            },
        }
    }
}

/// Borrowed write-boundary view of [`ChangeIndexEntry`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum ChangeIndexEntryBorrowed<'a> {
    CommitHeader {
        commit_id: &'a str,
        change_id: &'a str,
    },
    PackedChange {
        locator: ChangeLocatorBorrowed<'a>,
    },
}

/// Commit-local physical pack of adopted/shared membership locators.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MembershipPack {
    pub(crate) commit_id: String,
    pub(crate) pack_id: u32,
    pub(crate) members: Vec<ChangeLocator>,
}

impl MembershipPack {
    pub(crate) fn as_view(&self) -> MembershipPackView<'_> {
        MembershipPackView {
            commit_id: &self.commit_id,
            pack_id: self.pack_id,
            members: &self.members,
        }
    }
}

/// Borrowed read view for a decoded [`MembershipPack`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MembershipPackView<'a> {
    pub(crate) commit_id: &'a str,
    pub(crate) pack_id: u32,
    pub(crate) members: &'a [ChangeLocator],
}

/// Locators produced while staging a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedCommitStoreCommit {
    pub(crate) authored_locators: Vec<ChangeLocator>,
    pub(crate) adopted_locators: Vec<ChangeLocator>,
}
