use crate::row_pk::RowPk;
#[cfg(test)]
use crate::hot_state::MaterializedHotStateRow;
use crate::hot_state::MaterializedHotStateRowRef;
use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

/// Validation/storage coordinate for repository facts.
///
/// A domain is the complete scope in which a row identity is meaningful:
/// branch, durability, and file scope. Projection methods on this type are
/// deliberately named so callers cannot silently erase part of the coordinate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Domain {
    branch_id: String,
    untracked: bool,
    file_scope: DomainFileScope,
}

impl Domain {
    pub(crate) fn exact_file(
        branch_id: impl Into<String>,
        untracked: bool,
        file_id: Option<String>,
    ) -> Self {
        Self {
            branch_id: branch_id.into(),
            untracked,
            file_scope: DomainFileScope::Exact(file_id),
        }
    }

    pub(crate) fn any_file(branch_id: impl Into<String>, untracked: bool) -> Self {
        Self {
            branch_id: branch_id.into(),
            untracked,
            file_scope: DomainFileScope::Any,
        }
    }

    pub(crate) fn schema_catalog(branch_id: impl Into<String>, untracked: bool) -> Self {
        Self::any_file(branch_id, untracked)
    }

    #[cfg(test)]
    pub(crate) fn for_live_row(row: &MaterializedHotStateRow) -> Self {
        Self::exact_file(
            row.branch_id.to_string(),
            row.untracked,
            row.file_id.clone(),
        )
    }

    pub(crate) fn for_live_row_ref(row: MaterializedHotStateRowRef<'_>) -> Self {
        Self::exact_file(
            row.branch_id(),
            row.untracked(),
            row.file_id().map(str::to_owned),
        )
    }

    pub(crate) fn schema_catalog_domain(&self) -> Self {
        // Schema definitions are branch + durability scoped. They are not
        // owned by a data file, so schema catalog lookup deliberately erases
        // row file scope into `Any`.
        Self::schema_catalog(self.branch_id.clone(), self.untracked)
    }

    pub(crate) fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub(crate) fn untracked(&self) -> bool {
        self.untracked
    }

    pub(crate) fn fingerprint_component(&self) -> String {
        let file_scope = match &self.file_scope {
            DomainFileScope::Any => "*".to_string(),
            DomainFileScope::Exact(Some(file_id)) => format!("={file_id}"),
            DomainFileScope::Exact(None) => "=".to_string(),
        };
        format!("{}|{}|{}", self.branch_id, self.untracked, file_scope)
    }

    #[cfg(test)]
    pub(crate) fn file_scope(&self) -> &DomainFileScope {
        &self.file_scope
    }

    pub(crate) fn with_untracked(&self, untracked: bool) -> Self {
        Self {
            branch_id: self.branch_id.clone(),
            untracked,
            file_scope: self.file_scope.clone(),
        }
    }

    pub(crate) fn with_file_scope(&self, file_scope: DomainFileScope) -> Self {
        Self {
            branch_id: self.branch_id.clone(),
            untracked: self.untracked,
            file_scope,
        }
    }

    pub(crate) fn with_exact_file_scope(&self, file_id: Option<String>) -> Self {
        self.with_file_scope(DomainFileScope::Exact(file_id))
    }

    pub(crate) fn file_filters(&self) -> Vec<NullableKeyFilter<String>> {
        match &self.file_scope {
            DomainFileScope::Any => Vec::new(),
            DomainFileScope::Exact(file_id) => vec![nullable_filter_from_option(file_id.as_ref())],
        }
    }

    pub(crate) fn contains_ref(&self, row: MaterializedHotStateRowRef<'_>) -> bool {
        row.branch_id() == self.branch_id
            && row.untracked() == self.untracked
            && self.contains_canonical_ref(row)
    }

    /// Matches branch and file scope while accepting whichever durability
    /// member won canonical tracked/untracked overlay.
    pub(crate) fn contains_canonical_ref(&self, row: MaterializedHotStateRowRef<'_>) -> bool {
        row.branch_id() == self.branch_id
            && committed_row_ref_is_exact_branch_scoped(row, &self.branch_id)
            && match &self.file_scope {
                DomainFileScope::Any => true,
                DomainFileScope::Exact(file_id) => row.file_id() == file_id.as_deref(),
            }
    }

    fn reachable_target_domains(&self) -> Vec<Self> {
        if self.untracked {
            vec![self.with_untracked(false), self.clone()]
        } else {
            vec![self.clone()]
        }
    }

    fn source_domains_that_can_reach(&self) -> Vec<Self> {
        if self.untracked {
            vec![self.clone()]
        } else {
            vec![self.clone(), self.with_untracked(true)]
        }
    }

    fn can_reach(&self, target: &Self) -> bool {
        self.branch_id == target.branch_id
            && self.file_scope == target.file_scope
            && (self.untracked || !target.untracked)
    }

    pub(crate) fn schema_catalog_domains(&self) -> Vec<Self> {
        self.schema_catalog_domain().reachable_target_domains()
    }

    pub(crate) fn fk_target_domains(&self) -> Vec<Self> {
        self.reachable_target_domains()
    }

    pub(crate) fn fk_source_domains_for_target(&self) -> Vec<Self> {
        self.source_domains_that_can_reach()
    }

    /// A row's owning file is looked up in the row's own lane only.
    ///
    /// This is the enforcement seam for "a row and the file that owns it live in
    /// the same lane". Deliberately NOT `reachable_target_domains()`: that
    /// widening is still correct for `fk_target_domains()` and
    /// `directory_parent_domains()`, where an untracked row referencing a
    /// tracked schema, account or parent directory is load-bearing. File
    /// ownership is the one relationship that must not cross the lane
    /// boundary, because a tracked file deletion would otherwise silently take
    /// untracked rows with it.
    pub(crate) fn file_owner_domains(&self) -> Vec<Self> {
        vec![self.clone()]
    }

    pub(crate) fn directory_parent_domains(&self) -> Vec<Self> {
        self.reachable_target_domains()
    }

    pub(crate) fn branch_descriptor_domains_for_ref_delete(&self) -> Vec<Self> {
        self.source_domains_that_can_reach()
    }

    pub(crate) fn validation_scope_contains_constraint_domain(&self, target: &Self) -> bool {
        self.can_reach(target)
    }

    pub(crate) fn tombstone_domain_affects_validation_scope(
        &self,
        validation_scope: &Self,
    ) -> bool {
        self.can_reach(validation_scope)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DomainFileScope {
    Any,
    Exact(Option<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DomainRowIdentity {
    domain: Domain,
    schema_key: String,
    row_pk: RowPk,
}

impl DomainRowIdentity {
    pub(crate) fn new(domain: Domain, schema_key: impl Into<String>, row_pk: RowPk) -> Self {
        Self {
            domain,
            schema_key: schema_key.into(),
            row_pk,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_live_row(row: &MaterializedHotStateRow) -> Self {
        Self::new(
            Domain::for_live_row(row),
            row.schema_key.clone(),
            row.row_pk.clone(),
        )
    }

    pub(crate) fn in_domain(
        domain: Domain,
        schema_key: impl Into<String>,
        row_pk: RowPk,
    ) -> Self {
        Self::new(domain, schema_key, row_pk)
    }

    #[cfg(test)]
    pub(crate) fn exact(
        branch_id: impl Into<String>,
        untracked: bool,
        file_id: Option<String>,
        schema_key: impl Into<String>,
        row_pk: RowPk,
    ) -> Self {
        Self::new(
            Domain::exact_file(branch_id, untracked, file_id),
            schema_key,
            row_pk,
        )
    }

    pub(crate) fn domain(&self) -> &Domain {
        &self.domain
    }

    pub(crate) fn schema_key(&self) -> &str {
        &self.schema_key
    }

    pub(crate) fn schema_key_owned(&self) -> String {
        self.schema_key.clone()
    }

    pub(crate) fn row_pk(&self) -> &RowPk {
        &self.row_pk
    }

    pub(crate) fn row_pk_owned(&self) -> RowPk {
        self.row_pk.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DomainSchemaIdentity {
    domain: Domain,
    schema_key: String,
}

impl DomainSchemaIdentity {
    pub(crate) fn new(domain: Domain, schema_key: impl Into<String>) -> Self {
        Self {
            domain: domain.schema_catalog_domain(),
            schema_key: schema_key.into(),
        }
    }

    pub(crate) fn fingerprint_component(&self) -> String {
        format!(
            "{}|{}",
            self.domain.fingerprint_component(),
            self.schema_key
        )
    }
}

pub(crate) fn committed_row_ref_is_exact_branch_scoped(
    row: MaterializedHotStateRowRef<'_>,
    branch_id: &str,
) -> bool {
    row.branch_id() == branch_id && row.global() == (row.branch_id() == GLOBAL_BRANCH_ID)
}

fn nullable_filter_from_option(value: Option<&String>) -> NullableKeyFilter<String> {
    value.map_or(NullableKeyFilter::Null, |value| {
        NullableKeyFilter::Value(value.clone())
    })
}
