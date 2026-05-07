use crate::entity_identity::EntityIdentity;
use crate::live_state::MaterializedLiveStateRow;
use crate::{NullableKeyFilter, GLOBAL_VERSION_ID};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Domain {
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) file_scope: DomainFileScope,
}

impl Domain {
    pub(crate) fn exact_file(
        version_id: impl Into<String>,
        untracked: bool,
        file_id: Option<String>,
    ) -> Self {
        Self {
            version_id: version_id.into(),
            untracked,
            file_scope: DomainFileScope::Exact(file_id),
        }
    }

    pub(crate) fn any_file(version_id: impl Into<String>, untracked: bool) -> Self {
        Self {
            version_id: version_id.into(),
            untracked,
            file_scope: DomainFileScope::Any,
        }
    }

    pub(crate) fn schema_catalog(version_id: impl Into<String>, untracked: bool) -> Self {
        Self::any_file(version_id, untracked)
    }

    pub(crate) fn for_live_row(row: &MaterializedLiveStateRow) -> Self {
        Self::exact_file(row.version_id.clone(), row.untracked, row.file_id.clone())
    }

    pub(crate) fn schema_catalog_domain(&self) -> Self {
        // Schema definitions are version + durability scoped. They are not
        // owned by a data file, so schema catalog lookup deliberately erases
        // row file scope into `Any`.
        Self::schema_catalog(self.version_id.clone(), self.untracked)
    }

    pub(crate) fn with_untracked(&self, untracked: bool) -> Self {
        Self {
            version_id: self.version_id.clone(),
            untracked,
            file_scope: self.file_scope.clone(),
        }
    }

    pub(crate) fn with_file_scope(&self, file_scope: DomainFileScope) -> Self {
        Self {
            version_id: self.version_id.clone(),
            untracked: self.untracked,
            file_scope,
        }
    }

    pub(crate) fn file_filters(&self) -> Vec<NullableKeyFilter<String>> {
        match &self.file_scope {
            DomainFileScope::Any => Vec::new(),
            DomainFileScope::Exact(file_id) => vec![nullable_filter_from_option(file_id)],
        }
    }

    pub(crate) fn contains(&self, row: &MaterializedLiveStateRow) -> bool {
        row.version_id == self.version_id
            && row.untracked == self.untracked
            && committed_row_is_exact_version_scoped(row, &self.version_id)
            && match &self.file_scope {
                DomainFileScope::Any => true,
                DomainFileScope::Exact(file_id) => row.file_id == *file_id,
            }
    }

    pub(crate) fn reachable_target_domains(&self) -> Vec<Self> {
        if self.untracked {
            vec![self.with_untracked(false), self.clone()]
        } else {
            vec![self.clone()]
        }
    }

    pub(crate) fn source_domains_that_can_reach(&self) -> Vec<Self> {
        if self.untracked {
            vec![self.clone()]
        } else {
            vec![self.clone(), self.with_untracked(true)]
        }
    }

    pub(crate) fn can_reach(&self, target: &Self) -> bool {
        self.version_id == target.version_id
            && self.file_scope == target.file_scope
            && (self.untracked || !target.untracked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum DomainFileScope {
    Any,
    Exact(Option<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DomainRowIdentity {
    pub(crate) domain: Domain,
    pub(crate) schema_key: String,
    pub(crate) entity_id: EntityIdentity,
}

impl DomainRowIdentity {
    pub(crate) fn new(
        domain: Domain,
        schema_key: impl Into<String>,
        entity_id: EntityIdentity,
    ) -> Self {
        Self {
            domain,
            schema_key: schema_key.into(),
            entity_id,
        }
    }

    pub(crate) fn from_live_row(row: &MaterializedLiveStateRow) -> Self {
        Self::new(
            Domain::for_live_row(row),
            row.schema_key.clone(),
            row.entity_id.clone(),
        )
    }

    #[cfg(test)]
    pub(crate) fn exact(
        version_id: impl Into<String>,
        untracked: bool,
        file_id: Option<String>,
        schema_key: impl Into<String>,
        entity_id: EntityIdentity,
    ) -> Self {
        Self::new(
            Domain::exact_file(version_id, untracked, file_id),
            schema_key,
            entity_id,
        )
    }

    pub(crate) fn with_domain(&self, domain: Domain) -> Self {
        Self {
            domain,
            schema_key: self.schema_key.clone(),
            entity_id: self.entity_id.clone(),
        }
    }

    pub(crate) fn reachable_target_identities(&self) -> Vec<Self> {
        self.domain
            .reachable_target_domains()
            .into_iter()
            .map(|domain| self.with_domain(domain))
            .collect()
    }

    pub(crate) fn source_identities_that_can_reach(&self) -> Vec<Self> {
        self.domain
            .source_domains_that_can_reach()
            .into_iter()
            .map(|domain| self.with_domain(domain))
            .collect()
    }
}

pub(crate) fn committed_row_is_exact_version_scoped(
    row: &MaterializedLiveStateRow,
    version_id: &str,
) -> bool {
    row.version_id == version_id && row.global == (row.version_id == GLOBAL_VERSION_ID)
}

fn nullable_filter_from_option(value: &Option<String>) -> NullableKeyFilter<String> {
    match value {
        Some(value) => NullableKeyFilter::Value(value.clone()),
        None => NullableKeyFilter::Null,
    }
}
