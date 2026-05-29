use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
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

    pub(crate) fn for_live_row(row: &MaterializedLiveStateRow) -> Self {
        Self::exact_file(row.branch_id.clone(), row.untracked, row.file_id.clone())
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

    #[expect(clippy::ref_option)]
    pub(crate) fn is_exact_file(&self, file_id: &Option<String>) -> bool {
        matches!(&self.file_scope, DomainFileScope::Exact(exact) if exact == file_id)
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

    pub(crate) fn contains(&self, row: &MaterializedLiveStateRow) -> bool {
        row.branch_id == self.branch_id
            && row.untracked == self.untracked
            && committed_row_is_exact_branch_scoped(row, &self.branch_id)
            && match &self.file_scope {
                DomainFileScope::Any => true,
                DomainFileScope::Exact(file_id) => row.file_id == *file_id,
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

    pub(crate) fn file_owner_domains(&self) -> Vec<Self> {
        self.reachable_target_domains()
    }

    pub(crate) fn directory_parent_domains(&self) -> Vec<Self> {
        self.reachable_target_domains()
    }

    pub(crate) fn branch_descriptor_domains_for_ref_delete(&self) -> Vec<Self> {
        self.source_domains_that_can_reach()
    }

    pub(crate) fn file_scoped_row_domains_for_file_descriptor_delete(&self) -> Vec<Self> {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DomainRowIdentity {
    domain: Domain,
    schema_key: String,
    entity_pk: EntityPk,
}

impl DomainRowIdentity {
    pub(crate) fn new(domain: Domain, schema_key: impl Into<String>, entity_pk: EntityPk) -> Self {
        Self {
            domain,
            schema_key: schema_key.into(),
            entity_pk,
        }
    }

    pub(crate) fn from_live_row(row: &MaterializedLiveStateRow) -> Self {
        Self::new(
            Domain::for_live_row(row),
            row.schema_key.clone(),
            row.entity_pk.clone(),
        )
    }

    pub(crate) fn in_domain(
        domain: Domain,
        schema_key: impl Into<String>,
        entity_pk: EntityPk,
    ) -> Self {
        Self::new(domain, schema_key, entity_pk)
    }

    #[cfg(test)]
    pub(crate) fn exact(
        branch_id: impl Into<String>,
        untracked: bool,
        file_id: Option<String>,
        schema_key: impl Into<String>,
        entity_pk: EntityPk,
    ) -> Self {
        Self::new(
            Domain::exact_file(branch_id, untracked, file_id),
            schema_key,
            entity_pk,
        )
    }

    pub(crate) fn with_domain(&self, domain: Domain) -> Self {
        Self {
            domain,
            schema_key: self.schema_key.clone(),
            entity_pk: self.entity_pk.clone(),
        }
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

    pub(crate) fn entity_pk(&self) -> &EntityPk {
        &self.entity_pk
    }

    pub(crate) fn entity_pk_owned(&self) -> EntityPk {
        self.entity_pk.clone()
    }

    pub(crate) fn matches_parts(
        &self,
        domain: &Domain,
        schema_key: &str,
        entity_pk: &EntityPk,
    ) -> bool {
        &self.domain == domain && self.schema_key == schema_key && &self.entity_pk == entity_pk
    }

    pub(crate) fn reachable_target_identities(&self) -> Vec<Self> {
        self.domain
            .fk_target_domains()
            .into_iter()
            .map(|domain| self.with_domain(domain))
            .collect()
    }

    pub(crate) fn source_identities_that_can_reach(&self) -> Vec<Self> {
        self.domain
            .fk_source_domains_for_target()
            .into_iter()
            .map(|domain| self.with_domain(domain))
            .collect()
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

pub(crate) fn committed_row_is_exact_branch_scoped(
    row: &MaterializedLiveStateRow,
    branch_id: &str,
) -> bool {
    row.branch_id == branch_id && row.global == (row.branch_id == GLOBAL_BRANCH_ID)
}

fn nullable_filter_from_option(value: Option<&String>) -> NullableKeyFilter<String> {
    value.map_or(NullableKeyFilter::Null, |value| {
        NullableKeyFilter::Value(value.clone())
    })
}
