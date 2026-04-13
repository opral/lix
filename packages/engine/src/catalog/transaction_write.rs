use std::collections::BTreeSet;

use crate::catalog::{ResolvedRelation, SurfaceFamily, SurfaceVariant};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CatalogTransactionInsertSemantics {
    pub(crate) coalescable: bool,
    pub(crate) transaction_sensitive_columns: BTreeSet<String>,
}

pub(crate) fn transaction_insert_semantics(
    resolved_relation: &ResolvedRelation,
) -> Option<CatalogTransactionInsertSemantics> {
    match (
        resolved_relation.descriptor.surface_family,
        resolved_relation.descriptor.surface_variant,
        resolved_relation.descriptor.public_name.as_str(),
    ) {
        (SurfaceFamily::State, SurfaceVariant::ByVersion, "lix_state_by_version") => {
            Some(CatalogTransactionInsertSemantics {
                coalescable: true,
                transaction_sensitive_columns: BTreeSet::new(),
            })
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::Default, "lix_file") => {
            Some(CatalogTransactionInsertSemantics {
                coalescable: true,
                transaction_sensitive_columns: BTreeSet::from([
                    "global".to_string(),
                    "untracked".to_string(),
                    "version_id".to_string(),
                ]),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{transaction_insert_semantics, CatalogTransactionInsertSemantics};
    use crate::catalog::build_builtin_surface_registry;
    use std::collections::BTreeSet;

    #[test]
    fn builtin_surfaces_expose_transaction_insert_semantics() {
        let registry = build_builtin_surface_registry();

        let state = registry
            .bind_relation_name("lix_state_by_version")
            .expect("lix_state_by_version should bind");
        assert_eq!(
            transaction_insert_semantics(&state),
            Some(CatalogTransactionInsertSemantics {
                coalescable: true,
                transaction_sensitive_columns: BTreeSet::new(),
            })
        );

        let file = registry
            .bind_relation_name("lix_file")
            .expect("lix_file should bind");
        assert_eq!(
            transaction_insert_semantics(&file),
            Some(CatalogTransactionInsertSemantics {
                coalescable: true,
                transaction_sensitive_columns: BTreeSet::from([
                    "global".to_string(),
                    "untracked".to_string(),
                    "version_id".to_string(),
                ]),
            })
        );
    }
}
