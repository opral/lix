use crate::catalog::{ResolvedRelation, SurfaceFamily, SurfaceVariant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogHistoryReadSemantics {
    StateHistoryActiveVersion,
    EntityHistoryActiveVersion,
    FileHistory { active_version_lineage: bool },
    DirectoryHistoryActiveVersion,
}

pub(crate) fn history_read_semantics(
    resolved_relation: &ResolvedRelation,
) -> Option<CatalogHistoryReadSemantics> {
    match (
        resolved_relation.descriptor.surface_family,
        resolved_relation.descriptor.surface_variant,
        resolved_relation.descriptor.public_name.as_str(),
    ) {
        (SurfaceFamily::State, SurfaceVariant::History, "lix_state_history") => {
            Some(CatalogHistoryReadSemantics::StateHistoryActiveVersion)
        }
        (SurfaceFamily::Entity, SurfaceVariant::History, _) => {
            Some(CatalogHistoryReadSemantics::EntityHistoryActiveVersion)
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_file_history") => {
            Some(CatalogHistoryReadSemantics::FileHistory {
                active_version_lineage: true,
            })
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_file_history_by_version") => {
            Some(CatalogHistoryReadSemantics::FileHistory {
                active_version_lineage: false,
            })
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_directory_history") => {
            Some(CatalogHistoryReadSemantics::DirectoryHistoryActiveVersion)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{history_read_semantics, CatalogHistoryReadSemantics};
    use crate::catalog::build_builtin_surface_registry;

    #[test]
    fn builtin_history_surfaces_expose_history_read_semantics() {
        let registry = build_builtin_surface_registry();

        let state = registry
            .bind_relation_name("lix_state_history")
            .expect("lix_state_history should bind");
        assert_eq!(
            history_read_semantics(&state),
            Some(CatalogHistoryReadSemantics::StateHistoryActiveVersion)
        );

        let file = registry
            .bind_relation_name("lix_file_history")
            .expect("lix_file_history should bind");
        assert_eq!(
            history_read_semantics(&file),
            Some(CatalogHistoryReadSemantics::FileHistory {
                active_version_lineage: true,
            })
        );

        let file_by_version = registry
            .bind_relation_name("lix_file_history_by_version")
            .expect("lix_file_history_by_version should bind");
        assert_eq!(
            history_read_semantics(&file_by_version),
            Some(CatalogHistoryReadSemantics::FileHistory {
                active_version_lineage: false,
            })
        );

        let directory = registry
            .bind_relation_name("lix_directory_history")
            .expect("lix_directory_history should bind");
        assert_eq!(
            history_read_semantics(&directory),
            Some(CatalogHistoryReadSemantics::DirectoryHistoryActiveVersion)
        );
    }
}
