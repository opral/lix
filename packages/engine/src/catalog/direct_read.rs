use crate::catalog::{SurfaceBinding, SurfaceFamily, SurfaceVariant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogDirectReadSemantics {
    StateHistoryActiveVersion,
    EntityHistoryActiveVersion,
    FileHistory { active_version_lineage: bool },
    DirectoryHistoryActiveVersion,
}

pub(crate) fn direct_read_semantics(
    surface_binding: &SurfaceBinding,
) -> Option<CatalogDirectReadSemantics> {
    match (
        surface_binding.descriptor.surface_family,
        surface_binding.descriptor.surface_variant,
        surface_binding.descriptor.public_name.as_str(),
    ) {
        (SurfaceFamily::State, SurfaceVariant::History, "lix_state_history") => {
            Some(CatalogDirectReadSemantics::StateHistoryActiveVersion)
        }
        (SurfaceFamily::Entity, SurfaceVariant::History, _) => {
            Some(CatalogDirectReadSemantics::EntityHistoryActiveVersion)
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_file_history") => {
            Some(CatalogDirectReadSemantics::FileHistory {
                active_version_lineage: true,
            })
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_file_history_by_version") => {
            Some(CatalogDirectReadSemantics::FileHistory {
                active_version_lineage: false,
            })
        }
        (SurfaceFamily::Filesystem, SurfaceVariant::History, "lix_directory_history") => {
            Some(CatalogDirectReadSemantics::DirectoryHistoryActiveVersion)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{direct_read_semantics, CatalogDirectReadSemantics};
    use crate::catalog::build_builtin_surface_registry;

    #[test]
    fn builtin_history_surfaces_expose_direct_read_semantics() {
        let registry = build_builtin_surface_registry();

        let state = registry
            .bind_relation_name("lix_state_history")
            .expect("lix_state_history should bind");
        assert_eq!(
            direct_read_semantics(&state),
            Some(CatalogDirectReadSemantics::StateHistoryActiveVersion)
        );

        let file = registry
            .bind_relation_name("lix_file_history")
            .expect("lix_file_history should bind");
        assert_eq!(
            direct_read_semantics(&file),
            Some(CatalogDirectReadSemantics::FileHistory {
                active_version_lineage: true,
            })
        );

        let file_by_version = registry
            .bind_relation_name("lix_file_history_by_version")
            .expect("lix_file_history_by_version should bind");
        assert_eq!(
            direct_read_semantics(&file_by_version),
            Some(CatalogDirectReadSemantics::FileHistory {
                active_version_lineage: false,
            })
        );

        let directory = registry
            .bind_relation_name("lix_directory_history")
            .expect("lix_directory_history should bind");
        assert_eq!(
            direct_read_semantics(&directory),
            Some(CatalogDirectReadSemantics::DirectoryHistoryActiveVersion)
        );
    }
}
