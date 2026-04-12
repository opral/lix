use crate::catalog::{
    bind_surface_relation, direct_read_semantics, DefaultScopeSemantics, FilesystemProjectionScope,
    FilesystemRelationKind, RelationBindContext, RelationBinding, SurfaceBinding, SurfaceFamily,
    SurfaceVariant,
};
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogScanVersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogFilesystemScanSemantics {
    pub(crate) kind: FilesystemRelationKind,
    pub(crate) version_scope: CatalogScanVersionScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogAdminScanKind {
    Version,
}

pub(crate) fn filesystem_scan_semantics(
    surface_binding: &SurfaceBinding,
) -> Result<Option<CatalogFilesystemScanSemantics>, LixError> {
    if surface_binding.descriptor.surface_family != SurfaceFamily::Filesystem {
        return Ok(None);
    }

    if let Some(RelationBinding::FilesystemRelation(binding)) =
        bind_surface_relation(surface_binding, RelationBindContext::default())?
    {
        return Ok(Some(CatalogFilesystemScanSemantics {
            kind: binding.kind,
            version_scope: match binding.scope {
                FilesystemProjectionScope::ActiveVersion => CatalogScanVersionScope::ActiveVersion,
                FilesystemProjectionScope::ExplicitVersion => {
                    CatalogScanVersionScope::ExplicitVersion
                }
            },
        }));
    }

    Ok(match direct_read_semantics(surface_binding) {
        Some(crate::catalog::CatalogDirectReadSemantics::FileHistory { .. }) => {
            Some(CatalogFilesystemScanSemantics {
                kind: FilesystemRelationKind::File,
                version_scope: CatalogScanVersionScope::History,
            })
        }
        Some(crate::catalog::CatalogDirectReadSemantics::DirectoryHistoryActiveVersion) => {
            Some(CatalogFilesystemScanSemantics {
                kind: FilesystemRelationKind::Directory,
                version_scope: CatalogScanVersionScope::History,
            })
        }
        _ => None,
    })
}

pub(crate) fn is_working_changes_surface(surface_binding: &SurfaceBinding) -> bool {
    surface_binding.descriptor.surface_family == SurfaceFamily::Change
        && surface_binding.descriptor.surface_variant == SurfaceVariant::WorkingChanges
}

pub(crate) fn admin_scan_kind(surface_binding: &SurfaceBinding) -> Option<CatalogAdminScanKind> {
    (surface_binding.descriptor.surface_family == SurfaceFamily::Admin
        && surface_binding.default_scope == DefaultScopeSemantics::GlobalAdmin)
        .then_some(CatalogAdminScanKind::Version)
}

#[cfg(test)]
mod tests {
    use super::{
        admin_scan_kind, filesystem_scan_semantics, is_working_changes_surface,
        CatalogAdminScanKind, CatalogFilesystemScanSemantics, CatalogScanVersionScope,
    };
    use crate::catalog::{build_builtin_surface_registry, FilesystemRelationKind};

    #[test]
    fn builtin_filesystem_surfaces_expose_catalog_scan_semantics() {
        let registry = build_builtin_surface_registry();

        let file = registry
            .bind_relation_name("lix_file")
            .expect("lix_file should bind");
        assert_eq!(
            filesystem_scan_semantics(&file).expect("filesystem scan semantics should resolve"),
            Some(CatalogFilesystemScanSemantics {
                kind: FilesystemRelationKind::File,
                version_scope: CatalogScanVersionScope::ActiveVersion,
            })
        );

        let file_history = registry
            .bind_relation_name("lix_file_history")
            .expect("lix_file_history should bind");
        assert_eq!(
            filesystem_scan_semantics(&file_history)
                .expect("filesystem scan semantics should resolve"),
            Some(CatalogFilesystemScanSemantics {
                kind: FilesystemRelationKind::File,
                version_scope: CatalogScanVersionScope::History,
            })
        );
    }

    #[test]
    fn builtin_working_changes_and_admin_scans_come_from_catalog() {
        let registry = build_builtin_surface_registry();

        let working_changes = registry
            .bind_relation_name("lix_working_changes")
            .expect("lix_working_changes should bind");
        assert!(is_working_changes_surface(&working_changes));

        let version = registry
            .bind_relation_name("lix_version")
            .expect("lix_version should bind");
        assert_eq!(
            admin_scan_kind(&version),
            Some(CatalogAdminScanKind::Version)
        );
    }
}
