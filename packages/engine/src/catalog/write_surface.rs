use crate::catalog::{
    bind_surface_relation, DefaultScopeSemantics, FilesystemProjectionScope,
    FilesystemRelationKind, RelationBindContext, RelationBinding, ResolvedSurface, SurfaceFamily,
};
use crate::LixError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogWriteTargetKind {
    State,
    Entity,
    Filesystem,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogWriteVersionSemantics {
    /// Uses the requested active version by default and rejects explicit
    /// `version_id` write targeting through the public surface.
    ActiveVersionDefaultRejectedVersionId,
    /// Uses the requested active version by default.
    ActiveVersionDefault,
    /// Requires an explicit `version_id` target for bounded writes.
    ExplicitVersionRequired,
    /// Global admin write lane.
    GlobalAdmin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogAdminWriteBehavior {
    Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogWriteSurfaceSemantics {
    pub(crate) target_kind: CatalogWriteTargetKind,
    pub(crate) version_semantics: CatalogWriteVersionSemantics,
    pub(crate) filesystem_kind: Option<FilesystemRelationKind>,
    pub(crate) filesystem_scope: Option<FilesystemProjectionScope>,
    pub(crate) supports_untracked_writes: bool,
    pub(crate) admin_behavior: Option<CatalogAdminWriteBehavior>,
}

pub(crate) fn write_surface_semantics(
    surface_binding: &ResolvedSurface,
) -> Result<Option<CatalogWriteSurfaceSemantics>, LixError> {
    if !surface_binding.resolution_capabilities.semantic_write {
        return Ok(None);
    }

    let semantics = match surface_binding.descriptor.surface_family {
        SurfaceFamily::State => Some(CatalogWriteSurfaceSemantics {
            target_kind: CatalogWriteTargetKind::State,
            version_semantics: state_version_semantics(surface_binding),
            filesystem_kind: None,
            filesystem_scope: None,
            supports_untracked_writes: true,
            admin_behavior: None,
        }),
        SurfaceFamily::Entity => Some(CatalogWriteSurfaceSemantics {
            target_kind: CatalogWriteTargetKind::Entity,
            version_semantics: version_semantics_for_binding(surface_binding),
            filesystem_kind: None,
            filesystem_scope: None,
            supports_untracked_writes: true,
            admin_behavior: None,
        }),
        SurfaceFamily::Filesystem => filesystem_write_surface_semantics(surface_binding)?,
        SurfaceFamily::Admin => admin_write_surface_semantics(surface_binding),
        SurfaceFamily::Change => None,
    };

    Ok(semantics)
}

fn filesystem_write_surface_semantics(
    surface_binding: &ResolvedSurface,
) -> Result<Option<CatalogWriteSurfaceSemantics>, LixError> {
    let Some(RelationBinding::FilesystemRelation(binding)) =
        bind_surface_relation(surface_binding, RelationBindContext::default())?
    else {
        return Ok(None);
    };

    Ok(Some(CatalogWriteSurfaceSemantics {
        target_kind: CatalogWriteTargetKind::Filesystem,
        version_semantics: match binding.scope {
            FilesystemProjectionScope::ActiveVersion => {
                CatalogWriteVersionSemantics::ActiveVersionDefault
            }
            FilesystemProjectionScope::ExplicitVersion => {
                CatalogWriteVersionSemantics::ExplicitVersionRequired
            }
        },
        filesystem_kind: Some(binding.kind),
        filesystem_scope: Some(binding.scope),
        supports_untracked_writes: true,
        admin_behavior: None,
    }))
}

fn admin_write_surface_semantics(
    surface_binding: &ResolvedSurface,
) -> Option<CatalogWriteSurfaceSemantics> {
    (surface_binding.descriptor.surface_family == SurfaceFamily::Admin
        && surface_binding.default_scope == DefaultScopeSemantics::GlobalAdmin)
        .then_some(CatalogWriteSurfaceSemantics {
            target_kind: CatalogWriteTargetKind::Admin,
            version_semantics: CatalogWriteVersionSemantics::GlobalAdmin,
            filesystem_kind: None,
            filesystem_scope: None,
            supports_untracked_writes: true,
            admin_behavior: Some(CatalogAdminWriteBehavior::Version),
        })
}

fn version_semantics_for_binding(
    surface_binding: &ResolvedSurface,
) -> CatalogWriteVersionSemantics {
    match surface_binding.default_scope {
        crate::catalog::DefaultScopeSemantics::ActiveVersion => {
            CatalogWriteVersionSemantics::ActiveVersionDefault
        }
        crate::catalog::DefaultScopeSemantics::ExplicitVersion => {
            CatalogWriteVersionSemantics::ExplicitVersionRequired
        }
        crate::catalog::DefaultScopeSemantics::GlobalAdmin => {
            CatalogWriteVersionSemantics::GlobalAdmin
        }
        crate::catalog::DefaultScopeSemantics::History
        | crate::catalog::DefaultScopeSemantics::WorkingChanges => {
            CatalogWriteVersionSemantics::ActiveVersionDefault
        }
    }
}

fn state_version_semantics(surface_binding: &ResolvedSurface) -> CatalogWriteVersionSemantics {
    if surface_binding.default_scope == DefaultScopeSemantics::ActiveVersion
        && !surface_binding
            .descriptor
            .surface_traits
            .exposes_version_column
    {
        CatalogWriteVersionSemantics::ActiveVersionDefaultRejectedVersionId
    } else {
        version_semantics_for_binding(surface_binding)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        write_surface_semantics, CatalogAdminWriteBehavior, CatalogWriteSurfaceSemantics,
        CatalogWriteTargetKind, CatalogWriteVersionSemantics,
    };
    use crate::catalog::{
        build_builtin_surface_registry, FilesystemProjectionScope, FilesystemRelationKind,
    };

    #[test]
    fn builtin_state_and_admin_surfaces_expose_catalog_owned_write_metadata() {
        let registry = build_builtin_surface_registry();

        let state = registry
            .bind_relation_name("lix_state")
            .expect("lix_state should bind");
        assert_eq!(
            write_surface_semantics(&state).expect("catalog write semantics should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::State,
                version_semantics:
                    CatalogWriteVersionSemantics::ActiveVersionDefaultRejectedVersionId,
                filesystem_kind: None,
                filesystem_scope: None,
                supports_untracked_writes: true,
                admin_behavior: None,
            })
        );

        let state_by_version = registry
            .bind_relation_name("lix_state_by_version")
            .expect("lix_state_by_version should bind");
        assert_eq!(
            write_surface_semantics(&state_by_version)
                .expect("catalog write semantics should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::State,
                version_semantics: CatalogWriteVersionSemantics::ExplicitVersionRequired,
                filesystem_kind: None,
                filesystem_scope: None,
                supports_untracked_writes: true,
                admin_behavior: None,
            })
        );

        let version = registry
            .bind_relation_name("lix_version")
            .expect("lix_version should bind");
        assert_eq!(
            write_surface_semantics(&version).expect("catalog write semantics should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::Admin,
                version_semantics: CatalogWriteVersionSemantics::GlobalAdmin,
                filesystem_kind: None,
                filesystem_scope: None,
                supports_untracked_writes: true,
                admin_behavior: Some(CatalogAdminWriteBehavior::Version),
            })
        );
    }

    #[test]
    fn builtin_filesystem_surfaces_expose_kind_scope_and_untracked_metadata() {
        let registry = build_builtin_surface_registry();

        let file = registry
            .bind_relation_name("lix_file")
            .expect("lix_file should bind");
        assert_eq!(
            write_surface_semantics(&file).expect("catalog write semantics should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::Filesystem,
                version_semantics: CatalogWriteVersionSemantics::ActiveVersionDefault,
                filesystem_kind: Some(FilesystemRelationKind::File),
                filesystem_scope: Some(FilesystemProjectionScope::ActiveVersion),
                supports_untracked_writes: true,
                admin_behavior: None,
            })
        );

        let directory_by_version = registry
            .bind_relation_name("lix_directory_by_version")
            .expect("lix_directory_by_version should bind");
        assert_eq!(
            write_surface_semantics(&directory_by_version)
                .expect("catalog write semantics should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::Filesystem,
                version_semantics: CatalogWriteVersionSemantics::ExplicitVersionRequired,
                filesystem_kind: Some(FilesystemRelationKind::Directory),
                filesystem_scope: Some(FilesystemProjectionScope::ExplicitVersion),
                supports_untracked_writes: true,
                admin_behavior: None,
            })
        );
    }

    #[test]
    fn read_only_surfaces_do_not_expose_write_metadata() {
        let registry = build_builtin_surface_registry();
        let history = registry
            .bind_relation_name("lix_file_history")
            .expect("lix_file_history should bind");

        assert_eq!(
            write_surface_semantics(&history).expect("catalog write semantics should succeed"),
            None
        );
    }
}
