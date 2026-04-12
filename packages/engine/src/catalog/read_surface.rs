use crate::catalog::{
    state_by_version_relation_name, DefaultScopeSemantics, SurfaceBinding, SurfaceFamily,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CatalogReadPreparationSemantics {
    pub(crate) requires_current_version_heads: bool,
}

pub(crate) fn explicit_version_counterpart_surface_name(
    surface_binding: &SurfaceBinding,
    missing_columns: &[String],
) -> Option<String> {
    let requests_version_column = missing_columns
        .iter()
        .any(|column| matches!(column.as_str(), "version_id" | "lixcol_version_id"));

    if !requests_version_column {
        return None;
    }

    (surface_binding.descriptor.surface_family == SurfaceFamily::State
        && surface_binding.default_scope == DefaultScopeSemantics::ActiveVersion
        && !surface_binding
            .descriptor
            .surface_traits
            .exposes_version_column)
        .then(|| state_by_version_relation_name().to_string())
}

pub(crate) fn read_preparation_semantics(
    surface_binding: &SurfaceBinding,
) -> CatalogReadPreparationSemantics {
    CatalogReadPreparationSemantics {
        requires_current_version_heads: surface_binding.descriptor.surface_family
            == SurfaceFamily::Admin
            && surface_binding.default_scope == DefaultScopeSemantics::GlobalAdmin,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        explicit_version_counterpart_surface_name, read_preparation_semantics,
        CatalogReadPreparationSemantics,
    };
    use crate::catalog::build_builtin_surface_registry;

    #[test]
    fn builtin_active_state_surface_exposes_catalog_owned_explicit_version_counterpart() {
        let registry = build_builtin_surface_registry();
        let state = registry
            .bind_relation_name("lix_state")
            .expect("lix_state should bind");

        assert_eq!(
            explicit_version_counterpart_surface_name(&state, &["version_id".to_string()]),
            Some("lix_state_by_version".to_string())
        );
    }

    #[test]
    fn explicit_version_counterpart_is_absent_for_by_version_surface() {
        let registry = build_builtin_surface_registry();
        let state = registry
            .bind_relation_name("lix_state_by_version")
            .expect("lix_state_by_version should bind");

        assert_eq!(
            explicit_version_counterpart_surface_name(&state, &["version_id".to_string()]),
            None
        );
    }

    #[test]
    fn admin_version_surface_exposes_current_heads_requirement() {
        let registry = build_builtin_surface_registry();
        let version = registry
            .bind_relation_name("lix_version")
            .expect("lix_version should bind");

        assert_eq!(
            read_preparation_semantics(&version),
            CatalogReadPreparationSemantics {
                requires_current_version_heads: true,
            }
        );
    }
}
