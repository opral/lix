use crate::catalog::{SurfaceBinding, SurfaceFamily, SurfaceVariant};
use crate::contracts::GLOBAL_VERSION_ID;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverlayLane {
    GlobalTracked,
    LocalTracked,
    GlobalUntracked,
    LocalUntracked,
}

pub(crate) fn public_selector_version_column(surface_family: SurfaceFamily) -> &'static str {
    match surface_family {
        SurfaceFamily::State => "version_id",
        SurfaceFamily::Entity | SurfaceFamily::Filesystem => "lixcol_version_id",
        SurfaceFamily::Admin | SurfaceFamily::Change => "version_id",
    }
}

pub(crate) fn public_selector_column_name(
    surface_family: SurfaceFamily,
    canonical_column: &str,
) -> Option<String> {
    match surface_family {
        SurfaceFamily::State => match canonical_column {
            "entity_id" => Some("entity_id".to_string()),
            "schema_key" => Some("schema_key".to_string()),
            "file_id" => Some("file_id".to_string()),
            "plugin_key" => Some("plugin_key".to_string()),
            "schema_version" => Some("schema_version".to_string()),
            "version_id" => Some("version_id".to_string()),
            "global" => Some("global".to_string()),
            "untracked" => Some("untracked".to_string()),
            "metadata" => Some("metadata".to_string()),
            "writer_key" => Some("writer_key".to_string()),
            _ => None,
        },
        SurfaceFamily::Entity => match canonical_column {
            "entity_id" => Some("lixcol_entity_id".to_string()),
            "schema_key" => Some("lixcol_schema_key".to_string()),
            "file_id" => Some("lixcol_file_id".to_string()),
            "plugin_key" => Some("lixcol_plugin_key".to_string()),
            "schema_version" => Some("lixcol_schema_version".to_string()),
            "version_id" => Some("lixcol_version_id".to_string()),
            "global" => Some("lixcol_global".to_string()),
            "untracked" => Some("lixcol_untracked".to_string()),
            "metadata" => Some("lixcol_metadata".to_string()),
            "writer_key" => Some("lixcol_writer_key".to_string()),
            _ => Some(canonical_column.to_string()),
        },
        SurfaceFamily::Filesystem => match canonical_column {
            "id" => Some("id".to_string()),
            "path" => Some("path".to_string()),
            "name" => Some("name".to_string()),
            "parent_id" => Some("parent_id".to_string()),
            "directory_id" => Some("directory_id".to_string()),
            "hidden" => Some("hidden".to_string()),
            "entity_id" => Some("lixcol_entity_id".to_string()),
            "schema_key" => Some("lixcol_schema_key".to_string()),
            "schema_version" => Some("lixcol_schema_version".to_string()),
            "version_id" => Some("lixcol_version_id".to_string()),
            "global" => Some("lixcol_global".to_string()),
            "untracked" => Some("lixcol_untracked".to_string()),
            "metadata" => Some("lixcol_metadata".to_string()),
            "writer_key" => Some("lixcol_writer_key".to_string()),
            _ => None,
        },
        SurfaceFamily::Admin => match canonical_column {
            "id" => Some("id".to_string()),
            "name" => Some("name".to_string()),
            "hidden" => Some("hidden".to_string()),
            "commit_id" => Some("commit_id".to_string()),
            "version_id" => Some("version_id".to_string()),
            "account_id" => Some("account_id".to_string()),
            _ => None,
        },
        SurfaceFamily::Change => None,
    }
}

pub(crate) fn canonical_filter_column_name(expr: &Expr) -> Option<&'static str> {
    let column = match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| identifier.value.as_str()),
        Expr::Nested(inner) => return canonical_filter_column_name(inner),
        _ => None,
    }?;

    match column.to_ascii_lowercase().as_str() {
        "schema_key" => Some("schema_key"),
        "entity_id" => Some("entity_id"),
        "file_id" => Some("file_id"),
        "version_id" | "lixcol_version_id" => Some("version_id"),
        _ => None,
    }
}

pub(crate) fn overlay_lanes(
    include_global_overlay: bool,
    include_untracked_overlay: bool,
) -> Vec<OverlayLane> {
    let mut lanes = vec![OverlayLane::LocalTracked];
    if include_untracked_overlay {
        lanes.insert(0, OverlayLane::LocalUntracked);
    }
    if include_global_overlay {
        if include_untracked_overlay {
            lanes.push(OverlayLane::GlobalUntracked);
        }
        lanes.push(OverlayLane::GlobalTracked);
    }
    lanes
}

pub(crate) fn overlay_lanes_for_version(
    version_id: &str,
    include_global_overlay: bool,
    include_untracked_overlay: bool,
) -> Vec<OverlayLane> {
    overlay_lanes(
        include_global_overlay && version_id != GLOBAL_VERSION_ID,
        include_untracked_overlay,
    )
}

pub(crate) fn effective_state_pushdown_predicates(
    surface_binding: &SurfaceBinding,
    predicates: &[Expr],
) -> Vec<Expr> {
    if !surface_supports_effective_state_pushdown(surface_binding) {
        return Vec::new();
    }

    predicates
        .iter()
        .filter(|predicate| state_predicate_is_pushdown_safe(predicate, surface_binding))
        .cloned()
        .collect()
}

fn surface_supports_effective_state_pushdown(surface_binding: &SurfaceBinding) -> bool {
    let family = surface_binding.descriptor.surface_family;
    let variant = surface_binding.descriptor.surface_variant;
    family == SurfaceFamily::State
        || (family == SurfaceFamily::Entity && variant == SurfaceVariant::History)
}

fn state_predicate_is_pushdown_safe(expr: &Expr, surface_binding: &SurfaceBinding) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => state_pushdown_column(left, surface_binding).is_some() && constant_like_expr(right),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            state_pushdown_column(expr, surface_binding).is_some()
                && list.iter().all(constant_like_expr)
        }
        Expr::Nested(inner) => state_predicate_is_pushdown_safe(inner, surface_binding),
        _ => false,
    }
}

fn state_pushdown_column<'a>(expr: &'a Expr, surface_binding: &SurfaceBinding) -> Option<&'a str> {
    let column = identifier_column_name(expr)?;
    match surface_binding.descriptor.surface_variant {
        SurfaceVariant::Default => match column.to_ascii_lowercase().as_str() {
            "schema_key" | "entity_id" | "file_id" | "plugin_key" | "schema_version" => {
                Some(column)
            }
            _ => None,
        },
        SurfaceVariant::ByVersion => match column.to_ascii_lowercase().as_str() {
            "schema_key" | "entity_id" | "file_id" | "plugin_key" | "schema_version"
            | "version_id" | "lixcol_version_id" => Some(column),
            _ => None,
        },
        SurfaceVariant::History => match column.to_ascii_lowercase().as_str() {
            "root_commit_id" | "lixcol_root_commit_id" => Some(column),
            _ => None,
        },
        SurfaceVariant::WorkingChanges => None,
    }
}

fn identifier_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| identifier.value.as_str()),
        Expr::Nested(inner) => identifier_column_name(inner),
        _ => None,
    }
}

fn constant_like_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Nested(inner) => constant_like_expr(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => constant_like_expr(expr),
        Expr::Cast { expr, .. } => constant_like_expr(expr),
        _ => false,
    }
}
