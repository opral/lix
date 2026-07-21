use std::collections::{BTreeMap, BTreeSet};

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, Ident, Query, Select, SelectFlavor, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, Value as SqlValue,
};

use crate::Value;
use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};

const MAX_COMPOSITE_PRIMARY_KEYS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryKeyReadBinding {
    Declined,
    Ready(BoundPrimaryKeyRead),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundPrimaryKeyRead {
    pub(crate) target: BoundPrimaryKeyReadTarget,
    pub(crate) projection: Vec<BoundPrimaryKeyProjection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundPrimaryKeyReadTarget {
    File {
        ids: Vec<String>,
    },
    Entity {
        schema_key: String,
        primary_key_columns: Vec<String>,
        /// Exact keys in schema primary-key order.
        keys: Vec<Vec<String>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundPrimaryKeyProjection {
    /// Index in the public surface/provider schema.
    pub(crate) source_index: usize,
    pub(crate) source_name: String,
    pub(crate) output_name: String,
}

/// Attempts to bind the deliberately small exact-primary-key SELECT language.
///
/// Declining is behavior preserving: the caller must run the existing DataFusion
/// path. This function therefore never turns syntax, name-resolution, parameter,
/// or type errors into native-path errors, and it performs no I/O.
pub(crate) fn bind_primary_key_read(
    statement: &DataFusionStatement,
    catalog: &PublicCatalog,
    params: &[Value],
) -> PrimaryKeyReadBinding {
    bind_primary_key_read_inner(statement, catalog, params)
        .map(PrimaryKeyReadBinding::Ready)
        .unwrap_or(PrimaryKeyReadBinding::Declined)
}

fn bind_primary_key_read_inner(
    statement: &DataFusionStatement,
    catalog: &PublicCatalog,
    params: &[Value],
) -> Option<BoundPrimaryKeyRead> {
    let DataFusionStatement::Statement(statement) = statement else {
        return None;
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return None;
    };
    let select = plain_select(query)?;
    let (surface, qualifier) = bind_single_table(select, catalog)?;

    let projection = bind_projection(select, surface, &qualifier)?;
    let selection = select.selection.as_ref()?;
    let mut parameter_indexes = BTreeSet::new();
    let mut constraints = BTreeMap::<String, BTreeSet<String>>::new();
    bind_constraints(
        selection,
        surface,
        &qualifier,
        params,
        &mut parameter_indexes,
        &mut constraints,
    )?;

    // Match DataFusion's positional count rule (the highest $N) and decline on
    // every mismatch so its established error remains authoritative.
    let expected_parameter_count = parameter_indexes.last().copied().unwrap_or(0);
    if params.len() != expected_parameter_count {
        return None;
    }

    let target = match &surface.kind {
        PublicSurfaceKind::File => {
            if constraints.len() != 1 {
                return None;
            }
            let ids = constraints.remove("id")?.into_iter().collect::<Vec<_>>();
            if ids.is_empty() {
                return None;
            }
            BoundPrimaryKeyReadTarget::File { ids }
        }
        PublicSurfaceKind::EntityBase { schema_key } => {
            let spec = catalog.entity_spec(schema_key)?;
            let primary_key_columns = spec
                .flat_string_primary_key_columns()?
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            if constraints.len() != primary_key_columns.len()
                || primary_key_columns
                    .iter()
                    .any(|column| !constraints.contains_key(column))
            {
                return None;
            }
            let value_sets = primary_key_columns
                .iter()
                .map(|column| constraints.get(column).cloned())
                .collect::<Option<Vec<_>>>()?;
            let keys = cartesian_primary_keys(&value_sets)?;
            if keys.is_empty() {
                return None;
            }
            BoundPrimaryKeyReadTarget::Entity {
                schema_key: schema_key.clone(),
                primary_key_columns,
                keys,
            }
        }
        _ => return None,
    };

    Some(BoundPrimaryKeyRead { target, projection })
}

fn plain_select(query: &Query) -> Option<&Select> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.flavor != SelectFlavor::Standard
        || select.optimizer_hint.is_some()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.top_before_distinct
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !plain_group_by(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.window_before_qualify
        || select.from.len() != 1
        || select.projection.is_empty()
    {
        return None;
    }
    Some(select)
}

fn plain_group_by(group_by: &GroupByExpr) -> bool {
    matches!(group_by, GroupByExpr::Expressions(expressions, modifiers) if expressions.is_empty() && modifiers.is_empty())
}

fn bind_single_table<'a>(
    select: &Select,
    catalog: &'a PublicCatalog,
) -> Option<(&'a PublicSurfaceContract, String)> {
    let from = select.from.first()?;
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &from.relation
    else {
        return None;
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return None;
    }
    if alias
        .as_ref()
        .is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }
    if name.0.len() != 1 {
        return None;
    }
    let table_name = normalize_identifier(name.0.first()?.as_ident()?);
    let surface = catalog.surface(&table_name)?;
    if !matches!(
        surface.kind,
        PublicSurfaceKind::File | PublicSurfaceKind::EntityBase { .. }
    ) {
        return None;
    }
    let qualifier = alias
        .as_ref()
        .map(|alias| normalize_identifier(&alias.name))
        .unwrap_or(table_name);
    Some((surface, qualifier))
}

fn bind_projection(
    select: &Select,
    surface: &PublicSurfaceContract,
    qualifier: &str,
) -> Option<Vec<BoundPrimaryKeyProjection>> {
    select
        .projection
        .iter()
        .map(|item| {
            let (expression, alias) = match item {
                SelectItem::UnnamedExpr(expression) => (expression, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
                SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => return None,
            };
            let column_name = bind_column_reference(expression, qualifier)?;
            let column = surface.public_column(&column_name)?;
            Some(BoundPrimaryKeyProjection {
                source_index: column.id,
                source_name: column.name.clone(),
                output_name: alias
                    .map(normalize_identifier)
                    .unwrap_or_else(|| column.name.clone()),
            })
        })
        .collect()
}

fn bind_constraints(
    expression: &Expr,
    surface: &PublicSurfaceContract,
    qualifier: &str,
    params: &[Value],
    parameter_indexes: &mut BTreeSet<usize>,
    constraints: &mut BTreeMap<String, BTreeSet<String>>,
) -> Option<()> {
    match expression {
        Expr::Nested(expression) => bind_constraints(
            expression,
            surface,
            qualifier,
            params,
            parameter_indexes,
            constraints,
        ),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            bind_constraints(
                left,
                surface,
                qualifier,
                params,
                parameter_indexes,
                constraints,
            )?;
            bind_constraints(
                right,
                surface,
                qualifier,
                params,
                parameter_indexes,
                constraints,
            )
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (column_expression, value_expression) = if is_column_reference(left, qualifier)
                && !is_column_reference(right, qualifier)
            {
                (left.as_ref(), right.as_ref())
            } else if is_column_reference(right, qualifier) && !is_column_reference(left, qualifier)
            {
                (right.as_ref(), left.as_ref())
            } else {
                return None;
            };
            let column_name = bind_column_reference(column_expression, qualifier)?;
            surface.public_column(&column_name)?;
            let value = bind_text_value(value_expression, params, parameter_indexes)?;
            intersect_constraint(constraints, column_name, BTreeSet::from([value]))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column_name = bind_column_reference(expr, qualifier)?;
            surface.public_column(&column_name)?;
            if list.is_empty() {
                return None;
            }
            let values = list
                .iter()
                .map(|value| bind_text_value(value, params, parameter_indexes))
                .collect::<Option<BTreeSet<_>>>()?;
            intersect_constraint(constraints, column_name, values)
        }
        _ => None,
    }
}

fn intersect_constraint(
    constraints: &mut BTreeMap<String, BTreeSet<String>>,
    column_name: String,
    values: BTreeSet<String>,
) -> Option<()> {
    if values.is_empty() {
        return None;
    }
    match constraints.entry(column_name) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(values);
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            entry.get_mut().retain(|value| values.contains(value));
            if entry.get().is_empty() {
                return None;
            }
        }
    }
    Some(())
}

fn is_column_reference(expression: &Expr, qualifier: &str) -> bool {
    bind_column_reference(expression, qualifier).is_some()
}

fn bind_column_reference(expression: &Expr, qualifier: &str) -> Option<String> {
    match expression {
        Expr::Identifier(identifier) => Some(normalize_identifier(identifier)),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let reference_qualifier = normalize_identifier(parts.first()?);
            if reference_qualifier != qualifier {
                return None;
            }
            Some(normalize_identifier(parts.get(1)?))
        }
        _ => None,
    }
}

fn bind_text_value(
    expression: &Expr,
    params: &[Value],
    parameter_indexes: &mut BTreeSet<usize>,
) -> Option<String> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        SqlValue::SingleQuotedString(value) => Some(value.clone()),
        SqlValue::Placeholder(name) => {
            let index = name
                .strip_prefix('$')
                .and_then(|raw| raw.parse::<usize>().ok())
                .filter(|index| *index > 0)?;
            parameter_indexes.insert(index);
            match params.get(index - 1)? {
                Value::Text(value) => Some(value.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn cartesian_primary_keys(value_sets: &[BTreeSet<String>]) -> Option<Vec<Vec<String>>> {
    let key_count = value_sets.iter().try_fold(1usize, |count, values| {
        if values.is_empty() {
            return None;
        }
        count.checked_mul(values.len())
    })?;
    if key_count == 0 || key_count > MAX_COMPOSITE_PRIMARY_KEYS {
        return None;
    }

    let mut keys = vec![Vec::with_capacity(value_sets.len())];
    for values in value_sets {
        let mut next = Vec::with_capacity(keys.len() * values.len());
        for key in &keys {
            for value in values {
                let mut expanded = key.clone();
                expanded.push(value.clone());
                next.push(expanded);
            }
        }
        keys = next;
    }
    Some(keys)
}

fn normalize_identifier(identifier: &Ident) -> String {
    if identifier.quote_style.is_some() {
        identifier.value.clone()
    } else {
        identifier.value.to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::parser::Statement as DataFusionStatement;
    use serde_json::json;

    use super::*;

    fn catalog() -> PublicCatalog {
        PublicCatalog::from_visible_schemas(&[
            json!({
                "x-lix-key": "message",
                "x-lix-primary-key": ["/workspace_id", "/id"],
                "properties": {
                    "workspace_id": { "type": "string" },
                    "id": { "type": "string" },
                    "payload": { "type": "object" }
                }
            }),
            json!({
                "x-lix-key": "nested_key",
                "x-lix-primary-key": ["/identity/id"],
                "properties": {
                    "identity": { "type": "object" }
                }
            }),
            json!({
                "x-lix-key": "integer_key",
                "x-lix-primary-key": ["/id"],
                "properties": {
                    "id": { "type": "integer" }
                }
            }),
        ])
        .expect("catalog builds")
    }

    fn parse(sql: &str) -> DataFusionStatement {
        crate::sql2::parse_statement(sql).expect("SQL parses")
    }

    fn bind(sql: &str, params: &[Value]) -> PrimaryKeyReadBinding {
        bind_primary_key_read(&parse(sql), &catalog(), params)
    }

    fn ready(sql: &str, params: &[Value]) -> BoundPrimaryKeyRead {
        let PrimaryKeyReadBinding::Ready(read) = bind(sql, params) else {
            panic!("expected native binding for {sql}");
        };
        read
    }

    #[test]
    fn binds_file_ids_with_intersection_deduplication_reversed_equality_and_aliases() {
        assert!(matches!(
            bind(
                "SELECT f.data FROM lix_file AS f WHERE 'b' = f.id OR f.id = 'ignored'",
                &[]
            ),
            PrimaryKeyReadBinding::Declined
        ));

        let read = ready(
            "SELECT f.data AS payload, f.id, f.id AS duplicate_id \
             FROM lix_file AS f \
             WHERE 'b' = f.id AND f.id IN ('a', 'b', 'b')",
            &[],
        );
        assert_eq!(
            read.target,
            BoundPrimaryKeyReadTarget::File {
                ids: vec!["b".to_string()]
            }
        );
        assert_eq!(
            read.projection,
            vec![
                BoundPrimaryKeyProjection {
                    source_index: 4,
                    source_name: "data".to_string(),
                    output_name: "payload".to_string(),
                },
                BoundPrimaryKeyProjection {
                    source_index: 0,
                    source_name: "id".to_string(),
                    output_name: "id".to_string(),
                },
                BoundPrimaryKeyProjection {
                    source_index: 0,
                    source_name: "id".to_string(),
                    output_name: "duplicate_id".to_string(),
                },
            ]
        );
    }

    #[test]
    fn binds_parameters_only_when_count_and_referenced_types_match_datafusion() {
        let read = ready(
            "SELECT id FROM lix_file WHERE id IN ($2, $2)",
            &[Value::Integer(7), Value::Text("file-a".to_string())],
        );
        assert_eq!(
            read.target,
            BoundPrimaryKeyReadTarget::File {
                ids: vec!["file-a".to_string()]
            }
        );

        for (sql, params) in [
            (
                "SELECT id FROM lix_file WHERE id = $1",
                vec![Value::Integer(1)],
            ),
            (
                "SELECT id FROM lix_file WHERE id = $1",
                vec![
                    Value::Text("a".to_string()),
                    Value::Text("extra".to_string()),
                ],
            ),
            (
                "SELECT id FROM lix_file WHERE id = $2",
                vec![Value::Text("only-one".to_string())],
            ),
        ] {
            assert!(matches!(
                bind(sql, &params),
                PrimaryKeyReadBinding::Declined
            ));
        }
    }

    #[test]
    fn binds_complete_flat_string_composite_entity_keys_with_bounded_product() {
        let read = ready(
            "SELECT payload AS body, id, payload AS body_again FROM message \
             WHERE workspace_id IN ('w2', 'w1') AND id IN ('b', 'a', 'a')",
            &[],
        );
        assert_eq!(
            read.target,
            BoundPrimaryKeyReadTarget::Entity {
                schema_key: "message".to_string(),
                primary_key_columns: vec!["workspace_id".to_string(), "id".to_string()],
                keys: vec![
                    vec!["w1".to_string(), "a".to_string()],
                    vec!["w1".to_string(), "b".to_string()],
                    vec!["w2".to_string(), "a".to_string()],
                    vec!["w2".to_string(), "b".to_string()],
                ],
            }
        );
        assert_eq!(read.projection[0].source_name, "payload");
        assert_eq!(read.projection[0].output_name, "body");
        assert_eq!(read.projection[0].source_index, 1);
        assert_eq!(read.projection[2].source_index, 1);
    }

    #[test]
    fn declines_partial_nonflat_nonstring_contradictory_and_excessive_entity_keys() {
        for sql in [
            "SELECT id FROM message WHERE id = 'a'",
            "SELECT identity FROM nested_key WHERE identity = 'a'",
            "SELECT id FROM integer_key WHERE id = '1'",
            "SELECT id FROM message WHERE workspace_id = 'w' AND id = 'a' AND id = 'b'",
        ] {
            assert!(
                matches!(bind(sql, &[]), PrimaryKeyReadBinding::Declined),
                "{sql}"
            );
        }

        let workspaces = (0..65)
            .map(|index| format!("'w{index}'"))
            .collect::<Vec<_>>()
            .join(",");
        let ids = (0..64)
            .map(|index| format!("'i{index}'"))
            .collect::<Vec<_>>()
            .join(",");
        let at_limit =
            format!("SELECT id FROM message WHERE workspace_id IN ({ids}) AND id IN ({ids})");
        assert!(matches!(
            bind(&at_limit, &[]),
            PrimaryKeyReadBinding::Ready(_)
        ));
        let above_limit = format!(
            "SELECT id FROM message WHERE workspace_id IN ({workspaces}) AND id IN ({ids})"
        );
        assert!(matches!(
            bind(&above_limit, &[]),
            PrimaryKeyReadBinding::Declined
        ));
    }

    #[test]
    fn declines_every_complex_or_error_sensitive_select_shape() {
        for sql in [
            "SELECT * FROM lix_file WHERE id = 'a'",
            "SELECT upper(id) FROM lix_file WHERE id = 'a'",
            "SELECT missing FROM lix_file WHERE id = 'a'",
            "SELECT id FROM lix_file WHERE missing = 'a'",
            "SELECT id FROM lix_file WHERE id = 'a' OR id = 'b'",
            "SELECT id FROM lix_file WHERE id NOT IN ('a')",
            "SELECT id FROM lix_file WHERE id = 'a' ORDER BY id",
            "SELECT id FROM lix_file WHERE id = 'a' LIMIT 1",
            "SELECT DISTINCT id FROM lix_file WHERE id = 'a'",
            "SELECT id FROM lix_file JOIN lix_file AS other ON true WHERE lix_file.id = 'a'",
            "WITH files AS (SELECT * FROM lix_file) SELECT id FROM files WHERE id = 'a'",
            "SELECT id FROM lix_file_by_branch WHERE id = 'a'",
            "SELECT id FROM lix_file_history WHERE id = 'a'",
            "SELECT id FROM lix_file AS f WHERE lix_file.id = 'a'",
            "SELECT f.id FROM lix_file AS f (renamed) WHERE f.id = 'a'",
            "SELECT id FROM lix_file WHERE id = 1",
        ] {
            assert!(
                matches!(bind(sql, &[]), PrimaryKeyReadBinding::Declined),
                "{sql}"
            );
        }
    }

    #[test]
    fn quoted_projection_alias_preserves_exact_spelling() {
        let read = ready(
            "SELECT payload AS \"JSON Payload\" FROM message \
             WHERE workspace_id = 'w' AND id = 'i'",
            &[],
        );
        assert_eq!(read.projection[0].source_name, "payload");
        assert_eq!(read.projection[0].output_name, "JSON Payload");
    }
}
