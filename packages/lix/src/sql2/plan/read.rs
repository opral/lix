//! Shared finite identity planning for native SQL reads and DataFusion scans.
//!
//! Adapters only recognize syntax and literals. This module owns conjunction,
//! disjunction, completeness, and typed primary-key construction. A candidate
//! constraint may omit unsupported conjuncts; a complete native read may not.
pub(crate) mod statement;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr as DataFusionExpr, Operator};
use std::collections::{BTreeMap, BTreeSet};

use crate::row_pk::{RowPk, RowPkComponentType};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum IdentityValue {
    Text(String),
    Integer(i64),
    Null,
}

pub(crate) enum IdentityNode<'a, E> {
    And(&'a E, &'a E),
    Or(&'a E, &'a E),
    Values(String, BTreeSet<IdentityValue>),
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentityConstraint {
    Union(Vec<Self>),
    Full(BTreeSet<Vec<IdentityValue>>),
    Parts(BTreeMap<String, BTreeSet<IdentityValue>>),
}

impl IdentityConstraint {
    fn is_complete(&self, columns: &[&str]) -> bool {
        match self {
            Self::Full(_) => true,
            Self::Parts(parts) => columns.iter().all(|column| parts.contains_key(*column)),
            Self::Union(items) => items.iter().all(|item| item.is_complete(columns)),
        }
    }

    fn alternatives(&self) -> usize {
        match self {
            Self::Union(items) => items.iter().map(Self::alternatives).sum(),
            _ => 1,
        }
    }

    pub(crate) fn intersect(self, other: Self, columns: &[&str]) -> Self {
        match (self, other) {
            (Self::Union(left), right) => Self::Union(
                left.into_iter()
                    .map(|left| left.intersect(right.clone(), columns))
                    .collect(),
            ),
            (left, Self::Union(right)) => Self::Union(
                right
                    .into_iter()
                    .map(|right| left.clone().intersect(right, columns))
                    .collect(),
            ),
            (Self::Full(left), Self::Full(right)) => {
                Self::Full(left.intersection(&right).cloned().collect())
            }
            (Self::Full(rows), Self::Parts(parts)) | (Self::Parts(parts), Self::Full(rows)) => {
                Self::Full(
                    rows.into_iter()
                        .filter(|row| {
                            columns.iter().zip(row).all(|(column, value)| {
                                parts
                                    .get(*column)
                                    .is_none_or(|values| values.contains(value))
                            })
                        })
                        .collect(),
                )
            }
            (Self::Parts(mut left), Self::Parts(right)) => {
                for (column, values) in right {
                    left.entry(column)
                        .and_modify(|left| *left = left.intersection(&values).cloned().collect())
                        .or_insert(values);
                }
                Self::Parts(left)
            }
        }
    }

    /// Keep a one-column IN list in its set allocation instead of expanding
    /// every member into a separately allocated row (filesystem batch reads).
    pub(crate) fn into_single_values(self, column: &str) -> Option<BTreeSet<IdentityValue>> {
        match self {
            Self::Parts(mut parts) => parts.remove(column),
            Self::Full(rows) => rows
                .into_iter()
                .map(|mut row| {
                    if row.len() != 1 {
                        return None;
                    }
                    row.pop()
                })
                .collect(),
            Self::Union(items) => {
                let mut values = BTreeSet::new();
                for item in items {
                    values.extend(item.into_single_values(column)?);
                }
                Some(values)
            }
        }
    }

    pub(crate) fn into_rows(
        self,
        columns: &[&str],
        limit: usize,
    ) -> Option<BTreeSet<Vec<IdentityValue>>> {
        match self {
            Self::Union(constraints) => {
                let mut rows = BTreeSet::new();
                for constraint in constraints {
                    rows.extend(constraint.into_rows(columns, limit)?);
                    if rows.len() > limit {
                        return None;
                    }
                }
                Some(rows)
            }
            Self::Full(rows) => (rows.len() <= limit).then_some(rows),
            Self::Parts(parts) => {
                if columns.iter().any(|column| !parts.contains_key(*column)) {
                    return None;
                }
                let mut rows = BTreeSet::from([Vec::new()]);
                for column in columns {
                    let values = parts.get(*column)?;
                    if rows.len().checked_mul(values.len())? > limit {
                        return None;
                    }
                    rows = rows
                        .into_iter()
                        .flat_map(|prefix| {
                            values.iter().map(move |value| {
                                let mut row = prefix.clone();
                                row.push(value.clone());
                                row
                            })
                        })
                        .collect();
                }
                Some(rows)
            }
        }
    }
}

/// `allow_residual` permits extracting a necessary conjunct, never one arm of
/// a disjunction. Callers using it must continue evaluating the full filter.
pub(crate) fn bind_identity<E>(
    expression: &E,
    columns: &[&str],
    allow_residual: bool,
    limit: usize,
    node: &impl for<'a> Fn(&'a E) -> IdentityNode<'a, E>,
) -> Option<IdentityConstraint> {
    match node(expression) {
        IdentityNode::And(left, right) => {
            let left = bind_identity(left, columns, allow_residual, limit, node);
            let right = bind_identity(right, columns, allow_residual, limit, node);
            match (left, right) {
                (Some(left), Some(right)) => {
                    if left.alternatives().checked_mul(right.alternatives())? > limit {
                        return None;
                    }
                    Some(left.intersect(right, columns))
                }
                (Some(constraint), None) | (None, Some(constraint)) if allow_residual => {
                    Some(constraint)
                }
                _ => None,
            }
        }
        IdentityNode::Or(left, right) => {
            let left = bind_identity(left, columns, false, limit, node)?;
            let right = bind_identity(right, columns, false, limit, node)?;
            if left.is_complete(columns) && right.is_complete(columns) {
                let mut rows = left.into_rows(columns, limit)?;
                rows.extend(right.into_rows(columns, limit)?);
                return (rows.len() <= limit).then_some(IdentityConstraint::Full(rows));
            }
            let mut constraints = match left {
                IdentityConstraint::Union(items) => items,
                other => vec![other],
            };
            match right {
                IdentityConstraint::Union(items) => constraints.extend(items),
                other => constraints.push(other),
            };
            (constraints.len() <= limit).then_some(IdentityConstraint::Union(constraints))
        }
        IdentityNode::Values(column, values) if columns.contains(&column.as_str()) => Some(
            IdentityConstraint::Parts(BTreeMap::from([(column, values)])),
        ),
        _ => None,
    }
}

pub(crate) fn row_pk(
    values: &[IdentityValue],
    component_types: &[RowPkComponentType],
) -> Option<RowPk> {
    if values.len() != component_types.len() {
        return None;
    }
    let parts = values
        .iter()
        .zip(component_types)
        .map(|(value, component_type)| match (value, component_type) {
            (
                IdentityValue::Text(value),
                RowPkComponentType::String | RowPkComponentType::Uuid | RowPkComponentType::Bytes,
            ) => Some(value.clone()),
            (IdentityValue::Integer(value), RowPkComponentType::Integer) => Some(value.to_string()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    RowPk::from_external_parts(parts, component_types).ok()
}

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind, SchemaSurfaceSpec};
use crate::{LixError, Value};
use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UnboundSchemaRead {
    table_name: String,
    pub(crate) projected_columns: Vec<String>,
    pub(crate) output_columns: Vec<String>,
    predicate: Expr,
    params: Vec<Value>,
    order_by_columns: Vec<String>,
    has_limit: bool,
}

pub(crate) enum SchemaReadAccess {
    Point(RowPk),
    Batch(Vec<(RowPk, Option<String>)>),
}

pub(crate) struct BoundSchemaRead {
    pub(crate) spec: SchemaSurfaceSpec,
    pub(crate) access: SchemaReadAccess,
}

impl UnboundSchemaRead {
    pub(crate) fn new(
        table_name: String,
        projected_columns: Vec<String>,
        output_columns: Vec<String>,
        predicate: Expr,
        params: Vec<Value>,
        order_by_columns: Vec<String>,
        has_limit: bool,
    ) -> Option<Self> {
        fn supported(expr: &Expr, params: &[Value]) -> bool {
            match sql_identity_node(expr, params) {
                IdentityNode::And(left, right) | IdentityNode::Or(left, right) => {
                    supported(left, params) && supported(right, params)
                }
                IdentityNode::Values(..) => true,
                IdentityNode::Unsupported => false,
            }
        }
        supported(&predicate, &params).then_some(Self {
            table_name,
            projected_columns,
            output_columns,
            predicate,
            params,
            order_by_columns,
            has_limit,
        })
    }

    pub(crate) fn bind(
        &self,
        catalog: &PublicCatalog,
    ) -> Result<Option<BoundSchemaRead>, LixError> {
        let Some(surface) = catalog.surface(&self.table_name) else {
            return Ok(None);
        };
        let PublicSurfaceKind::SchemaBase { schema_key } = &surface.kind else {
            return Ok(None);
        };
        let spec = catalog.schema_spec(schema_key).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "native schema read is missing schema metadata",
            )
        })?;
        if self
            .projected_columns
            .iter()
            .any(|column| spec.visible_column(column).is_none())
        {
            return Ok(None);
        }
        let Some(mut columns) = spec
            .primary_key_paths
            .iter()
            .map(|path| match path.as_slice() {
                [column] => Some(column.as_str()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        if columns.is_empty() {
            return Ok(None);
        }
        if self.order_by_columns.is_empty() {
            if let Some(rows) = bind_identity(&self.predicate, &columns, false, 4096, &|expr| {
                normalize_node(
                    sql_identity_node(expr, &self.params),
                    &columns,
                    &spec.primary_key_component_types,
                )
            })
            .and_then(|constraint| constraint.into_rows(&columns, 4096))
            {
                if rows.len() == 1 {
                    if let Some(key) =
                        row_pk(rows.first().unwrap(), &spec.primary_key_component_types)
                    {
                        return Ok(Some(BoundSchemaRead {
                            spec: spec.clone(),
                            access: SchemaReadAccess::Point(key),
                        }));
                    }
                }
            }
        }
        if self.has_limit
            || (!self.order_by_columns.is_empty()
                && self
                    .order_by_columns
                    .iter()
                    .map(String::as_str)
                    .ne(columns.iter().copied()))
        {
            return Ok(None);
        }
        columns.push("lixcol_file_id");
        let Some(rows) = bind_identity(&self.predicate, &columns, false, 4096, &|expr| {
            normalize_node(
                sql_identity_node(expr, &self.params),
                &columns,
                &spec.primary_key_component_types,
            )
        })
        .and_then(|constraint| constraint.into_rows(&columns, 4096)) else {
            return Ok(None);
        };
        let identities = rows
            .into_iter()
            .map(|mut values| {
                let file_id = match values.pop()? {
                    IdentityValue::Null => None,
                    IdentityValue::Text(value) => Some(value),
                    _ => return None,
                };
                Some((row_pk(&values, &spec.primary_key_component_types)?, file_id))
            })
            .collect::<Option<BTreeSet<_>>>();
        Ok(identities.map(|identities| BoundSchemaRead {
            spec: spec.clone(),
            access: SchemaReadAccess::Batch(identities.into_iter().collect()),
        }))
    }
}

/// Validate identity literals before set operations. Noncanonical UUID
/// spellings retain full SQL evaluation, matching the durable key boundary.
/// Metadata file scope is the optional final column and accepts SQL IS NULL.
pub(crate) fn normalize_node<'a, E>(
    node: IdentityNode<'a, E>,
    columns: &[&str],
    types: &[RowPkComponentType],
) -> IdentityNode<'a, E> {
    let IdentityNode::Values(column, values) = node else {
        return node;
    };
    let Some(index) = columns.iter().position(|name| *name == column) else {
        return IdentityNode::Unsupported;
    };
    let normalized = values
        .into_iter()
        .map(|value| {
            let Some(component_type) = types.get(index) else {
                return matches!(value, IdentityValue::Text(_) | IdentityValue::Null)
                    .then_some(value);
            };
            match (component_type, value) {
                (RowPkComponentType::String, value @ IdentityValue::Text(_))
                | (RowPkComponentType::Integer, value @ IdentityValue::Integer(_)) => Some(value),
                (
                    RowPkComponentType::Uuid | RowPkComponentType::Bytes,
                    value @ IdentityValue::Text(_),
                ) => {
                    let key = row_pk(
                        std::slice::from_ref(&value),
                        std::slice::from_ref(component_type),
                    )?;
                    Some(IdentityValue::Text(key.components[0].external_string()))
                }
                _ => None,
            }
        })
        .collect::<Option<BTreeSet<_>>>();
    normalized
        .map(|values| IdentityNode::Values(column, values))
        .unwrap_or(IdentityNode::Unsupported)
}

fn sql_identity_node<'a>(mut expr: &'a Expr, params: &[Value]) -> IdentityNode<'a, Expr> {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    let column = |expr: &Expr| match expr {
        Expr::Identifier(identifier) if identifier.quote_style.is_none() => {
            Some(identifier.value.to_ascii_lowercase())
        }
        _ => None,
    };
    let literal = |expr: &Expr| -> Option<IdentityValue> {
        let Expr::Value(value) = expr else {
            return None;
        };
        match &value.value {
            SqlValue::SingleQuotedString(value) => Some(IdentityValue::Text(value.clone())),
            SqlValue::Number(value, _) => value.parse().ok().map(IdentityValue::Integer),
            SqlValue::Placeholder(value) => match params.get(
                value
                    .strip_prefix('$')?
                    .parse::<usize>()
                    .ok()?
                    .checked_sub(1)?,
            )? {
                Value::Text(value) => Some(IdentityValue::Text(value.clone())),
                Value::Integer(value) => Some(IdentityValue::Integer(*value)),
                // `column = NULL` is UNKNOWN, never an IS NULL identity.
                _ => None,
            },
            _ => None,
        }
    };
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => IdentityNode::And(left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => IdentityNode::Or(left, right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => column(left)
            .zip(literal(right))
            .or_else(|| column(right).zip(literal(left)))
            .map(|(column, value)| IdentityNode::Values(column, BTreeSet::from([value])))
            .unwrap_or(IdentityNode::Unsupported),
        Expr::InList {
            expr,
            list,
            negated: false,
        } if !list.is_empty() => column(expr)
            .zip(list.iter().map(literal).collect::<Option<BTreeSet<_>>>())
            .map(|(column, values)| IdentityNode::Values(column, values))
            .unwrap_or(IdentityNode::Unsupported),
        Expr::IsNull(expr) => column(expr)
            .map(|column| IdentityNode::Values(column, BTreeSet::from([IdentityValue::Null])))
            .unwrap_or(IdentityNode::Unsupported),
        _ => IdentityNode::Unsupported,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identities(
        sql: &str,
        columns: &[&str],
        types: &[RowPkComponentType],
        residual: bool,
        limit: usize,
    ) -> Option<BTreeSet<Vec<IdentityValue>>> {
        let statement =
            crate::sql2::parse_statement(&format!("SELECT id FROM probe WHERE {sql}")).unwrap();
        let datafusion::sql::parser::Statement::Statement(statement) = statement else {
            panic!("SQL statement")
        };
        let datafusion::sql::sqlparser::ast::Statement::Query(query) = *statement else {
            panic!("query")
        };
        let datafusion::sql::sqlparser::ast::SetExpr::Select(select) = *query.body else {
            panic!("select")
        };
        bind_identity(
            select.selection.as_ref().unwrap(),
            columns,
            residual,
            limit,
            &|expr| normalize_node(sql_identity_node(expr, &[]), columns, types),
        )?
        .into_rows(columns, limit)
    }

    #[test]
    fn native_and_candidate_identity_binding_share_completeness_and_residual_rules() {
        let columns = ["tenant", "revision"];
        let types = [RowPkComponentType::String, RowPkComponentType::Integer];
        let complete = "tenant = 'docs' AND revision IN (7, 9, 7)";
        let rows = identities(complete, &columns, &types, false, 4096).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            Some(rows.clone()),
            identities(complete, &columns, &types, true, 4096)
        );
        assert!(identities("tenant = 'docs'", &columns, &types, false, 4096).is_none());
        let residual = format!("({complete}) AND payload = 'wanted'");
        assert!(identities(&residual, &columns, &types, false, 4096).is_none());
        assert_eq!(
            Some(rows),
            identities(&residual, &columns, &types, true, 4096)
        );
        assert!(
            identities(
                "tenant = 'docs' OR revision = 7",
                &columns,
                &types,
                false,
                4096
            )
            .is_none(),
            "a disjunction of partial keys is not a complete identity"
        );
        let disjunction = format!("({complete}) OR payload = 'wanted'");
        assert!(identities(&disjunction, &columns, &types, true, 4096).is_none());
    }

    #[test]
    fn sparse_disjunctions_preserve_correlated_file_scopes_and_bound_expansion() {
        let columns = ["tenant", "revision", "lixcol_file_id"];
        let types = [RowPkComponentType::String, RowPkComponentType::Integer];
        let rows = identities(
            "(tenant = 'a' OR tenant = 'b') AND revision IN (1, 2) AND lixcol_file_id IS NULL",
            &columns,
            &types,
            false,
            4096,
        )
        .unwrap();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row[2] == IdentityValue::Null));
        assert!(
            identities(
                "tenant IN ('a', 'b') AND revision IN (1, 2) AND lixcol_file_id IS NULL",
                &columns,
                &types,
                false,
                3
            )
            .is_none()
        );
        let rows = identities("(tenant = 'a' AND revision = 1 AND lixcol_file_id = 'file-a') OR (tenant = 'b' AND revision = 2 AND lixcol_file_id IS NULL)", &columns, &types, false, 4096).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![
            IdentityValue::Text("a".into()),
            IdentityValue::Integer(1),
            IdentityValue::Text("file-a".into())
        ]));
    }

    #[test]
    fn identities_validate_uuid_before_intersection_and_keep_null_unknown() {
        let columns = ["id"];
        let types = [RowPkComponentType::Uuid];
        let lower = "550e8400-e29b-41d4-a716-446655440000";
        let upper = lower.to_ascii_uppercase();
        assert!(
            identities(&format!("id = '{upper}'"), &columns, &types, false, 4096).is_none(),
            "noncanonical UUID spellings must retain DataFusion evaluation"
        );
        let sql = format!(
            "(id = '{lower}' OR id = '550e8400-e29b-41d4-a716-446655440001') AND id = '{lower}'"
        );
        let rows = identities(&sql, &columns, &types, false, 4096).unwrap();
        assert_eq!(
            rows,
            BTreeSet::from([vec![IdentityValue::Text(lower.into())]])
        );
        assert!(identities("id = NULL", &columns, &types, false, 4096).is_none());
        assert!(identities("id IS NULL", &columns, &types, false, 4096).is_none());
        assert!(identities("id IN ('bad-uuid')", &columns, &types, false, 4096).is_none());
        assert!(
            identities(
                "id = '1'",
                &columns,
                &[RowPkComponentType::Integer],
                false,
                4096
            )
            .is_none()
        );
        assert!(
            identities(
                "id = 1 AND id = 2",
                &columns,
                &[RowPkComponentType::Integer],
                false,
                4096
            )
            .unwrap()
            .is_empty()
        );
    }
}

/// Executes only plans whose entire delivered result is proved by the SQL
/// planner. Ambiguous schema point reads return None to retain full SQL.
pub(crate) async fn execute_native_read<C: crate::sql2::SqlExecutionContext>(
    ctx: &C,
    plan: &statement::StatementReadPlan,
) -> Result<Option<(crate::SqlQueryResult, usize)>, LixError> {
    use crate::sql2;
    use statement::{ExactFilesystemRead as Filesystem, NativeReadPlan};
    let Some(native) = &plan.native else {
        return Ok(None);
    };
    let branch = ctx.active_branch_id();
    let query = match native {
        NativeReadPlan::Filesystem(filesystem) => match filesystem {
            Filesystem::RootFileListing => {
                sql2::execute_exact_lix_file_root_listing(
                    branch,
                    ctx.filesystem_path_index(),
                    ctx.branch_ref(),
                )
                .await?
            }
            Filesystem::RootDirectoryListing => {
                sql2::execute_exact_lix_directory_root_listing(
                    branch,
                    ctx.filesystem_path_index(),
                    ctx.branch_ref(),
                )
                .await?
            }
            Filesystem::Point(selector, column) => {
                sql2::execute_exact_lix_file_read(
                    branch,
                    ctx.hot_state(),
                    ctx.filesystem_path_index(),
                    ctx.branch_ref(),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.session_file_views(),
                    selector,
                    *column,
                )
                .await?
            }
            Filesystem::PathContentBatch(paths) => {
                sql2::execute_exact_lix_file_batch_read(
                    branch,
                    ctx.hot_state(),
                    ctx.filesystem_path_index(),
                    ctx.branch_ref(),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.session_file_views(),
                    None,
                    paths,
                    None,
                )
                .await?
            }
            Filesystem::IdManifestBatch(ids) => {
                sql2::execute_exact_lix_file_id_manifest_batch_read(
                    branch,
                    ctx.hot_state(),
                    ctx.filesystem_path_index(),
                    ctx.branch_ref(),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.session_file_views(),
                    ids,
                )
                .await?
            }
        },
        NativeReadPlan::Schema(schema) => {
            let Some(bound) = schema.bind(ctx.public_catalog().await?.as_ref())? else {
                return Ok(None);
            };
            return match bound.access {
                SchemaReadAccess::Point(key) => Ok(sql2::execute_exact_schema_point_read(
                    &bound.spec,
                    branch,
                    ctx.hot_state(),
                    key,
                    &schema.projected_columns,
                    schema.output_columns.clone(),
                )
                .await?
                .map(|query| (query, 1))),
                SchemaReadAccess::Batch(identities) => {
                    let examined = identities.len();
                    Ok(Some((
                        sql2::execute_exact_schema_batch_read(
                            &bound.spec,
                            branch,
                            ctx.hot_state(),
                            identities,
                            &schema.projected_columns,
                            schema.output_columns.clone(),
                        )
                        .await?,
                        examined,
                    )))
                }
            };
        }
    };
    Ok(Some((query, 0)))
}

pub(crate) fn datafusion_identity_node(expr: &DataFusionExpr) -> IdentityNode<'_, DataFusionExpr> {
    match expr {
        DataFusionExpr::BinaryExpr(binary) if binary.op == Operator::And => {
            IdentityNode::And(&binary.left, &binary.right)
        }
        DataFusionExpr::BinaryExpr(binary) if binary.op == Operator::Or => {
            IdentityNode::Or(&binary.left, &binary.right)
        }
        DataFusionExpr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let pair = match (binary.left.as_ref(), binary.right.as_ref()) {
                (DataFusionExpr::Column(column), value)
                | (value, DataFusionExpr::Column(column)) => Some((column.name.clone(), value)),
                _ => None,
            };
            pair.and_then(|(column, value)| {
                Some(IdentityNode::Values(
                    column,
                    BTreeSet::from([datafusion_identity_value(value)?]),
                ))
            })
            .unwrap_or(IdentityNode::Unsupported)
        }
        DataFusionExpr::InList(list) if !list.negated && !list.list.is_empty() => {
            let DataFusionExpr::Column(column) = list.expr.as_ref() else {
                return IdentityNode::Unsupported;
            };
            list.list
                .iter()
                .map(datafusion_identity_value)
                .collect::<Option<BTreeSet<_>>>()
                .map(|values| IdentityNode::Values(column.name.clone(), values))
                .unwrap_or(IdentityNode::Unsupported)
        }
        _ => IdentityNode::Unsupported,
    }
}

fn datafusion_identity_value(expr: &DataFusionExpr) -> Option<IdentityValue> {
    let DataFusionExpr::Literal(literal, _) = expr else {
        return None;
    };
    Some(match literal {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => IdentityValue::Text(value.clone()),
        ScalarValue::Int8(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => IdentityValue::Integer(*value),
        ScalarValue::UInt8(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => IdentityValue::Integer(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => IdentityValue::Integer(i64::try_from(*value).ok()?),
        _ => return None,
    })
}
