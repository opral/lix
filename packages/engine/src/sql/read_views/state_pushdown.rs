use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Select,
    SelectItem, Value as AstValue, Visit, Visitor,
};

#[derive(Default)]
pub(crate) struct StatePushdown {
    pub(crate) source_predicates: Vec<String>,
    pub(crate) ranked_predicates: Vec<RankedPushdownPredicate>,
}

#[derive(Clone, Debug)]
pub(crate) struct RankedPushdownPredicate {
    pub(crate) ranked_sql: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateColumn {
    EntityId,
    SchemaKey,
    FileId,
    VersionId,
    PluginKey,
}

impl StateColumn {
    fn canonical_name(self) -> &'static str {
        match self {
            Self::EntityId => "entity_id",
            Self::SchemaKey => "schema_key",
            Self::FileId => "file_id",
            Self::VersionId => "version_id",
            Self::PluginKey => "plugin_key",
        }
    }

    fn from_identifier(raw: &str) -> Option<Self> {
        if raw.eq_ignore_ascii_case("entity_id") || raw.eq_ignore_ascii_case("lixcol_entity_id") {
            return Some(Self::EntityId);
        }
        if raw.eq_ignore_ascii_case("schema_key") || raw.eq_ignore_ascii_case("lixcol_schema_key") {
            return Some(Self::SchemaKey);
        }
        if raw.eq_ignore_ascii_case("file_id") || raw.eq_ignore_ascii_case("lixcol_file_id") {
            return Some(Self::FileId);
        }
        if raw.eq_ignore_ascii_case("version_id") || raw.eq_ignore_ascii_case("lixcol_version_id") {
            return Some(Self::VersionId);
        }
        if raw.eq_ignore_ascii_case("plugin_key") || raw.eq_ignore_ascii_case("lixcol_plugin_key") {
            return Some(Self::PluginKey);
        }
        None
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PushdownBucket {
    Source = 0,
    Ranked = 1,
    Remaining = 2,
}

struct PredicatePart {
    predicate: Expr,
    extracted: Option<ExtractedPushdownPredicate>,
    has_bare_placeholder: bool,
}

struct ExtractedPushdownPredicate {
    bucket: PushdownBucket,
    predicate_sql: String,
}

pub(crate) fn select_projects_count_star(select: &Select) -> bool {
    if select.projection.len() != 1 {
        return false;
    }

    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return false;
    };
    if function.uses_odbc_syntax
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }
    if !function_name_is_count(function) {
        return false;
    }
    if !matches!(function.parameters, FunctionArguments::None) {
        return false;
    }
    let FunctionArguments::List(list) = &function.args else {
        return false;
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() || list.args.len() != 1 {
        return false;
    }
    matches!(
        &list.args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
    )
}

pub(crate) fn select_supports_count_fast_path(select: &Select) -> bool {
    if !select_projects_count_star(select) {
        return false;
    }

    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return false;
    }
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !exprs.is_empty() || !modifiers.is_empty() {
                return false;
            }
        }
        GroupByExpr::All(_) => return false,
    }

    select.from.len() == 1 && select.from[0].joins.is_empty()
}

pub(crate) fn take_pushdown_predicates(
    selection: &mut Option<Expr>,
    relation_name: &str,
    allow_unqualified: bool,
) -> StatePushdown {
    let Some(selection_expr) = selection.take() else {
        return StatePushdown::default();
    };

    let mut parts = Vec::new();
    for predicate in split_conjunction(selection_expr) {
        let extracted = extract_pushdown_predicate(&predicate, relation_name, allow_unqualified);
        let has_bare_placeholder = expr_contains_bare_placeholder(&predicate);
        parts.push(PredicatePart {
            predicate,
            extracted,
            has_bare_placeholder,
        });
    }

    let has_bare_placeholder_reordering = has_bare_placeholder_reordering(&parts);

    let mut pushdown = StatePushdown::default();
    let mut remaining = Vec::new();
    for part in parts {
        match part.extracted {
            Some(extracted) if !(part.has_bare_placeholder && has_bare_placeholder_reordering) => {
                match extracted.bucket {
                    PushdownBucket::Source => pushdown
                        .source_predicates
                        .push(format!("s.{}", extracted.predicate_sql)),
                    PushdownBucket::Ranked => {
                        pushdown.ranked_predicates.push(RankedPushdownPredicate {
                            ranked_sql: format!("ranked.{}", extracted.predicate_sql),
                        })
                    }
                    PushdownBucket::Remaining => remaining.push(part.predicate),
                }
            }
            _ => remaining.push(part.predicate),
        }
    }

    *selection = join_conjunction(remaining);
    pushdown
}

fn has_bare_placeholder_reordering(parts: &[PredicatePart]) -> bool {
    let mut last_bucket = PushdownBucket::Source;
    let mut saw_any = false;
    for part in parts {
        if !part.has_bare_placeholder {
            continue;
        }
        let bucket = part
            .extracted
            .as_ref()
            .map(|extracted| extracted.bucket)
            .unwrap_or(PushdownBucket::Remaining);
        if saw_any && bucket < last_bucket {
            return true;
        }
        last_bucket = bucket;
        saw_any = true;
    }
    false
}

fn split_conjunction(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut out = split_conjunction(*left);
            out.extend(split_conjunction(*right));
            out
        }
        other => vec![other],
    }
}

fn join_conjunction(mut predicates: Vec<Expr>) -> Option<Expr> {
    if predicates.is_empty() {
        return None;
    }
    let mut current = predicates.remove(0);
    for predicate in predicates {
        current = Expr::BinaryOp {
            left: Box::new(current),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        };
    }
    Some(current)
}

fn extract_pushdown_predicate(
    predicate: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<ExtractedPushdownPredicate> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_target_column(left, relation_name, allow_unqualified) {
                return build_binary_pushdown_predicate(column, right);
            }
            if let Some(column) = extract_target_column(right, relation_name, allow_unqualified) {
                return build_binary_pushdown_predicate(column, left);
            }
            None
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            let bucket = pushdown_bucket_for_column(column)?;
            let list_sql = render_in_list_sql(list);
            Some(ExtractedPushdownPredicate {
                bucket,
                predicate_sql: format!("{} IN ({list_sql})", column.canonical_name()),
            })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated: false,
        } => {
            let column = extract_target_column(expr, relation_name, allow_unqualified)?;
            let bucket = pushdown_bucket_for_column(column)?;
            Some(ExtractedPushdownPredicate {
                bucket,
                predicate_sql: format!("{} IN ({subquery})", column.canonical_name()),
            })
        }
        _ => None,
    }
}

fn build_binary_pushdown_predicate(
    column: StateColumn,
    rhs: &Expr,
) -> Option<ExtractedPushdownPredicate> {
    let bucket = pushdown_bucket_for_column(column)?;
    Some(ExtractedPushdownPredicate {
        bucket,
        predicate_sql: format!("{} = {rhs}", column.canonical_name()),
    })
}

fn pushdown_bucket_for_column(column: StateColumn) -> Option<PushdownBucket> {
    match column {
        StateColumn::EntityId | StateColumn::SchemaKey | StateColumn::FileId => {
            Some(PushdownBucket::Source)
        }
        // Keep plugin filtering after winner selection to preserve row-choice semantics.
        StateColumn::VersionId | StateColumn::PluginKey => Some(PushdownBucket::Ranked),
    }
}

fn extract_target_column(
    expr: &Expr,
    relation_name: &str,
    allow_unqualified: bool,
) -> Option<StateColumn> {
    match expr {
        Expr::Identifier(ident) if allow_unqualified => StateColumn::from_identifier(&ident.value),
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[parts.len() - 2].value;
            if !qualifier.eq_ignore_ascii_case(relation_name) {
                return None;
            }
            let column = &parts[parts.len() - 1].value;
            StateColumn::from_identifier(column)
        }
        Expr::Nested(inner) => extract_target_column(inner, relation_name, allow_unqualified),
        _ => None,
    }
}

fn render_in_list_sql(list: &[Expr]) -> String {
    list.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn expr_contains_bare_placeholder(expr: &Expr) -> bool {
    expr_contains_placeholder(expr, true)
}

fn expr_contains_placeholder(expr: &Expr, bare_only: bool) -> bool {
    let mut detector = PlaceholderDetector {
        bare_only,
        found: false,
    };
    let _ = expr.visit(&mut detector);
    detector.found
}

struct PlaceholderDetector {
    bare_only: bool,
    found: bool,
}

impl Visitor for PlaceholderDetector {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        let Expr::Value(value) = expr else {
            return ControlFlow::Continue(());
        };
        let AstValue::Placeholder(token) = &value.value else {
            return ControlFlow::Continue(());
        };
        if !self.bare_only || token == "?" {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn function_name_is_count(function: &sqlparser::ast::Function) -> bool {
    function
        .name
        .0
        .last()
        .and_then(sqlparser::ast::ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("count"))
}
