#[cfg(test)]
use std::borrow::Cow;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::sql2::catalog::PublicCatalog;
use crate::sql2::plan::LogicalWritePlan;
use crate::{LixError, Value};
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider,
};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DataFusionStatement;
use lru::LruCache;
use smallvec::SmallVec;

const PARSED_STATEMENT_CAPACITY: usize = 256;
const PUBLIC_CATALOG_CAPACITY: usize = 16;
const WRITE_PLAN_CAPACITY: usize = 256;
const READ_PLAN_CAPACITY: usize = 256;
const READ_SESSION_CAPACITY: usize = 16;
const READ_PLANNER_CONTRACT: u32 = 1;

#[derive(Clone)]
pub(crate) struct CachedReadPlan {
    pub(crate) plan: LogicalPlan,
    pub(crate) json_predicate_params: std::collections::BTreeSet<usize>,
    pub(crate) expected_parameter_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ParameterType {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Json,
    Timestamp,
    Blob,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReadPlanCacheKey<CatalogKey> {
    sql: Arc<str>,
    parameter_types: SmallVec<[ParameterType; 4]>,
    catalog: CatalogKey,
    planner_contract: u32,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PhysicalReadPlanCacheKey<CatalogKey> {
    sql: Arc<str>,
    parameters: Arc<[u8]>,
    catalog: CatalogKey,
    planner_contract: u32,
}

/// One conservative auto-parameterization of a literal UPDATE statement.
///
/// The normalized SQL is suitable as a planning-cache key while `params`
/// preserves the values from the caller's original statement. Keeping this
/// representation private to SQL execution avoids making automatic
/// parameterization part of the public API contract.
pub(crate) struct AutoParameterizedUpdate {
    pub(crate) sql: Arc<str>,
    pub(crate) statement: DataFusionStatement,
    pub(crate) params: Vec<Value>,
}

/// Bounded, engine-owned cache for snapshot-independent SQL planning templates.
///
/// Parsed statements depend only on the exact SQL text. Bound write plans also
/// depend on the visible catalog and active branch because binding resolves
/// public surfaces and injects branch scope. Cached DataFusion read plans are
/// stripped of snapshot-bound providers and rebound to the current read
/// session before execution.
pub(crate) struct SqlPlanningCache<CatalogKey> {
    datafusion_state: SessionState,
    read_sessions: Mutex<Vec<SessionContext>>,
    parsed_statements: Mutex<LruCache<Arc<str>, Arc<DataFusionStatement>>>,
    public_catalogs: Mutex<LruCache<CatalogKey, Arc<PublicCatalog>>>,
    write_plans: Mutex<LruCache<WritePlanCacheKey<CatalogKey>, Arc<LogicalWritePlan>>>,
    read_plans: Mutex<LruCache<ReadPlanCacheKey<CatalogKey>, Arc<CachedReadPlan>>>,
    physical_read_plans:
        Mutex<LruCache<PhysicalReadPlanCacheKey<CatalogKey>, Arc<dyn ExecutionPlan>>>,
}

impl<CatalogKey> std::fmt::Debug for SqlPlanningCache<CatalogKey> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlPlanningCache")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WritePlanCacheKey<CatalogKey> {
    sql: Arc<str>,
    catalog: CatalogKey,
    active_branch_id: Arc<str>,
}

impl<CatalogKey> Default for SqlPlanningCache<CatalogKey>
where
    CatalogKey: Clone + Eq + Hash,
{
    fn default() -> Self {
        let session = super::session::new_sql_session_context();
        Self::with_state_and_capacities(
            session.state(),
            PARSED_STATEMENT_CAPACITY,
            PUBLIC_CATALOG_CAPACITY,
            WRITE_PLAN_CAPACITY,
            READ_PLAN_CAPACITY,
        )
    }
}

impl<CatalogKey> SqlPlanningCache<CatalogKey>
where
    CatalogKey: Clone + Eq + Hash,
{
    pub(crate) fn datafusion_session(&self) -> SessionContext {
        let catalogs = Arc::new(MemoryCatalogProviderList::new());
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .expect("fresh DataFusion catalog accepts the public schema");
        catalogs.register_catalog("datafusion".to_string(), catalog);
        let state = SessionStateBuilder::new_from_existing(self.datafusion_state.clone())
            .with_catalog_list(catalogs)
            .build();
        SessionContext::new_with_state(state)
    }

    pub(crate) fn datafusion_read_session(&self) -> SessionContext {
        lock_or_recover(&self.read_sessions)
            .pop()
            .unwrap_or_else(|| self.datafusion_session())
    }

    pub(crate) fn recycle_datafusion_read_session(&self, session: SessionContext) {
        if let Some(catalog) = session.catalog("datafusion")
            && let Some(public) = catalog.schema("public")
        {
            for table in public.table_names() {
                let _ = session.deregister_table(datafusion::common::TableReference::full(
                    "datafusion",
                    "public",
                    table,
                ));
            }
        }
        let state = session.state();
        let execution_scalar_functions = state
            .scalar_functions()
            .keys()
            .filter(|name| !self.datafusion_state.scalar_functions().contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        let execution_table_functions = state
            .table_functions()
            .keys()
            .filter(|name| !self.datafusion_state.table_functions().contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        drop(state);
        for name in execution_scalar_functions {
            session.deregister_udf(&name);
        }
        for name in execution_table_functions {
            session.deregister_udtf(&name);
        }

        let mut sessions = lock_or_recover(&self.read_sessions);
        if sessions.len() < READ_SESSION_CAPACITY {
            sessions.push(session);
        }
    }

    pub(crate) fn read_plan(
        &self,
        sql: &str,
        params: &[Value],
        catalog: &CatalogKey,
    ) -> Option<Arc<CachedReadPlan>> {
        let key = ReadPlanCacheKey::new(sql, params, catalog.clone());
        lock_or_recover(&self.read_plans).get(&key).cloned()
    }

    pub(crate) fn remember_read_plan(
        &self,
        sql: &str,
        params: &[Value],
        catalog: CatalogKey,
        plan: CachedReadPlan,
    ) {
        let key = ReadPlanCacheKey::new(sql, params, catalog);
        lock_or_recover(&self.read_plans).put(key, Arc::new(plan));
    }

    pub(crate) fn physical_read_plan(
        &self,
        key: &PhysicalReadPlanCacheKey<CatalogKey>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        lock_or_recover(&self.physical_read_plans).get(key).cloned()
    }

    pub(crate) fn remember_physical_read_plan(
        &self,
        key: PhysicalReadPlanCacheKey<CatalogKey>,
        plan: Arc<dyn ExecutionPlan>,
    ) {
        lock_or_recover(&self.physical_read_plans).put(key, plan);
    }

    pub(crate) fn forget_physical_read_plan(&self, key: &PhysicalReadPlanCacheKey<CatalogKey>) {
        lock_or_recover(&self.physical_read_plans).pop(key);
    }

    #[cfg(test)]
    pub(crate) fn read_plan_count(&self) -> usize {
        lock_or_recover(&self.read_plans).len()
    }

    /// Parses exact SQL once and returns an owned clone of the cached AST.
    ///
    /// Parse failures are intentionally not cached. Two concurrent cold calls
    /// may do duplicate parsing, but parsing happens outside the mutex and the
    /// second insertion reuses the first successful template.
    pub(crate) fn parse_statement(&self, sql: &str) -> Result<DataFusionStatement, LixError> {
        let cached = lock_or_recover(&self.parsed_statements).get(sql).cloned();
        if let Some(statement) = cached {
            return Ok(statement.as_ref().clone());
        }

        let parsed = crate::sql2::parse::parse_statement(sql)?;
        let mut statements = lock_or_recover(&self.parsed_statements);
        if let Some(statement) = statements.get(sql).cloned() {
            drop(statements);
            return Ok(statement.as_ref().clone());
        }
        statements.put(Arc::from(sql), Arc::new(parsed.clone()));
        Ok(parsed)
    }

    /// Reuses one parsed template for UPDATE statements that differ only in
    /// ordinary string literals.
    ///
    /// This intentionally accepts a narrow, unambiguous SQL subset. Existing
    /// placeholders, comments, prefixed string forms, and malformed literals
    /// fall back to exact parsing. The fallback keeps the full SQL surface and
    /// its existing diagnostics authoritative while the common literal CRUD
    /// shape avoids churning the bounded exact-text caches.
    pub(crate) fn auto_parameterized_update(&self, sql: &str) -> Option<AutoParameterizedUpdate> {
        let (normalized_sql, params) = normalize_update_string_literals(sql)?;
        let statement = self.parse_statement(&normalized_sql).ok()?;
        Some(AutoParameterizedUpdate {
            sql: Arc::from(normalized_sql),
            statement,
            params,
        })
    }

    /// Checks an UPDATE template without retaining decoded literal values.
    ///
    /// Large heterogeneous batches use this as a cheap certification pass so
    /// a late shape mismatch cannot leave almost an entire parameter batch in
    /// memory before ordinary sequential classification resumes.
    pub(crate) fn update_literal_shape_matches(&self, sql: &str, normalized_shape: &str) -> bool {
        update_string_literals_match_shape(sql, normalized_shape)
    }

    /// Decodes a previously shape-certified UPDATE into reusable string slots.
    ///
    /// The batch classifier calls [`Self::update_literal_shape_matches`] for
    /// every row before entering this path. Reusing one string per parameter
    /// avoids allocating a transient `Vec<Value>` and owned string set for
    /// every statement while the Arrow columns are being assembled.
    pub(crate) fn decode_certified_update_literals_into(
        &self,
        sql: &str,
        params: &mut [String],
    ) -> bool {
        decode_update_string_literals_into(sql, params)
    }

    /// Returns borrowed decoded string literals when a warm UPDATE still
    /// matches its normalized shape. Ordinary literals borrow the caller's
    /// SQL directly; SQL quote escapes allocate only the affected slot.
    #[cfg(test)]
    pub(crate) fn decode_update_literals_for_shape<'a>(
        &self,
        sql: &'a str,
        normalized_shape: &str,
        parameter_count: usize,
        escape_scratch: &mut SmallVec<[String; 4]>,
    ) -> Option<SmallVec<[Cow<'a, str>; 4]>> {
        if escape_scratch.len() < parameter_count {
            escape_scratch.resize_with(parameter_count, String::new);
        }
        decode_update_string_literals_for_shape(
            sql,
            normalized_shape,
            parameter_count,
            escape_scratch,
        )
    }

    /// Returns stable public-surface metadata for one catalog generation.
    ///
    /// Callers are responsible for using a key that changes whenever any
    /// visible schema changes. Invalid catalogs are not cached.
    pub(crate) fn public_catalog<F>(
        &self,
        catalog_key: &CatalogKey,
        visible_schemas: F,
    ) -> Result<Arc<PublicCatalog>, LixError>
    where
        F: FnOnce() -> Result<Vec<serde_json::Value>, LixError>,
    {
        if let Some(catalog) = lock_or_recover(&self.public_catalogs).get(catalog_key) {
            return Ok(Arc::clone(catalog));
        }

        let visible_schemas = visible_schemas()?;
        let built = Arc::new(PublicCatalog::from_visible_schemas(&visible_schemas)?);
        let mut catalogs = lock_or_recover(&self.public_catalogs);
        if let Some(catalog) = catalogs.get(catalog_key) {
            return Ok(Arc::clone(catalog));
        }
        catalogs.put(catalog_key.clone(), Arc::clone(&built));
        Ok(built)
    }

    /// Returns a cloned write template for this exact planning environment.
    ///
    /// Execution may resolve parameterized branch scopes by mutating its owned
    /// plan, so cache hits must never expose the stored template by reference.
    pub(crate) fn write_plan(
        &self,
        sql: &str,
        catalog_key: &CatalogKey,
        active_branch_id: &str,
    ) -> Option<LogicalWritePlan> {
        let key = WritePlanCacheKey::new(sql, catalog_key.clone(), active_branch_id);
        let cached = lock_or_recover(&self.write_plans).get(&key).cloned();
        cached.map(|plan| plan.as_ref().clone())
    }

    pub(crate) fn remember_write_plan(
        &self,
        sql: &str,
        catalog_key: CatalogKey,
        active_branch_id: &str,
        plan: &LogicalWritePlan,
    ) {
        let key = WritePlanCacheKey::new(sql, catalog_key, active_branch_id);
        lock_or_recover(&self.write_plans).put(key, Arc::new(plan.clone()));
    }

    #[cfg(test)]
    fn with_capacities(
        parsed_statement_capacity: usize,
        public_catalog_capacity: usize,
        write_plan_capacity: usize,
    ) -> Self {
        let session = super::session::new_sql_session_context();
        Self::with_state_and_capacities(
            session.state(),
            parsed_statement_capacity,
            public_catalog_capacity,
            write_plan_capacity,
            parsed_statement_capacity,
        )
    }

    fn with_state_and_capacities(
        datafusion_state: SessionState,
        parsed_statement_capacity: usize,
        public_catalog_capacity: usize,
        write_plan_capacity: usize,
        read_plan_capacity: usize,
    ) -> Self {
        Self {
            datafusion_state,
            read_sessions: Mutex::new(Vec::new()),
            parsed_statements: Mutex::new(LruCache::new(non_zero(parsed_statement_capacity))),
            public_catalogs: Mutex::new(LruCache::new(non_zero(public_catalog_capacity))),
            write_plans: Mutex::new(LruCache::new(non_zero(write_plan_capacity))),
            read_plans: Mutex::new(LruCache::new(non_zero(read_plan_capacity))),
            physical_read_plans: Mutex::new(LruCache::new(non_zero(read_plan_capacity))),
        }
    }
}

impl<CatalogKey> ReadPlanCacheKey<CatalogKey> {
    fn new(sql: &str, params: &[Value], catalog: CatalogKey) -> Self {
        Self {
            sql: Arc::from(sql),
            parameter_types: params.iter().map(ParameterType::from).collect(),
            catalog,
            planner_contract: READ_PLANNER_CONTRACT,
        }
    }
}

impl<CatalogKey> PhysicalReadPlanCacheKey<CatalogKey> {
    pub(crate) fn new(sql: &str, params: &[Value], catalog: CatalogKey) -> Option<Self> {
        let mut parameters = Vec::new();
        for value in params {
            let (tag, bytes) = match value {
                Value::Null => (0, Vec::new()),
                Value::Boolean(false) => (1, Vec::new()),
                Value::Boolean(true) => (2, Vec::new()),
                Value::Integer(value) => (3, value.to_le_bytes().to_vec()),
                Value::Real(value) => (4, value.to_bits().to_le_bytes().to_vec()),
                Value::Text(value) => (5, value.as_bytes().to_vec()),
                Value::Json(value) => {
                    parameters.push(6);
                    let bytes = value.as_bytes();
                    parameters.extend_from_slice(&bytes.len().to_le_bytes());
                    parameters.extend_from_slice(bytes);
                    continue;
                }
                Value::Blob(value) => (7, value.to_vec()),
                Value::Timestamp(value) => (8, value.to_le_bytes().to_vec()),
            };
            parameters.push(tag);
            parameters.extend_from_slice(&bytes.len().to_le_bytes());
            parameters.extend_from_slice(&bytes);
        }
        Some(Self {
            sql: Arc::from(sql),
            parameters: parameters.into(),
            catalog,
            planner_contract: READ_PLANNER_CONTRACT,
        })
    }
}

impl From<&Value> for ParameterType {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(_) => Self::Boolean,
            Value::Integer(_) => Self::Integer,
            Value::Real(_) => Self::Real,
            Value::Text(_) => Self::Text,
            Value::Json(_) => Self::Json,
            Value::Timestamp(_) => Self::Timestamp,
            Value::Blob(_) => Self::Blob,
        }
    }
}

impl<CatalogKey> WritePlanCacheKey<CatalogKey> {
    fn new(sql: &str, catalog: CatalogKey, active_branch_id: &str) -> Self {
        Self {
            sql: Arc::from(sql),
            catalog,
            active_branch_id: Arc::from(active_branch_id),
        }
    }
}

fn non_zero(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity.max(1)).expect("SQL planning cache capacity is non-zero")
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn normalize_update_string_literals(sql: &str) -> Option<(String, Vec<Value>)> {
    let trimmed = sql.trim_start();
    let update = trimmed.get(.."UPDATE".len())?;
    if !update.eq_ignore_ascii_case("UPDATE")
        || !trimmed
            .as_bytes()
            .get("UPDATE".len())
            .is_some_and(u8::is_ascii_whitespace)
    {
        return None;
    }

    let bytes = sql.as_bytes();
    let mut normalized = String::with_capacity(sql.len());
    let mut params = Vec::new();
    let mut cursor = 0;
    let mut copied = 0;
    let mut quoted_identifier = false;

    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => {
                if quoted_identifier && bytes.get(cursor + 1) == Some(&b'"') {
                    cursor += 2;
                    continue;
                }
                quoted_identifier = !quoted_identifier;
                cursor += 1;
            }
            b'-' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'-') => return None,
            b'/' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'*') => return None,
            b'?' | b'$' if !quoted_identifier => return None,
            b'\'' if !quoted_identifier => {
                if bytes[..cursor]
                    .iter()
                    .rposition(|byte| !byte.is_ascii_whitespace())
                    .is_some_and(|index| {
                        matches!(bytes[index], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                    })
                {
                    return None;
                }
                normalized.push_str(&sql[copied..cursor]);
                normalized.push('$');
                normalized.push_str(&(params.len() + 1).to_string());

                cursor += 1;
                let mut value = String::new();
                let value_start = cursor;
                let mut segment_start = cursor;
                loop {
                    let quote = bytes[cursor..]
                        .iter()
                        .position(|byte| *byte == b'\'')
                        .map(|offset| cursor + offset)?;
                    value.push_str(&sql[segment_start..quote]);
                    if bytes.get(quote + 1) == Some(&b'\'') {
                        value.push('\'');
                        cursor = quote + 2;
                        segment_start = cursor;
                        continue;
                    }
                    cursor = quote + 1;
                    copied = cursor;
                    break;
                }
                debug_assert!(cursor > value_start);
                params.push(Value::Text(value));
            }
            _ => cursor += 1,
        }
    }

    if quoted_identifier || params.is_empty() {
        return None;
    }
    normalized.push_str(&sql[copied..]);
    Some((normalized, params))
}

fn update_string_literals_match_shape(sql: &str, normalized_shape: &str) -> bool {
    let trimmed = sql.trim_start();
    let Some(update) = trimmed.get(.."UPDATE".len()) else {
        return false;
    };
    if !update.eq_ignore_ascii_case("UPDATE")
        || !trimmed
            .as_bytes()
            .get("UPDATE".len())
            .is_some_and(u8::is_ascii_whitespace)
    {
        return false;
    }

    let bytes = sql.as_bytes();
    let shape = normalized_shape.as_bytes();
    let mut cursor = 0;
    let mut copied = 0;
    let mut shape_cursor = 0;
    let mut param_count = 0_usize;
    let mut quoted_identifier = false;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => {
                if quoted_identifier && bytes.get(cursor + 1) == Some(&b'"') {
                    cursor += 2;
                    continue;
                }
                quoted_identifier = !quoted_identifier;
                cursor += 1;
            }
            b'-' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'-') => return false,
            b'/' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'*') => return false,
            b'?' | b'$' if !quoted_identifier => return false,
            b'\'' if !quoted_identifier => {
                if bytes[..cursor]
                    .iter()
                    .rposition(|byte| !byte.is_ascii_whitespace())
                    .is_some_and(|index| {
                        matches!(bytes[index], b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
                    })
                {
                    return false;
                }
                let outside = &bytes[copied..cursor];
                if !shape
                    .get(shape_cursor..)
                    .is_some_and(|remaining| remaining.starts_with(outside))
                {
                    return false;
                }
                shape_cursor += outside.len();
                if shape.get(shape_cursor) != Some(&b'$') {
                    return false;
                }
                shape_cursor += 1;
                let digit_start = shape_cursor;
                let mut parameter_number = 0_usize;
                while let Some(digit @ b'0'..=b'9') = shape.get(shape_cursor) {
                    parameter_number = parameter_number
                        .saturating_mul(10)
                        .saturating_add(usize::from(*digit - b'0'));
                    shape_cursor += 1;
                }
                if shape_cursor == digit_start || parameter_number != param_count + 1 {
                    return false;
                }
                param_count += 1;

                cursor += 1;
                loop {
                    let Some(quote) = bytes[cursor..]
                        .iter()
                        .position(|byte| *byte == b'\'')
                        .map(|offset| cursor + offset)
                    else {
                        return false;
                    };
                    if bytes.get(quote + 1) == Some(&b'\'') {
                        cursor = quote + 2;
                        continue;
                    }
                    cursor = quote + 1;
                    copied = cursor;
                    break;
                }
            }
            _ => cursor += 1,
        }
    }
    !quoted_identifier && param_count > 0 && shape.get(shape_cursor..) == Some(&bytes[copied..])
}

#[cfg(test)]
enum DecodedUpdateLiteral<'a> {
    Borrowed(&'a str),
    Escaped(usize),
}

#[cfg(test)]
fn decode_update_string_literals_for_shape<'a>(
    sql: &'a str,
    normalized_shape: &str,
    parameter_count: usize,
    escape_scratch: &mut [String],
) -> Option<SmallVec<[Cow<'a, str>; 4]>> {
    let trimmed = sql.trim_start();
    let update = trimmed.get(.."UPDATE".len())?;
    if !update.eq_ignore_ascii_case("UPDATE")
        || !trimmed
            .as_bytes()
            .get("UPDATE".len())
            .is_some_and(u8::is_ascii_whitespace)
    {
        return None;
    }

    // The row UPDATE lane has exactly two string parameters. Once its
    // normalized shape is certified, compare the three static SQL spans and
    // scan only the two quoted values. The generic state machine below is
    // retained for wider mutation programs, but rerunning it over every byte
    // of one million homogeneous statements is unnecessary.
    if parameter_count == 2
        && let Some((prefix, remainder)) = normalized_shape.split_once("$1")
        && let Some((middle, suffix)) = remainder.split_once("$2")
        && !prefix.contains('$')
        && !middle.contains('$')
        && !suffix.contains('$')
    {
        return decode_two_update_string_literals(sql, prefix, middle, suffix, escape_scratch);
    }

    let bytes = sql.as_bytes();
    let shape = normalized_shape.as_bytes();
    let mut cursor = 0;
    let mut copied = 0;
    let mut shape_cursor = 0;
    let mut quoted_identifier = false;
    let mut decoded = SmallVec::<[DecodedUpdateLiteral<'a>; 4]>::new();
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => {
                if quoted_identifier && bytes.get(cursor + 1) == Some(&b'"') {
                    cursor += 2;
                    continue;
                }
                quoted_identifier = !quoted_identifier;
                cursor += 1;
            }
            b'-' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'-') => return None,
            b'/' if !quoted_identifier && bytes.get(cursor + 1) == Some(&b'*') => return None,
            b'?' | b'$' if !quoted_identifier => return None,
            b'\'' if !quoted_identifier => {
                if decoded.len() >= parameter_count
                    || bytes[..cursor]
                        .iter()
                        .rposition(|byte| !byte.is_ascii_whitespace())
                        .is_some_and(|index| {
                            matches!(
                                bytes[index],
                                b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'
                            )
                        })
                {
                    return None;
                }
                let outside = &bytes[copied..cursor];
                if !shape
                    .get(shape_cursor..)
                    .is_some_and(|remaining| remaining.starts_with(outside))
                {
                    return None;
                }
                shape_cursor += outside.len();
                if shape.get(shape_cursor) != Some(&b'$') {
                    return None;
                }
                shape_cursor += 1;
                let digit_start = shape_cursor;
                let mut parameter_number = 0_usize;
                while let Some(digit @ b'0'..=b'9') = shape.get(shape_cursor) {
                    parameter_number = parameter_number
                        .saturating_mul(10)
                        .saturating_add(usize::from(*digit - b'0'));
                    shape_cursor += 1;
                }
                if shape_cursor == digit_start || parameter_number != decoded.len() + 1 {
                    return None;
                }

                cursor += 1;
                let value_start = cursor;
                let mut segment_start = cursor;
                let mut escaped = false;
                loop {
                    let quote = bytes[cursor..]
                        .iter()
                        .position(|byte| *byte == b'\'')
                        .map(|offset| cursor + offset)?;
                    if bytes.get(quote + 1) == Some(&b'\'') {
                        let value = &mut escape_scratch[decoded.len()];
                        if !escaped {
                            value.clear();
                            value.reserve(quote.saturating_sub(value_start).saturating_add(1));
                            escaped = true;
                        }
                        value.push_str(&sql[segment_start..quote]);
                        value.push('\'');
                        cursor = quote + 2;
                        segment_start = cursor;
                        continue;
                    }
                    let value = if escaped {
                        escape_scratch[decoded.len()].push_str(&sql[segment_start..quote]);
                        DecodedUpdateLiteral::Escaped(decoded.len())
                    } else {
                        DecodedUpdateLiteral::Borrowed(&sql[value_start..quote])
                    };
                    decoded.push(value);
                    cursor = quote + 1;
                    copied = cursor;
                    break;
                }
            }
            _ => cursor += 1,
        }
    }
    if quoted_identifier
        || decoded.len() != parameter_count
        || shape.get(shape_cursor..) != Some(&bytes[copied..])
    {
        return None;
    }
    Some(
        decoded
            .into_iter()
            .map(|value| match value {
                DecodedUpdateLiteral::Borrowed(value) => Cow::Borrowed(value),
                DecodedUpdateLiteral::Escaped(index) => {
                    Cow::Owned(std::mem::take(&mut escape_scratch[index]))
                }
            })
            .collect(),
    )
}

#[cfg(test)]
fn decode_two_update_string_literals<'a>(
    sql: &'a str,
    prefix: &str,
    middle: &str,
    suffix: &str,
    escape_scratch: &mut [String],
) -> Option<SmallVec<[Cow<'a, str>; 4]>> {
    fn decode_one<'a>(
        sql: &'a str,
        cursor: &mut usize,
        scratch: &mut String,
        scratch_index: usize,
    ) -> Option<DecodedUpdateLiteral<'a>> {
        let bytes = sql.as_bytes();
        if bytes.get(*cursor) != Some(&b'\'') {
            return None;
        }
        *cursor += 1;
        let value_start = *cursor;
        let mut segment_start = *cursor;
        let mut escaped = false;
        loop {
            let quote = bytes[*cursor..]
                .iter()
                .position(|byte| *byte == b'\'')
                .map(|offset| *cursor + offset)?;
            if bytes.get(quote + 1) == Some(&b'\'') {
                if !escaped {
                    scratch.clear();
                    scratch.reserve(quote.saturating_sub(value_start).saturating_add(1));
                    escaped = true;
                }
                scratch.push_str(&sql[segment_start..quote]);
                scratch.push('\'');
                *cursor = quote + 2;
                segment_start = *cursor;
                continue;
            }
            *cursor = quote + 1;
            return if escaped {
                scratch.push_str(&sql[segment_start..quote]);
                Some(DecodedUpdateLiteral::Escaped(scratch_index))
            } else {
                Some(DecodedUpdateLiteral::Borrowed(&sql[value_start..quote]))
            };
        }
    }

    let bytes = sql.as_bytes();
    if !bytes.starts_with(prefix.as_bytes()) {
        return None;
    }
    let mut cursor = prefix.len();
    let first = decode_one(sql, &mut cursor, &mut escape_scratch[0], 0)?;
    if !bytes[cursor..].starts_with(middle.as_bytes()) {
        return None;
    }
    cursor += middle.len();
    let second = decode_one(sql, &mut cursor, &mut escape_scratch[1], 1)?;
    if bytes.get(cursor..) != Some(suffix.as_bytes()) {
        return None;
    }
    Some(
        [first, second]
            .into_iter()
            .map(|value| match value {
                DecodedUpdateLiteral::Borrowed(value) => Cow::Borrowed(value),
                DecodedUpdateLiteral::Escaped(index) => {
                    Cow::Owned(std::mem::take(&mut escape_scratch[index]))
                }
            })
            .collect(),
    )
}

fn decode_update_string_literals_into(sql: &str, params: &mut [String]) -> bool {
    params.iter_mut().for_each(String::clear);
    decode_update_string_literals_with(sql, params.len(), |index, segment, escaped| {
        let Some(value) = params.get_mut(index) else {
            return false;
        };
        value.push_str(segment);
        if escaped {
            value.push('\'');
        }
        true
    })
}

fn decode_update_string_literals_with<'a>(
    sql: &'a str,
    parameter_count: usize,
    mut append: impl FnMut(usize, &'a str, bool) -> bool,
) -> bool {
    let bytes = sql.as_bytes();
    let mut cursor = 0_usize;
    let mut param_index = 0_usize;
    let mut quoted_identifier = false;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => {
                if quoted_identifier && bytes.get(cursor + 1) == Some(&b'"') {
                    cursor += 2;
                    continue;
                }
                quoted_identifier = !quoted_identifier;
                cursor += 1;
            }
            b'\'' if !quoted_identifier => {
                if param_index >= parameter_count {
                    return false;
                }
                let current_param = param_index;
                param_index += 1;
                cursor += 1;
                let mut segment_start = cursor;
                loop {
                    let Some(quote) = bytes[cursor..]
                        .iter()
                        .position(|byte| *byte == b'\'')
                        .map(|offset| cursor + offset)
                    else {
                        return false;
                    };
                    let escaped = bytes.get(quote + 1) == Some(&b'\'');
                    if !append(current_param, &sql[segment_start..quote], escaped) {
                        return false;
                    }
                    if escaped {
                        cursor = quote + 2;
                        segment_start = cursor;
                        continue;
                    }
                    cursor = quote + 1;
                    break;
                }
            }
            _ => cursor += 1,
        }
    }
    !quoted_identifier && param_index == parameter_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use crate::sql2::bind_statement;
    use crate::sql2::plan::branch_scope::BranchScope;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use serde_json::json;

    fn test_cache(capacity: usize) -> SqlPlanningCache<String> {
        SqlPlanningCache::with_capacities(capacity, capacity, capacity)
    }

    fn write_plan(sql: &str, active_branch_id: &str) -> LogicalWritePlan {
        let statement = crate::sql2::parse_statement(sql).expect("SQL parses");
        let bound = bind_statement(&statement, &[], active_branch_id).expect("SQL binds");
        crate::sql2::plan_write(bound).expect("SQL plans")
    }

    #[test]
    fn parsed_statements_are_keyed_by_exact_sql_and_bounded() {
        let cache = test_cache(2);
        let sql = "SELECT $1";

        let first = cache.parse_statement(sql).expect("first parse");
        let second = cache.parse_statement(sql).expect("cached parse");
        assert_eq!(first, second);
        assert_eq!(lock_or_recover(&cache.parsed_statements).len(), 1);

        cache
            .parse_statement("SELECT  $1")
            .expect("whitespace variant parses");
        assert_eq!(lock_or_recover(&cache.parsed_statements).len(), 2);

        cache.parse_statement("SELECT 2").expect("third SQL parses");
        let statements = lock_or_recover(&cache.parsed_statements);
        assert_eq!(statements.len(), 2);
        assert!(!statements.contains(sql));
    }

    #[test]
    fn read_plan_keys_include_parameter_type_and_catalog_and_are_bounded() {
        let cache = test_cache(2);
        let plan = || CachedReadPlan {
            plan: LogicalPlanBuilder::empty(false).build().unwrap(),
            json_predicate_params: BTreeSet::new(),
            expected_parameter_count: 1,
        };

        cache.remember_read_plan("SELECT $1", &[Value::Integer(1)], "a".into(), plan());
        assert!(
            cache
                .read_plan("SELECT $1", &[Value::Integer(2)], &"a".into())
                .is_some()
        );
        assert!(
            cache
                .read_plan("SELECT $1", &[Value::Text("2".into())], &"a".into())
                .is_none()
        );
        assert!(
            cache
                .read_plan("SELECT $1", &[Value::Integer(2)], &"b".into())
                .is_none()
        );

        cache.remember_read_plan("SELECT 2", &[], "a".into(), plan());
        cache.remember_read_plan("SELECT 3", &[], "a".into(), plan());
        assert_eq!(cache.read_plan_count(), 2);
        assert!(
            cache
                .read_plan("SELECT $1", &[Value::Integer(1)], &"a".into())
                .is_none()
        );
    }

    #[test]
    fn recycled_read_sessions_drop_snapshot_bound_tables() {
        let cache = test_cache(2);
        let session = cache.datafusion_read_session();
        let session_id = session.session_id();
        session
            .register_table(
                "snapshot_table",
                Arc::new(EmptyTable::new(Arc::new(Schema::empty()))),
            )
            .unwrap();

        cache.recycle_datafusion_read_session(session);
        let recycled = cache.datafusion_read_session();
        assert_eq!(recycled.session_id(), session_id);
        assert!(!recycled.table_exist("snapshot_table").unwrap());
    }

    #[test]
    fn parse_failures_are_not_cached() {
        let cache = test_cache(2);

        assert!(cache.parse_statement("SELECT (").is_err());
        assert!(lock_or_recover(&cache.parsed_statements).is_empty());
    }

    #[test]
    fn literal_updates_share_one_parameterized_parse_template() {
        let cache = test_cache(8);
        let first = cache
            .auto_parameterized_update(
                "UPDATE notes SET value = lix_json('{\"text\":\"first\"}') WHERE id = 'a'",
            )
            .expect("literal update auto-parameterizes");
        let second = cache
            .auto_parameterized_update(
                "UPDATE notes SET value = lix_json('{\"text\":\"second\"}') WHERE id = 'b'",
            )
            .expect("case-equivalent literal update auto-parameterizes");

        assert_eq!(
            first.sql.as_ref(),
            "UPDATE notes SET value = lix_json($1) WHERE id = $2"
        );
        assert_eq!(
            first.params,
            [
                Value::Text("{\"text\":\"first\"}".to_string()),
                Value::Text("a".to_string())
            ]
        );
        assert_eq!(
            second.params,
            [
                Value::Text("{\"text\":\"second\"}".to_string()),
                Value::Text("b".to_string())
            ]
        );
        assert_eq!(lock_or_recover(&cache.parsed_statements).len(), 1);
    }

    #[test]
    fn literal_update_shape_matcher_streams_without_decoding_values() {
        let cache = test_cache(8);
        let template = cache
            .auto_parameterized_update(
                "UPDATE \"notes\" SET value = lix_json('{\"text\":\"first\"}') WHERE id = 'a'",
            )
            .unwrap();

        assert!(cache.update_literal_shape_matches(
            "UPDATE \"notes\" SET value = lix_json('{\"text\":\"it''''s fine\"}') WHERE id = 'b'",
            &template.sql,
        ));
        for different_shape in [
            "UPDATE \"notes\" SET value = lix_json('{}') WHERE other_id = 'b'",
            "UPDATE notes SET value = lix_json('{}') WHERE id = 'b'",
            "UPDATE \"notes\" SET value = lix_json('{}') WHERE id = 'b' -- comment",
            "UPDATE \"notes\" SET value = lix_json('{}') WHERE id = $1",
        ] {
            assert!(
                !cache.update_literal_shape_matches(different_shape, &template.sql),
                "{different_shape}"
            );
        }
    }

    #[test]
    fn certified_literal_decoder_reuses_slots_and_unescapes_quotes() {
        let cache = test_cache(8);
        let mut params = vec![String::with_capacity(32), String::with_capacity(8)];
        params[0].push_str("stale-value");
        params[1].push_str("stale-id");

        assert!(cache.decode_certified_update_literals_into(
            "UPDATE notes SET value = lix_json('{\"text\":\"it''s fine\"}') WHERE id = 'b'",
            &mut params,
        ));
        assert_eq!(params, ["{\"text\":\"it's fine\"}", "b"]);
        assert!(!cache.decode_certified_update_literals_into(
            "UPDATE notes SET value = 'only-one'",
            &mut params,
        ));
    }

    #[test]
    fn warm_literal_decoder_borrows_unescaped_slots_in_one_shape_pass() {
        let cache = test_cache(8);
        let template = cache
            .auto_parameterized_update(
                "UPDATE notes SET value = lix_json('{\"text\":\"first\"}') WHERE id = 'a'",
            )
            .unwrap();
        let mut escape_scratch = SmallVec::new();
        let borrowed = cache
            .decode_update_literals_for_shape(
                "UPDATE notes SET value = lix_json('{\"text\":\"second\"}') WHERE id = 'b'",
                &template.sql,
                2,
                &mut escape_scratch,
            )
            .unwrap();
        assert!(matches!(borrowed[0], Cow::Borrowed(_)));
        assert!(matches!(borrowed[1], Cow::Borrowed(_)));
        assert_eq!(borrowed[0], "{\"text\":\"second\"}");
        assert_eq!(borrowed[1], "b");

        let escaped = cache
            .decode_update_literals_for_shape(
                "UPDATE notes SET value = lix_json('{\"text\":\"it''s fine\"}') WHERE id = 'c'",
                &template.sql,
                2,
                &mut escape_scratch,
            )
            .unwrap();
        assert!(matches!(escaped[0], Cow::Owned(_)));
        assert!(matches!(escaped[1], Cow::Borrowed(_)));
        assert_eq!(escaped[0], "{\"text\":\"it's fine\"}");

        let recycled = match escaped.into_iter().next().unwrap() {
            Cow::Owned(value) => value,
            Cow::Borrowed(_) => panic!("escaped literal must own the scratch buffer"),
        };
        escape_scratch[0] = recycled;
        let allocation = escape_scratch[0].as_ptr();
        let escaped_again = cache
            .decode_update_literals_for_shape(
                "UPDATE notes SET value = lix_json('{\"text\":\"it''s fine\"}') WHERE id = 'd'",
                &template.sql,
                2,
                &mut escape_scratch,
            )
            .unwrap();
        let Cow::Owned(reused) = &escaped_again[0] else {
            panic!("escaped literal must reuse owned scratch storage");
        };
        assert_eq!(reused.as_ptr(), allocation);
    }

    #[test]
    fn ambiguous_literal_updates_keep_exact_sql_path() {
        let cache = test_cache(8);
        for sql in [
            "UPDATE notes SET value = $1 WHERE id = 'a'",
            "UPDATE notes SET value = DATE '2026-08-01' WHERE id = 'a'",
            "UPDATE notes SET value = 'a' -- comment",
            "SELECT 'not an update'",
        ] {
            assert!(cache.auto_parameterized_update(sql).is_none(), "{sql}");
        }
    }

    #[test]
    fn public_catalogs_reuse_stable_metadata_by_catalog_key() {
        let cache = test_cache(2);
        let catalog_a = "catalog-a".to_string();
        let catalog_b = "catalog-b".to_string();
        let schema = json!({
            "x-lix-key": "app_note",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "text": { "type": "string" }
            },
            "required": ["id", "text"]
        });

        let first = cache
            .public_catalog(&catalog_a, || Ok(vec![schema.clone()]))
            .expect("first catalog builds");
        let second = cache
            .public_catalog(&catalog_a, || Ok(vec![schema]))
            .expect("catalog cache hit");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.surface("app_note").is_some());

        let system_only = cache
            .public_catalog(&catalog_b, || Ok(Vec::new()))
            .expect("second catalog builds");
        assert!(system_only.surface("app_note").is_none());
    }

    #[test]
    fn write_plans_are_keyed_by_sql_catalog_and_active_branch() {
        let cache = test_cache(8);
        let catalog_a = "catalog-a".to_string();
        let catalog_b = "catalog-b".to_string();
        let sql = "DELETE FROM lix_file WHERE id = $1";
        let plan = write_plan(sql, "01920000-0000-7000-8000-0000000000a1");
        cache.remember_write_plan(
            sql,
            catalog_a.clone(),
            "01920000-0000-7000-8000-0000000000a1",
            &plan,
        );

        assert_eq!(
            cache.write_plan(sql, &catalog_a, "01920000-0000-7000-8000-0000000000a1"),
            Some(plan)
        );
        assert!(
            cache
                .write_plan(
                    "DELETE  FROM lix_file WHERE id = $1",
                    &catalog_a,
                    "01920000-0000-7000-8000-0000000000a1"
                )
                .is_none()
        );
        assert!(
            cache
                .write_plan(sql, &catalog_b, "01920000-0000-7000-8000-0000000000a1")
                .is_none()
        );
        assert!(
            cache
                .write_plan(sql, &catalog_a, "01920000-0000-7000-8000-0000000000b1")
                .is_none()
        );
    }

    #[test]
    fn write_plan_hits_clone_the_immutable_template() {
        let cache = test_cache(2);
        let catalog = "catalog-a".to_string();
        let sql = "DELETE FROM lix_file WHERE id = $1";
        let plan = write_plan(sql, "01920000-0000-7000-8000-0000000000a1");
        cache.remember_write_plan(
            sql,
            catalog.clone(),
            "01920000-0000-7000-8000-0000000000a1",
            &plan,
        );

        let mut first = cache
            .write_plan(sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
            .expect("first cache hit");
        first.bound.branch_scope = BranchScope::Empty;

        let second = cache
            .write_plan(sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
            .expect("second cache hit");
        assert_eq!(second, plan);
    }

    #[test]
    fn write_plan_cache_evicts_least_recently_used_template() {
        let cache = test_cache(2);
        let catalog = "catalog-a".to_string();
        let first_sql = "DELETE FROM lix_file WHERE id = 'first'";
        let second_sql = "DELETE FROM lix_file WHERE id = 'second'";
        let third_sql = "DELETE FROM lix_file WHERE id = 'third'";
        for sql in [first_sql, second_sql] {
            cache.remember_write_plan(
                sql,
                catalog.clone(),
                "01920000-0000-7000-8000-0000000000a1",
                &write_plan(sql, "01920000-0000-7000-8000-0000000000a1"),
            );
        }
        cache
            .write_plan(first_sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
            .expect("first entry is promoted");
        cache.remember_write_plan(
            third_sql,
            catalog.clone(),
            "01920000-0000-7000-8000-0000000000a1",
            &write_plan(third_sql, "01920000-0000-7000-8000-0000000000a1"),
        );

        assert!(
            cache
                .write_plan(first_sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
                .is_some()
        );
        assert!(
            cache
                .write_plan(second_sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
                .is_none()
        );
        assert!(
            cache
                .write_plan(third_sql, &catalog, "01920000-0000-7000-8000-0000000000a1")
                .is_some()
        );
    }
}
