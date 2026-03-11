use super::*;

pub(super) async fn lower_public_read_query_with_sql2_backend(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    if !statement_references_public_surface(&registry, &Statement::Query(Box::new(query.clone()))) {
        return Ok(query);
    }
    let active_version_id = load_active_version_id_for_sql2_read(backend).await?;
    let parsed = vec![Statement::Query(Box::new(query.clone()))];
    let prepared = try_prepare_sql2_read_with_internal_access(
        backend,
        &parsed,
        params,
        &active_version_id,
        None,
        true,
    )
    .await?;
    let lowered = if let Some(lowered) = prepared.and_then(|prepared| prepared.lowered_read) {
        lowered
    } else {
        let rewritten = rewrite_public_read_query_to_lowered_sql_with_registry(
            query.clone(),
            backend.dialect(),
            &registry,
        )?;
        if rewritten != query {
            return Ok(rewritten);
        }
        let bound_statement = BoundStatement::from_statement(
            Statement::Query(Box::new(query)),
            params.to_vec(),
            ExecutionContext {
                dialect: Some(backend.dialect()),
                writer_key: None,
                requested_version_id: Some(active_version_id),
            },
        );
        let canonicalized = canonicalize_read(bound_statement, &registry).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "sql2 read subquery canonicalization failed: {}",
                    error.message
                ),
            )
        })?;
        let dependency_spec = augment_dependency_spec_for_public_read(
            &registry,
            &canonicalized,
            derive_dependency_spec_from_canonicalized_read(&canonicalized),
        );
        let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
        lower_read_for_execution(
            &canonicalized,
            effective_state.as_ref().map(|(request, _)| request),
            effective_state.as_ref().map(|(_, plan)| plan),
        )?
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "sql2 could not prepare read subquery"))?
    };
    let statement = lowered.statements.into_iter().next().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "sql2 read subquery lowered to no statements",
        )
    })?;
    let statement = lower_statement(statement, backend.dialect())?;
    match statement {
        Statement::Query(query) => Ok(*query),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "expected lowered subquery to remain a SELECT query",
        )),
    }
}

pub(super) async fn try_prepare_sql2_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    try_prepare_sql2_read_with_internal_access(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
        false,
    )
    .await
}

pub(super) async fn try_prepare_sql2_read_with_internal_access(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    if parsed_statements.len() != 1 {
        return Ok(None);
    }
    let registry = SurfaceRegistry::bootstrap_with_backend(backend)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    if let Some(error) = sql2_public_read_preflight_error(&registry, &parsed_statements[0]) {
        return Err(error);
    }
    let Some((statement, explain_envelope)) = explain_query_statement(&parsed_statements[0]) else {
        return Ok(None);
    };
    let read_summary = summarize_bound_public_read_statement(&registry, &statement);
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }
    let bound_statement = BoundStatement::from_statement(
        statement,
        params.to_vec(),
        ExecutionContext {
            dialect: Some(backend.dialect()),
            writer_key: writer_key.map(ToString::to_string),
            requested_version_id: Some(active_version_id.to_string()),
        },
    );
    let canonicalized = match canonicalize_read(bound_statement.clone(), &registry) {
        Ok(canonicalized) => canonicalized,
        Err(canonicalize_error) => {
            if let Some(prepared) = prepare_sql2_read_via_surface_expansion(
                backend,
                bound_statement,
                explain_envelope.as_ref(),
                &registry,
                allow_internal_tables,
            )
            .await?
            {
                return Ok(Some(prepared));
            }
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "sql2 read preparation failed: {}",
                    canonicalize_error.message
                ),
            ));
        }
    };
    let canonicalized =
        maybe_bind_active_history_root(backend, canonicalized, active_version_id, &registry)
            .await
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "sql2 read preparation could not bind active history root",
                )
            })?;
    ensure_sql2_history_timeline_roots(backend, &canonicalized.bound_statement.statement)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    let dependency_spec = augment_dependency_spec_for_public_read(
        &registry,
        &canonicalized,
        derive_dependency_spec_from_canonicalized_read(&canonicalized),
    );
    if canonicalized.surface_binding.descriptor.surface_family == SurfaceFamily::State {
        if let Some(error) = unknown_public_state_schema_error(&registry, dependency_spec.as_ref())
        {
            return Err(error);
        }
    }
    let effective_state = build_effective_state(&canonicalized, dependency_spec.as_ref());
    let lowered_read = lower_read_for_execution(
        &canonicalized,
        effective_state.as_ref().map(|(request, _)| request),
        effective_state.as_ref().map(|(_, plan)| plan),
    )?
    .map(|program| wrap_lowered_read_for_explain(program, explain_envelope.as_ref()));
    let lowered_sql = lowered_read
        .as_ref()
        .map(|program| {
            program
                .statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(Some(Sql2PreparedRead {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement],
            surface_bindings: vec![canonicalized.surface_binding.descriptor.public_name.clone()],
            bound_public_leaves: vec![sql2_bound_public_leaf(&canonicalized.surface_binding)],
            dependency_spec: dependency_spec.clone(),
            effective_state_request: effective_state.as_ref().map(|(request, _)| request.clone()),
            effective_state_plan: effective_state.as_ref().map(|(_, plan)| plan.clone()),
            pushdown_decision: lowered_read
                .as_ref()
                .map(|program| program.pushdown_decision.clone()),
            write_command: None,
            scope_proof: None,
            schema_proof: None,
            target_set_proof: None,
            resolved_write_plan: None,
            domain_change_batch: None,
            commit_preconditions: None,
            invariant_trace: None,
            write_phase_trace: Vec::new(),
            lowered_sql,
        },
        dependency_spec,
        effective_state_request: effective_state.as_ref().map(|(request, _)| request.clone()),
        effective_state_plan: effective_state.as_ref().map(|(_, plan)| plan.clone()),
        lowered_read,
        canonicalized: Some(canonicalized),
    }))
}

async fn prepare_sql2_read_via_surface_expansion(
    backend: &dyn LixBackend,
    bound_statement: BoundStatement,
    explain_envelope: Option<&ExplainEnvelope>,
    registry: &SurfaceRegistry,
    allow_internal_tables: bool,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    ensure_sql2_history_timeline_roots(backend, &bound_statement.statement)
        .await
        .map_err(|error| LixError::new(error.code, error.description))?;
    let read_summary = summarize_bound_public_read_statement(registry, &bound_statement.statement);
    if read_summary.bound_surface_bindings.is_empty() {
        return Ok(None);
    }
    if !allow_internal_tables && !read_summary.internal_relations.is_empty() {
        return Err(mixed_public_internal_query_error(
            &read_summary.internal_relations,
        ));
    }

    let mut rewritten_statement = bound_statement.statement.clone();
    rewrite_supported_public_read_surfaces_in_statement_with_registry(
        &mut rewritten_statement,
        registry,
    )?;
    if statement_references_public_surface(registry, &rewritten_statement) {
        return Ok(None);
    }
    if rewritten_statement == bound_statement.statement {
        return Ok(None);
    }

    let lowered_read = wrap_lowered_read_for_explain(
        LoweredReadProgram {
            statements: vec![rewritten_statement.clone()],
            pushdown_decision: PushdownDecision::default(),
        },
        explain_envelope,
    );
    let dependency_spec = augment_dependency_spec_for_broad_public_read(
        registry,
        derive_dependency_spec_from_bound_public_surface_bindings(
            &read_summary.bound_surface_bindings,
        ),
    );
    if let Some(error) = unknown_public_state_schema_error(registry, dependency_spec.as_ref()) {
        return Err(error);
    }
    let bound_public_leaves = read_summary
        .bound_surface_bindings
        .iter()
        .map(sql2_bound_public_leaf)
        .collect::<Vec<_>>();

    Ok(Some(Sql2PreparedRead {
        debug_trace: Sql2DebugTrace {
            bound_statements: vec![bound_statement.clone()],
            surface_bindings: bound_public_surface_names(registry, &bound_statement.statement),
            bound_public_leaves,
            dependency_spec: dependency_spec.clone(),
            effective_state_request: None,
            effective_state_plan: None,
            pushdown_decision: Some(PushdownDecision::default()),
            write_command: None,
            scope_proof: None,
            schema_proof: None,
            target_set_proof: None,
            resolved_write_plan: None,
            domain_change_batch: None,
            commit_preconditions: None,
            invariant_trace: None,
            write_phase_trace: Vec::new(),
            lowered_sql: lowered_read
                .statements
                .iter()
                .map(ToString::to_string)
                .collect(),
        },
        dependency_spec,
        effective_state_request: None,
        effective_state_plan: None,
        lowered_read: Some(lowered_read),
        canonicalized: None,
    }))
}

pub(super) async fn prepare_sql2_read(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Option<Sql2PreparedRead> {
    try_prepare_sql2_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
    .ok()
    .flatten()
}

pub(super) async fn prepare_sql2_read_strict(
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
) -> Result<Option<Sql2PreparedRead>, LixError> {
    try_prepare_sql2_read(
        backend,
        parsed_statements,
        params,
        active_version_id,
        writer_key,
    )
    .await
}
