use crate::live_state::{SchemaRegistration, SchemaRegistrationSet};
use crate::sql::{
    coalesce_live_table_requirements, PreparedPublicRead, PreparedWriteStatementKind,
    PublicReadSource, WriteDiagnosticContext,
};
use crate::transaction::{
    PreparedDirectWriteArtifact, PreparedScalarReadArtifact, PreparedWriteFunctionBindings,
    PreparedWriteStatement,
};
use crate::{LixError, QueryResult};

use super::buffered::{build_transaction_write_delta, TransactionWriteDelta};
use super::pipeline::WriteExecutionOutcome;

pub(crate) struct WriteCommand {
    prepared: PreparedWriteStatement,
    function_bindings: PreparedWriteFunctionBindings,
    transaction_write_delta: Option<TransactionWriteDelta>,
    schema_registrations: SchemaRegistrationSet,
}

pub(crate) enum WritePath<'a> {
    ExplainOnly,
    PendingRead(&'a PreparedPublicRead),
    CommittedRead(&'a PreparedPublicRead),
    ScalarRead(&'a PreparedScalarReadArtifact),
    BufferedDelta(&'a TransactionWriteDelta),
    NoopWrite,
    DirectWrite(&'a PreparedDirectWriteArtifact),
}

pub(crate) enum WriteResult {
    Immediate(QueryResult),
    Outcome(WriteExecutionOutcome),
}

impl WriteCommand {
    pub(crate) fn build(
        prepared: PreparedWriteStatement,
        function_bindings: &PreparedWriteFunctionBindings,
    ) -> Result<Self, LixError> {
        let schema_registrations = schema_registrations_for_prepared_write_statement(&prepared);
        let transaction_write_delta = if prepared.diagnostic_context.explain_mode.is_some() {
            None
        } else {
            build_transaction_write_delta(&prepared, function_bindings)?
        };
        Ok(Self {
            prepared,
            function_bindings: function_bindings.clone(),
            transaction_write_delta,
            schema_registrations,
        })
    }

    pub(crate) fn prepared(&self) -> &PreparedWriteStatement {
        &self.prepared
    }

    pub(crate) fn diagnostic_context(&self) -> &WriteDiagnosticContext {
        &self.prepared.diagnostic_context
    }

    pub(crate) fn statement_kind(&self) -> PreparedWriteStatementKind {
        self.prepared.statement_kind
    }

    pub(crate) fn transaction_write_delta(&self) -> Option<&TransactionWriteDelta> {
        self.transaction_write_delta.as_ref()
    }

    pub(crate) fn function_bindings(&self) -> &PreparedWriteFunctionBindings {
        &self.function_bindings
    }

    pub(crate) fn schema_registrations(&self) -> &SchemaRegistrationSet {
        &self.schema_registrations
    }

    pub(crate) fn has_materialization_plan(&self) -> bool {
        self.transaction_write_delta
            .as_ref()
            .is_some_and(|delta| !delta.materialization_plan().units.is_empty())
    }

    pub(crate) fn is_bufferable_write(&self) -> bool {
        self.prepared.diagnostic_context.explain_mode.is_none()
            && self.transaction_write_delta.is_some()
            && !matches!(
                self.prepared.result_contract,
                crate::sql::ResultContract::DmlReturning
            )
            && !matches!(
                self.prepared.statement_kind,
                PreparedWriteStatementKind::Query | PreparedWriteStatementKind::Explain
            )
    }

    pub(crate) fn path(&self) -> WritePath<'_> {
        if self
            .prepared
            .diagnostic_context
            .plain_explain_template
            .is_some()
        {
            return WritePath::ExplainOnly;
        }
        if let Some(public_read) = self.prepared.public_read() {
            return match public_read.contract.source() {
                PublicReadSource::PendingOverlay => WritePath::PendingRead(public_read),
                PublicReadSource::Committed(_) => WritePath::CommittedRead(public_read),
            };
        }
        if let Some(scalar_read) = self.prepared.scalar_read() {
            return WritePath::ScalarRead(scalar_read);
        }
        if let Some(delta) = self.transaction_write_delta.as_ref() {
            return WritePath::BufferedDelta(delta);
        }
        if self.prepared.public_write().is_some() {
            return WritePath::NoopWrite;
        }
        WritePath::DirectWrite(
            self.prepared
                .direct_write()
                .expect("prepared non-public execution must include direct ops"),
        )
    }
}

fn schema_registrations_for_prepared_write_statement(
    statement: &PreparedWriteStatement,
) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    let Some(direct) = statement.direct_write() else {
        return registrations;
    };

    for requirement in coalesce_live_table_requirements(&direct.live_table_requirements) {
        match requirement.schema_definition.as_ref() {
            Some(schema_definition) => {
                registrations.insert(SchemaRegistration::with_schema_definition(
                    requirement.schema_key.clone(),
                    schema_definition.clone(),
                ));
            }
            None => {
                registrations.insert(requirement.schema_key.clone());
            }
        }
    }

    registrations
}
