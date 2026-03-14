#![allow(dead_code)]

use crate::sql::execution::contracts::prepared_statement::PreparedBatch;
use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ProgramSlotId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlotShape {
    Scalar,
    OptionalRow,
    ExactlyOneRow,
    RowSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlotValueType {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Json,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotColumn {
    pub(crate) name: String,
    pub(crate) value_type: SlotValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProgramSlot {
    pub(crate) id: ProgramSlotId,
    pub(crate) shape: SlotShape,
    pub(crate) columns: Vec<SlotColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedParam {
    Literal(Value),
    FromScalarSlot { slot: ProgramSlotId },
    FromRowColumn { slot: ProgramSlotId, column: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationBinding {
    pub(crate) alias: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedRelationStep {
    pub(crate) sql: String,
    pub(crate) params: Vec<PreparedParam>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedRelationInput {
    pub(crate) slot: ProgramSlotId,
    pub(crate) binding: RelationBinding,
    pub(crate) setup_steps: Vec<PreparedRelationStep>,
    pub(crate) cleanup_steps: Vec<PreparedRelationStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedStep {
    pub(crate) sql: String,
    pub(crate) params: Vec<PreparedParam>,
    pub(crate) capture: Option<ProgramSlotId>,
    pub(crate) relation_inputs: Vec<PreparedRelationInput>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct PreparedProgram {
    pub(crate) slots: Vec<ProgramSlot>,
    pub(crate) steps: Vec<PreparedStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteStep {
    PreparedBatch(PreparedBatch),
    Statement { sql: String, params: Vec<Value> },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct WriteProgram {
    pub(crate) steps: Vec<WriteStep>,
}

impl WriteProgram {
    pub(crate) fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub(crate) fn push_batch(&mut self, batch: PreparedBatch) {
        self.steps.push(WriteStep::PreparedBatch(batch));
    }

    pub(crate) fn push_statement(&mut self, sql: impl Into<String>, params: Vec<Value>) {
        self.steps.push(WriteStep::Statement {
            sql: sql.into(),
            params,
        });
    }

    pub(crate) fn extend(&mut self, other: WriteProgram) {
        self.steps.extend(other.steps);
    }
}

pub(crate) fn lower_write_program(program: WriteProgram) -> PreparedProgram {
    let steps = program
        .steps
        .into_iter()
        .map(|step| match step {
            WriteStep::PreparedBatch(batch) => PreparedStep {
                sql: batch.sql,
                params: batch
                    .params
                    .into_iter()
                    .map(PreparedParam::Literal)
                    .collect(),
                capture: None,
                relation_inputs: Vec::new(),
            },
            WriteStep::Statement { sql, params } => PreparedStep {
                sql,
                params: params.into_iter().map(PreparedParam::Literal).collect(),
                capture: None,
                relation_inputs: Vec::new(),
            },
        })
        .collect();

    PreparedProgram {
        slots: Vec::new(),
        steps,
    }
}
