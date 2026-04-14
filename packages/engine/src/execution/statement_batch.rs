#![allow(dead_code)]

use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CaptureSlotId(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaptureShape {
    Scalar,
    OptionalRow,
    ExactlyOneRow,
    RowSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CaptureValueType {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Json,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureColumn {
    pub(crate) name: String,
    pub(crate) value_type: CaptureValueType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaptureSlot {
    pub(crate) id: CaptureSlotId,
    pub(crate) shape: CaptureShape,
    pub(crate) columns: Vec<CaptureColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PreparedParam {
    Literal(Value),
    FromScalarSlot { slot: CaptureSlotId },
    FromRowColumn { slot: CaptureSlotId, column: String },
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
    pub(crate) slot: CaptureSlotId,
    pub(crate) binding: RelationBinding,
    pub(crate) setup_steps: Vec<PreparedRelationStep>,
    pub(crate) cleanup_steps: Vec<PreparedRelationStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedStep {
    pub(crate) sql: String,
    pub(crate) params: Vec<PreparedParam>,
    pub(crate) capture: Option<CaptureSlotId>,
    pub(crate) relation_inputs: Vec<PreparedRelationInput>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct PreparedStatementBatch {
    pub(crate) slots: Vec<CaptureSlot>,
    pub(crate) steps: Vec<PreparedStep>,
}
