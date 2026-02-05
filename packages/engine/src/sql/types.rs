use sqlparser::ast::Statement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistration {
    pub schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableUpdatePlan {
    pub schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableDeletePlan {
    pub schema_key: String,
}

#[derive(Debug, Clone)]
pub enum PostprocessPlan {
    VtableUpdate(VtableUpdatePlan),
    VtableDelete(VtableDeletePlan),
}

#[derive(Debug, Clone)]
pub struct RewriteOutput {
    pub statements: Vec<Statement>,
    pub registrations: Vec<SchemaRegistration>,
    pub postprocess: Option<PostprocessPlan>,
}

#[derive(Debug, Clone)]
pub struct PreprocessOutput {
    pub sql: String,
    pub registrations: Vec<SchemaRegistration>,
    pub postprocess: Option<PostprocessPlan>,
}
