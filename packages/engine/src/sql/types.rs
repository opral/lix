use sqlparser::ast::Statement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistration {
    pub schema_key: String,
}

#[derive(Debug, Clone)]
pub struct RewriteOutput {
    pub statements: Vec<Statement>,
    pub registrations: Vec<SchemaRegistration>,
}

#[derive(Debug, Clone)]
pub struct PreprocessOutput {
    pub sql: String,
    pub registrations: Vec<SchemaRegistration>,
}
