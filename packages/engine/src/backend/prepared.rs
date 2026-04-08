pub use crate::contracts::artifacts::{PreparedBatch, PreparedStatement};

impl PreparedBatch {
    pub fn append_sql(&mut self, sql: impl AsRef<str>) {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return;
        }
        self.steps.push(PreparedStatement {
            sql: sql.to_string(),
            params: Vec::new(),
        });
    }

    pub fn push_statement(&mut self, statement: PreparedStatement) {
        self.steps.push(statement);
    }

    pub fn extend(&mut self, other: PreparedBatch) {
        self.steps.extend(other.steps);
    }
}
