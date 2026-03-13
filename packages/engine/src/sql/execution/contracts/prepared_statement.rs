use crate::Value;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedStatement {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedBatch {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}

impl PreparedBatch {
    pub(crate) fn append_sql(&mut self, sql: impl AsRef<str>) {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return;
        }
        if !self.sql.trim().is_empty() {
            self.sql.push_str("; ");
        }
        self.sql.push_str(sql);
    }
}
