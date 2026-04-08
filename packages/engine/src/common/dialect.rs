#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Sqlite,
    Postgres,
}

impl SqlDialect {
    /// Returns a positional placeholder token for this dialect.
    ///
    /// `index` is 1-based. SQLite uses `?N`, Postgres uses `$N`.
    pub fn placeholder(self, index: usize) -> String {
        match self {
            Self::Sqlite => format!("?{index}"),
            Self::Postgres => format!("${index}"),
        }
    }
}
