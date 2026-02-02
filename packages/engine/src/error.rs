#[derive(Debug)]
pub struct LixError {
    pub message: String,
}

impl std::fmt::Display for LixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for LixError {}
