use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AppContext {
    pub lix_path: Option<PathBuf>,
}
