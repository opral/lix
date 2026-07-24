//! Current stateless whole-state facade (control).

use crate::{ChangeSet, Entity, Result};

pub trait Plugin: Send + Sync + 'static {
    fn detect_changes(&self, active_state: &[Entity], next_file: &[u8]) -> Result<ChangeSet>;

    fn render(&self, active_state: &[Entity]) -> Result<Vec<u8>>;
}
