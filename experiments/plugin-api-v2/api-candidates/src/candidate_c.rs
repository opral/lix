//! Pure reducer whose opaque checkpoint crosses the boundary on every call.

use crate::{
    Change, ChangeSet, Checkpoint, EntitySource, FileEdits, IdAllocator, Result, Source, Splice,
};

pub struct OpenDocument<'a> {
    pub file: Source<'a>,
    pub entities: EntitySource<'a>,
}

pub struct FileUpdate<'a> {
    pub edits: &'a [Splice],
    pub after: Source<'a>,
    pub ids: &'a mut IdAllocator,
}

pub struct EntityUpdate<'a> {
    pub changes: &'a [Change],
    pub current_entities: EntitySource<'a>,
}

pub struct FileTransition {
    pub checkpoint: Checkpoint,
    pub changes: ChangeSet,
}

pub struct EntityTransition {
    pub checkpoint: Checkpoint,
    pub edits: FileEdits,
}

pub trait Plugin: Send + Sync + 'static {
    fn open(&self, input: OpenDocument<'_>) -> Result<Checkpoint>;

    fn file_changed(
        &self,
        checkpoint: &Checkpoint,
        input: FileUpdate<'_>,
    ) -> Result<FileTransition>;

    fn entities_changed(
        &self,
        checkpoint: &Checkpoint,
        input: EntityUpdate<'_>,
    ) -> Result<EntityTransition>;
}
