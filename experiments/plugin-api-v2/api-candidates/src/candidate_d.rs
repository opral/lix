//! Stateless exports over a host-owned transactional document context.

use crate::{
    Change, ChangeSet, EntitySource, FileEdits, IdAllocator, PrivateIndex, Result, Source, Splice,
};

pub struct DocumentContext<'a> {
    pub before: Source<'a>,
    pub after: Source<'a>,
    pub entities: EntitySource<'a>,
    pub index: &'a mut PrivateIndex,
    pub ids: &'a mut IdAllocator,
}

pub trait Plugin: Send + Sync + 'static {
    fn file_changed(
        &self,
        context: &mut DocumentContext<'_>,
        edits: &[Splice],
    ) -> Result<ChangeSet>;

    fn entities_changed(
        &self,
        context: &mut DocumentContext<'_>,
        changes: &[Change],
    ) -> Result<FileEdits>;
}
