use crate::live_state::tracked::{TrackedReadView, TrackedTombstoneView};
use crate::live_state::untracked::UntrackedReadView;
use crate::workspace::writer_key::WorkspaceWriterKeyReadView;

pub struct ReadViews<'a> {
    pub tracked: &'a dyn TrackedReadView,
    pub untracked: &'a dyn UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
    pub(crate) workspace_writer_keys: &'a dyn WorkspaceWriterKeyReadView,
}

impl<'a> ReadViews<'a> {
    pub(crate) fn new(
        tracked: &'a dyn TrackedReadView,
        untracked: &'a dyn UntrackedReadView,
        workspace_writer_keys: &'a dyn WorkspaceWriterKeyReadView,
    ) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
            workspace_writer_keys,
        }
    }

    pub fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }
}
