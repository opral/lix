use crate::live_state::tracked::{TrackedReadView, TrackedTombstoneView};
use crate::live_state::untracked::UntrackedReadView;

pub struct ReadViews<'a> {
    pub tracked: &'a dyn TrackedReadView,
    pub untracked: &'a dyn UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn TrackedTombstoneView>,
}

impl<'a> ReadViews<'a> {
    pub fn new(tracked: &'a dyn TrackedReadView, untracked: &'a dyn UntrackedReadView) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
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
