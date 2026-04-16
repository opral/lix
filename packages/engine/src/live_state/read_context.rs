pub struct LiveReadContext<'a> {
    pub tracked: &'a dyn super::TrackedReadView,
    pub untracked: &'a dyn super::UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn super::TrackedTombstoneView>,
}

impl<'a> LiveReadContext<'a> {
    pub fn new(
        tracked: &'a dyn super::TrackedReadView,
        untracked: &'a dyn super::UntrackedReadView,
    ) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
        }
    }

    pub fn with_tracked_tombstones(
        mut self,
        tracked_tombstones: &'a dyn super::TrackedTombstoneView,
    ) -> Self {
        self.tracked_tombstones = Some(tracked_tombstones);
        self
    }
}
