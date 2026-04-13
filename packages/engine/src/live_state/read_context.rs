pub struct LiveReadContext<'a> {
    pub tracked: &'a dyn super::TrackedReadView,
    pub untracked: &'a dyn super::UntrackedReadView,
    pub tracked_tombstones: Option<&'a dyn super::TrackedTombstoneView>,
    pub writer_keys: &'a dyn super::WriterKeyReadView,
}

impl<'a> LiveReadContext<'a> {
    pub fn new(
        tracked: &'a dyn super::TrackedReadView,
        untracked: &'a dyn super::UntrackedReadView,
        writer_keys: &'a dyn super::WriterKeyReadView,
    ) -> Self {
        Self {
            tracked,
            untracked,
            tracked_tombstones: None,
            writer_keys,
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
