use super::types::{
    Change, ChangeId, Commit, CommitId, CommitVisibility, Segment, SegmentObjectLocation,
};
use crate::common::LixError;

pub(crate) trait ChangelogReader {
    fn load_visible_commit(&self, commit_id: &CommitId) -> Result<Option<Commit>, LixError>;

    fn load_change(&self, change_id: &ChangeId) -> Result<Option<Change>, LixError>;

    fn locate_change(
        &self,
        change_id: &ChangeId,
    ) -> Result<Option<SegmentObjectLocation>, LixError>;
}

pub(crate) trait ChangelogWriter {
    fn write_segment(&mut self, segment: Segment) -> Result<(), LixError>;

    fn publish_commit(&mut self, visibility: CommitVisibility) -> Result<(), LixError>;
}
