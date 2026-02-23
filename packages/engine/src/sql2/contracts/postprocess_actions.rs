#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PostprocessAction {
    None,
    SqlFollowup,
}
