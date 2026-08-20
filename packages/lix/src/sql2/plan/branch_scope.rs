#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BranchScope {
    Active {
        branch_id: String,
    },
    Global,
    Empty,
}
