use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BranchScope {
    Active {
        branch_id: String,
    },
    Explicit {
        branch_ids: BTreeSet<String>,
    },
    ExplicitDynamic {
        branch_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
    ExplicitRequired {
        branch_ids: BTreeSet<String>,
    },
    ExplicitRequiredDynamic {
        branch_ids: BTreeSet<String>,
        param_indexes: BTreeSet<usize>,
    },
    Global,
    Empty,
}
