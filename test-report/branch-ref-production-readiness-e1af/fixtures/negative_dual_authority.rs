fn publish(read: &StorageRead, view: &CoherentView, next_root: &Root) -> Result<()> {
    let BranchHeadControl = legacy_authority();
    let prepared = PreparedPublication::from_branch_view(read, view, next_root);
    compare_and_swap(read, prepared)
}
