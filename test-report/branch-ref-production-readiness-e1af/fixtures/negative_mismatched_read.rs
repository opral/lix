fn publish(read: &StorageRead, view: &CoherentView, next_root: &Root) -> Result<()> {
    let other_read = read.clone();
    let prepared = PreparedPublication::from_branch_view(other_read, view, next_root);
    compare_and_swap(read, prepared)
}
