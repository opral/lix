fn publish(read: &StorageRead, view: &CoherentView, next_root: &Root) -> Result<()> {
    let fresh = storage.begin_read();
    let prepared = PreparedPublication::from_branch_view(fresh, view, next_root);
    compare_and_swap(read, prepared)
}
