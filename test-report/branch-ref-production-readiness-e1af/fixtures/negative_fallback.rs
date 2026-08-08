fn publish(read: &StorageRead, view: &CoherentView, next_root: &Root) -> Result<()> {
    let prepared = PreparedPublication::from_branch_view(read, view, next_root);
    fallback_branch_ref(read).unwrap_or_else(|| legacy_reader());
    compare_and_swap(read, prepared)
}
