use crate::forktree::view::ForkTreeReadFacade;

pub async fn reconcile_cohort_files(facade: &ForkTreeReadFacade<'_>) -> Result<(), ()> {
    facade.load_semantic_row("file-a");
    load_cohort_plugin_groups(facade);
    Ok(())
}

fn load_cohort_plugin_groups(facade: &ForkTreeReadFacade<'_>) {
    facade.load_owner_proof("file-a", 4, "change-a");
    facade.load_registry_proof(4, "change-a");
}
