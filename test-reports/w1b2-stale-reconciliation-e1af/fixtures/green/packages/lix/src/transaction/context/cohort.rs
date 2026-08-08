use super::{ForkTreeReadFacade, PreparedWrite};

pub async fn reconcile_cohort_files(facade: &ForkTreeReadFacade) -> Result<(), ()> {
    facade.load_semantic_row("cohort-file");
    load_cohort_plugin_groups(facade).await
}

async fn load_cohort_plugin_groups(facade: &ForkTreeReadFacade) -> Result<(), ()> {
    let _ = PreparedWrite { rank: 2 };
    facade.load_owner_proof("cohort-file", 4, "change-cohort");
    Ok(())
}
