pub struct Transaction;
pub struct ForkTreeReadFacade;
pub struct PreparedWrite { pub rank: u64 }
pub enum Outcome { Idempotent, Reconciled }

impl Transaction {
    async fn commit_prepared(&mut self) -> Result<(), ()> {
        let facade = self.forktree_read_facade_from_opening_read();
        self.reconcile_stale_disjoint_writes(&facade).await?;
        self.reconcile_stale_plugin_writes(&facade).await?;
        crate::transaction::context::cohort::reconcile_cohort_files(&facade).await?;
        validate_complete_plan();
        publish_once();
        Ok(())
    }

    fn forktree_read_facade_from_opening_read(&self) -> ForkTreeReadFacade { ForkTreeReadFacade }

    async fn reconcile_stale_disjoint_writes(
        &mut self,
        facade: &ForkTreeReadFacade,
    ) -> Result<(), ()> {
        facade.load_semantic_row("file-a");
        Ok(())
    }

    async fn reconcile_stale_plugin_writes(
        &mut self,
        facade: &ForkTreeReadFacade,
    ) -> Result<Outcome, ()> {
        authenticate_owner_registry(facade, "file-a", 4, "change-a");
        facade.load_semantic_row("file-a");
        let mut writes = vec![PreparedWrite { rank: 1 }];
        writes.sort_by_key(|write| (write.rank, "operation-a"));
        validate_complete_plan();
        let idempotency_keys = ["operation-a"];
        if idempotency_keys.contains(&"operation-a") {
            return Ok(Outcome::Idempotent);
        }
        let _ = writes;
        Ok(Outcome::Reconciled)
    }
}

fn authenticate_owner_registry(
    facade: &ForkTreeReadFacade,
    file_id: &str,
    revision: u64,
    change_id: &str,
) {
    facade.load_owner_proof(file_id, revision, change_id);
    facade.load_registry_proof(revision, change_id);
}

fn validate_complete_plan() {}
fn publish_once() {}
