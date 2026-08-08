use crate::forktree::view::{ForkTreeReadFacade, OpeningStorageRead};

pub struct Transaction {
    opening_read: OpeningStorageRead,
    atomic_commit: AtomicCommit,
}

pub struct AtomicCommit;
pub struct PreparedWrite {
    pub rank: u64,
}
pub struct PreparedPlan;
pub enum Outcome {
    Idempotent,
    Reconciled,
}

impl Transaction {
    pub async fn commit_prepared(&mut self, writes: &[PreparedWrite]) -> Result<(), ()> {
        let facade = ForkTreeReadFacade::new(&self.opening_read);
        self.reconcile_stale_disjoint_writes(&facade).await?;
        self.reconcile_stale_plugin_writes(&facade, writes).await?;
        crate::transaction::context::cohort::reconcile_cohort_files(&facade).await?;
        validate_complete_plan(&writes);
        let plan = PreparedPlan;
        self.atomic_commit.commit(&self.opening_read, plan);
        Ok(())
    }

    async fn reconcile_stale_disjoint_writes(
        &mut self,
        facade: &ForkTreeReadFacade<'_>,
    ) -> Result<(), ()> {
        facade.load_semantic_row("file-a");
        Ok(())
    }

    async fn reconcile_stale_plugin_writes(
        &mut self,
        facade: &ForkTreeReadFacade<'_>,
        writes: &[PreparedWrite],
    ) -> Result<Outcome, ()> {
        authenticate_owner_registry(facade, "file-a", 4, "change-a");
        facade.load_semantic_row("file-a");
        let mut writes = writes.to_vec();
        writes.sort_by_key(|write| (write.rank, "operation-a"));
        validate_complete_plan(&writes);
        let idempotency_keys = ["operation-a", "operation-b"];
        for key in idempotency_keys {
            let _ = key;
        }
        if idempotency_keys.len() == writes.len() {
            return Ok(Outcome::Idempotent);
        }
        Ok(Outcome::Reconciled)
    }
}

impl AtomicCommit {
    fn commit(&mut self, read: &OpeningStorageRead, plan: PreparedPlan) {
        let _ = (read.identity, plan);
    }
}

fn authenticate_owner_registry(
    facade: &ForkTreeReadFacade<'_>,
    file_id: &str,
    revision: u64,
    change_id: &str,
) {
    facade.load_owner_proof(file_id, revision, change_id);
    facade.load_registry_proof(revision, change_id);
}

fn validate_complete_plan(writes: &[PreparedWrite]) {
    assert!(!writes.is_empty());
    assert!(writes.windows(2).all(|pair| pair[0].rank < pair[1].rank));
}
