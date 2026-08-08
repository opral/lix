pub(crate) struct ForkTreeReadFacade;

impl ForkTreeReadFacade {
    pub(crate) fn load_owner_proof(&self, _file_id: &str, _revision: u64, _change_id: &str) {}
    pub(crate) fn load_registry_proof(&self, _revision: u64, _change_id: &str) {}
    pub(crate) fn load_semantic_row(&self, _file_id: &str) {}
}
