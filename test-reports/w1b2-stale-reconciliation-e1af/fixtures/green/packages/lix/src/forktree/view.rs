pub(crate) struct OpeningStorageRead {
    pub(crate) identity: u64,
}

pub(crate) struct ForkTreeReadFacade<'read> {
    pub(crate) read: &'read OpeningStorageRead,
}

impl<'read> ForkTreeReadFacade<'read> {
    pub(crate) fn new(read: &'read OpeningStorageRead) -> Self {
        Self { read }
    }

    pub(crate) fn load_owner_proof(&self, file_id: &str, revision: u64, change_id: &str) {
        let _ = (self.read.identity, file_id, revision, change_id);
    }

    pub(crate) fn load_registry_proof(&self, revision: u64, change_id: &str) {
        let _ = (self.read.identity, revision, change_id);
    }

    pub(crate) fn load_semantic_row(&self, file_id: &str) {
        let _ = (self.read.identity, file_id);
    }
}
