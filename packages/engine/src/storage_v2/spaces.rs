use crate::backend_v2::SpaceId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSpace {
    pub id: SpaceId,
}

impl StorageSpace {
    pub const fn new(id: SpaceId) -> Self {
        Self { id }
    }
}
