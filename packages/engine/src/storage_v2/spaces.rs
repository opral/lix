use crate::backend_v2::SpaceId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSpace {
    pub id: SpaceId,
    pub name: &'static str,
}

impl StorageSpace {
    pub const fn new(id: SpaceId, name: &'static str) -> Self {
        Self { id, name }
    }
}

impl std::fmt::Display for StorageSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", self.name, self.id)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend_v2::SpaceId;
    use crate::storage_v2::StorageSpace;

    #[test]
    fn storage_space_preserves_id_and_name() {
        let space = StorageSpace::new(SpaceId(7), "test.space");

        assert_eq!(space.id, SpaceId(7));
        assert_eq!(space.name, "test.space");
        assert_eq!(space.to_string(), "test.space(SpaceId(7))");
    }
}
