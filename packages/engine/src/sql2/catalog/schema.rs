#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicColumn {
    pub(crate) id: usize,
    pub(crate) name: String,
    pub(crate) role: PublicColumnRole,
    pub(crate) write: PublicColumnWrite,
}

impl PublicColumn {
    pub(crate) fn public(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Public,
            write: PublicColumnWrite::READ_WRITE,
        }
    }

    pub(crate) fn public_insert_only(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Public,
            write: PublicColumnWrite {
                insert: true,
                update: false,
            },
        }
    }

    pub(crate) fn public_update_only(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Public,
            write: PublicColumnWrite {
                insert: false,
                update: true,
            },
        }
    }

    pub(crate) fn public_read_only(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Public,
            write: PublicColumnWrite::READ_ONLY,
        }
    }

    pub(crate) fn hidden(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Hidden,
            write: PublicColumnWrite::READ_ONLY,
        }
    }

    pub(crate) fn internal(name: impl Into<String>) -> Self {
        Self {
            id: 0,
            name: name.into(),
            role: PublicColumnRole::Internal,
            write: PublicColumnWrite::READ_ONLY,
        }
    }

    pub(crate) fn is_public(&self) -> bool {
        self.role == PublicColumnRole::Public
    }

    pub(crate) fn with_id(mut self, id: usize) -> Self {
        self.id = id;
        self
    }

    pub(crate) fn is_insertable(&self) -> bool {
        self.write.insert
    }

    pub(crate) fn is_updatable(&self) -> bool {
        self.write.update
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicColumnRole {
    Public,
    Hidden,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublicColumnWrite {
    insert: bool,
    update: bool,
}

impl PublicColumnWrite {
    const READ_WRITE: Self = Self {
        insert: true,
        update: true,
    };
    const READ_ONLY: Self = Self {
        insert: false,
        update: false,
    };
}
