#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicColumn {
    pub(crate) name: String,
    pub(crate) role: PublicColumnRole,
}

impl PublicColumn {
    pub(crate) fn public(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: PublicColumnRole::Public,
        }
    }

    pub(crate) fn hidden(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: PublicColumnRole::Hidden,
        }
    }

    pub(crate) fn internal(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            role: PublicColumnRole::Internal,
        }
    }

    pub(crate) fn is_public(&self) -> bool {
        self.role == PublicColumnRole::Public
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicColumnRole {
    Public,
    Hidden,
    Internal,
}
