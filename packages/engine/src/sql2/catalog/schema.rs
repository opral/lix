#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicColumn {
    pub(crate) id: usize,
    pub(crate) name: String,
    /// Whether a row returned by the public SQL surface may contain SQL NULL.
    ///
    /// This is a SQL surface contract, not a copy of the provider's Arrow
    /// field. Provider schemas also serve planning and write normalization and
    /// can therefore be more permissive than the values a public read emits.
    pub(crate) read_nullable: bool,
    pub(crate) role: PublicColumnRole,
    pub(crate) write: PublicColumnWrite,
    pub(crate) insert_policy: PublicColumnInsertPolicy,
    pub(crate) column_default: Option<String>,
}

impl PublicColumn {
    pub(crate) fn public(name: impl Into<String>, read_nullable: bool) -> Self {
        Self {
            id: 0,
            name: name.into(),
            read_nullable,
            role: PublicColumnRole::Public,
            write: PublicColumnWrite::READ_WRITE,
            insert_policy: PublicColumnInsertPolicy::Required,
            column_default: None,
        }
    }

    pub(crate) fn public_insert_only(name: impl Into<String>, read_nullable: bool) -> Self {
        Self {
            id: 0,
            name: name.into(),
            read_nullable,
            role: PublicColumnRole::Public,
            write: PublicColumnWrite {
                insert: true,
                update: false,
            },
            insert_policy: PublicColumnInsertPolicy::Required,
            column_default: None,
        }
    }

    pub(crate) fn public_read_only(name: impl Into<String>, read_nullable: bool) -> Self {
        Self {
            id: 0,
            name: name.into(),
            read_nullable,
            role: PublicColumnRole::Public,
            write: PublicColumnWrite::READ_ONLY,
            insert_policy: PublicColumnInsertPolicy::ReadOnly,
            column_default: None,
        }
    }

    pub(crate) fn hidden(name: impl Into<String>, read_nullable: bool) -> Self {
        Self {
            id: 0,
            name: name.into(),
            read_nullable,
            role: PublicColumnRole::Hidden,
            write: PublicColumnWrite::READ_ONLY,
            insert_policy: PublicColumnInsertPolicy::ReadOnly,
            column_default: None,
        }
    }

    pub(crate) fn optional_on_insert(mut self) -> Self {
        self.insert_policy = PublicColumnInsertPolicy::Optional;
        self
    }

    pub(crate) fn conditional_on_insert(mut self) -> Self {
        self.insert_policy = PublicColumnInsertPolicy::Conditional;
        self
    }

    pub(crate) fn with_default(mut self, default: impl Into<String>) -> Self {
        self.insert_policy = PublicColumnInsertPolicy::Default;
        self.column_default = Some(default.into());
        self
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicColumnInsertPolicy {
    ReadOnly,
    Required,
    Optional,
    Conditional,
    Default,
}

impl PublicColumnInsertPolicy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "READ_ONLY",
            Self::Required => "REQUIRED",
            Self::Optional => "OPTIONAL",
            Self::Conditional => "CONDITIONAL",
            Self::Default => "DEFAULT",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PublicColumnRole {
    Public,
    Hidden,
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
