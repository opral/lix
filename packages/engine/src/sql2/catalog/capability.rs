#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SurfaceCapabilities {
    pub(crate) insert: bool,
    pub(crate) update: bool,
    pub(crate) delete: bool,
    pub(crate) select: bool,
}

impl SurfaceCapabilities {
    pub(crate) fn read_only() -> Self {
        Self {
            select: true,
            ..Self::default()
        }
    }

    pub(crate) fn read_write() -> Self {
        Self {
            insert: true,
            update: true,
            delete: true,
            select: true,
        }
    }
}
