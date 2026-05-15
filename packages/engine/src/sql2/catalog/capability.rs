#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SurfaceCapabilities {
    pub(crate) insert: bool,
    pub(crate) update: bool,
    pub(crate) delete: bool,
    pub(crate) select: bool,
}
