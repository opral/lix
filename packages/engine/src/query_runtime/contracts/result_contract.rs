#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultContract {
    Select,
    DmlNoReturning,
    DmlReturning,
    Other,
}

impl ResultContract {
    pub(crate) fn expects_postprocess_output(self) -> bool {
        matches!(self, Self::DmlReturning)
    }
}
