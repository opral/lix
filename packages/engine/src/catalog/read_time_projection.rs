#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogReadTimeProjectionRequest {
    pub(crate) surface_name: String,
    pub(crate) requested_version_id: Option<String>,
}
