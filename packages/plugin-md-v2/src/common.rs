#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DocumentSnapshotContent {
    pub(crate) id: String,
    pub(crate) order: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockSnapshotContent {
    pub(crate) id: String,
    #[serde(rename = "type")]
    pub(crate) node_type: String,
    pub(crate) node: serde_json::Value,
    pub(crate) markdown: String,
}
