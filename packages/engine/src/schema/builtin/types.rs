#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LixAccount {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LixActiveVersion {
    pub id: String,
    pub version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LixCommit {
    pub id: String,
    #[serde(default)]
    pub change_set_id: Option<String>,
    #[serde(default)]
    pub change_ids: Vec<String>,
    #[serde(default)]
    pub author_account_ids: Vec<String>,
    #[serde(default)]
    pub parent_commit_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LixVersionDescriptor {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LixVersionRef {
    pub id: String,
    pub commit_id: String,
}
