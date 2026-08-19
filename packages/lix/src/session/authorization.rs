use crate::authorization::{
    AuthorizationDecision, AuthorizationDocuments, AuthorizationRequest, ENTITIES_PATH,
    PERMISSIONS_DIRECTORY, SCHEMA_PATH,
};
use crate::storage_adapter::Storage;
use crate::{LixError, Value};

use super::SessionContext;

const LOAD_AUTHORIZATION_DOCUMENTS_SQL: &str = "SELECT path, content FROM lix_file \
     WHERE path = '/.lix/permissions/schema.cedarschema' \
        OR path = '/.lix/permissions/entities.cedar.json' \
        OR path LIKE '/.lix/permissions/%.cedar' \
     ORDER BY path";
const LOAD_PROJECTED_AUTHORIZATION_DOCUMENTS_SQL: &str =
    "SELECT kind, path, source FROM cedar_permission_source ORDER BY path";
const LOAD_PERMISSION_GRANTS_SQL: &str = "SELECT principal_type, principal_id, access_level, \
     resource_type, directory_id, file_id, schema_key, row_pk \
     FROM lix_permission_grant ORDER BY id";

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn authorize(
        &self,
        action: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<AuthorizationDecision, LixError> {
        let Some(mut documents) = self.load_authorization_documents().await? else {
            return Ok(AuthorizationDecision::Inactive);
        };
        let default_grants = self
            .load_applicable_default_grant_policies(action, resource_type, resource_id)
            .await?;
        if !default_grants.is_empty() {
            documents
                .policies
                .push_str("// source: lix_permission_grant default adapter\n");
            documents.policies.push_str(&default_grants);
        }
        crate::authorization::authorize(
            AuthorizationDocuments {
                schema: &documents.schema,
                policies: &documents.policies,
                entities: documents.entities.as_deref(),
            },
            AuthorizationRequest {
                principal_id: self.active_account_id(),
                action,
                resource_type,
                resource_id,
            },
        )
    }

    pub(crate) async fn require_file_view(&self, path: &str) -> Result<(), LixError> {
        let result = self
            .execute(
                "SELECT id FROM lix_file WHERE path = $1",
                &[Value::Text(path.into())],
            )
            .await?;
        let Some(row) = result.rows().first() else {
            return Ok(());
        };
        let Some(Value::Text(file_id)) = result.get(row, "id") else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "file authorization lookup returned an invalid id",
            ));
        };
        match self.authorize("view", "File", file_id).await? {
            AuthorizationDecision::Inactive | AuthorizationDecision::Allow => Ok(()),
            AuthorizationDecision::Deny => Err(LixError::new(
                LixError::CODE_PERMISSION_DENIED,
                "Cedar denied access to the requested file",
            )
            .with_details(serde_json::json!({
                "principal": self.active_account_id(),
                "action": "view",
                "resource_type": "File",
                "resource_id": file_id,
            }))),
        }
    }

    async fn load_authorization_documents(
        &self,
    ) -> Result<Option<OwnedAuthorizationDocuments>, LixError> {
        match self
            .execute(LOAD_PROJECTED_AUTHORIZATION_DOCUMENTS_SQL, &[])
            .await
        {
            Ok(result) => match documents_from_projection(&result)? {
                Some(documents) => Ok(Some(documents)),
                // Installing a plugin and observing its schema must never turn
                // a repository with canonical policy files into fail-open.
                // Projection rebuilds are asynchronous with respect to some
                // installation paths, so consult the files when no projected
                // schema is available yet.
                None => {
                    let result = self.execute(LOAD_AUTHORIZATION_DOCUMENTS_SQL, &[]).await?;
                    documents_from_files(&result)
                }
            },
            Err(error) if error.code == LixError::CODE_TABLE_NOT_FOUND => {
                let result = self.execute(LOAD_AUTHORIZATION_DOCUMENTS_SQL, &[]).await?;
                documents_from_files(&result)
            }
            Err(error) => Err(error),
        }
    }

    async fn load_applicable_default_grant_policies(
        &self,
        action: &str,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<String, LixError> {
        let requested = RequestedPermissionResource::parse(resource_type, resource_id);
        let directory_ancestry = self.load_directory_ancestry(&requested).await?;
        let result = self.execute(LOAD_PERMISSION_GRANTS_SQL, &[]).await?;
        let mut policies = String::new();
        for row in result.rows() {
            let grant = PermissionGrant::from_result(&result, row)?;
            if !default_access_level_allows(&grant.access_level, action)
                || !grant.resource.applies_to(&requested, &directory_ancestry)
            {
                continue;
            }
            policies.push_str(&crate::authorization::default_grant_policy(
                &grant.principal_type,
                grant.principal_id.as_deref(),
                action,
                resource_type,
                resource_id,
            )?);
            policies.push('\n');
        }
        Ok(policies)
    }

    async fn load_directory_ancestry(
        &self,
        requested: &RequestedPermissionResource,
    ) -> Result<std::collections::BTreeSet<String>, LixError> {
        let mut next = match requested {
            RequestedPermissionResource::Directory { directory_id } => {
                Some(directory_id.clone())
            }
            RequestedPermissionResource::File { file_id }
            | RequestedPermissionResource::Table { file_id, .. }
            | RequestedPermissionResource::Row { file_id, .. } => {
                let result = self
                    .execute(
                        "SELECT directory_id FROM lix_file WHERE id = $1",
                        &[Value::Text(file_id.clone())],
                    )
                    .await?;
                match result.rows().first() {
                    Some(row) => nullable_text(&result, row, "directory_id")?,
                    None => None,
                }
            }
            RequestedPermissionResource::Repository
            | RequestedPermissionResource::Other => None,
        };
        let mut ancestry = std::collections::BTreeSet::new();
        while let Some(directory_id) = next {
            if !ancestry.insert(directory_id.clone()) {
                return Err(invalid_document("directory permission ancestry contains a cycle"));
            }
            let result = self
                .execute(
                    "SELECT parent_id FROM lix_directory WHERE id = $1",
                    &[Value::Text(directory_id)],
                )
                .await?;
            next = match result.rows().first() {
                Some(row) => nullable_text(&result, row, "parent_id")?,
                None => None,
            };
        }
        Ok(ancestry)
    }
}

#[derive(Debug)]
struct PermissionGrant {
    principal_type: String,
    principal_id: Option<String>,
    access_level: String,
    resource: GrantPermissionResource,
}

impl PermissionGrant {
    fn from_result(result: &super::ExecuteResult, row: &crate::Row) -> Result<Self, LixError> {
        let resource_type = required_text(result, row, "resource_type")?;
        let directory_id = nullable_text(result, row, "directory_id")?;
        let file_id = nullable_text(result, row, "file_id")?;
        let schema_key = nullable_text(result, row, "schema_key")?;
        let row_pk = nullable_json(result, row, "row_pk")?;
        let resource = match resource_type.as_str() {
            "repository" => GrantPermissionResource::Repository,
            "directory" => GrantPermissionResource::Directory {
                directory_id: directory_id.ok_or_else(|| {
                    invalid_document("directory permission grant has no directory_id")
                })?,
            },
            "file" => GrantPermissionResource::File {
                file_id: file_id
                    .ok_or_else(|| invalid_document("file permission grant has no file_id"))?,
            },
            "table" => GrantPermissionResource::Table {
                file_id: file_id
                    .ok_or_else(|| invalid_document("table permission grant has no file_id"))?,
                schema_key: schema_key.ok_or_else(|| {
                    invalid_document("table permission grant has no schema_key")
                })?,
            },
            "row" => GrantPermissionResource::Row {
                file_id: file_id
                    .ok_or_else(|| invalid_document("row permission grant has no file_id"))?,
                schema_key: schema_key
                    .ok_or_else(|| invalid_document("row permission grant has no schema_key"))?,
                row_pk: row_pk
                    .ok_or_else(|| invalid_document("row permission grant has no row_pk"))?,
            },
            other => {
                return Err(invalid_document(format!(
                    "permission grant has unknown resource_type '{other}'"
                )));
            }
        };
        Ok(Self {
            principal_type: required_text(result, row, "principal_type")?,
            principal_id: nullable_text(result, row, "principal_id")?,
            access_level: required_text(result, row, "access_level")?,
            resource,
        })
    }
}

#[derive(Debug)]
enum GrantPermissionResource {
    Repository,
    Directory {
        directory_id: String,
    },
    File {
        file_id: String,
    },
    Table {
        file_id: String,
        schema_key: String,
    },
    Row {
        file_id: String,
        schema_key: String,
        row_pk: serde_json::Value,
    },
}

impl GrantPermissionResource {
    fn applies_to(
        &self,
        requested: &RequestedPermissionResource,
        directory_ancestry: &std::collections::BTreeSet<String>,
    ) -> bool {
        match self {
            Self::Repository => true,
            Self::Directory { directory_id } => directory_ancestry.contains(directory_id),
            Self::File { file_id } => requested.file_id() == Some(file_id.as_str()),
            Self::Table {
                file_id,
                schema_key,
            } => requested.table_identity() == Some((schema_key.as_str(), file_id.as_str())),
            Self::Row {
                file_id,
                schema_key,
                row_pk,
            } => {
                requested.row_identity()
                    == Some((schema_key.as_str(), file_id.as_str(), row_pk))
            }
        }
    }
}

#[derive(Debug)]
enum RequestedPermissionResource {
    Repository,
    Directory {
        directory_id: String,
    },
    File {
        file_id: String,
    },
    Table {
        schema_key: String,
        file_id: String,
    },
    Row {
        schema_key: String,
        file_id: String,
        row_pk: serde_json::Value,
    },
    Other,
}

impl RequestedPermissionResource {
    fn parse(resource_type: &str, resource_id: &str) -> Self {
        match resource_type {
            "Repository" if resource_id == "repository" => Self::Repository,
            "Directory" => Self::Directory {
                directory_id: resource_id.to_string(),
            },
            "File" => Self::File {
                file_id: resource_id.to_string(),
            },
            "Table" => parse_table_resource_id(resource_id)
                .map(|(schema_key, file_id)| Self::Table {
                    schema_key,
                    file_id,
                })
                .unwrap_or(Self::Other),
            "Row" => parse_row_resource_id(resource_id)
                .map(|(schema_key, file_id, row_pk)| Self::Row {
                    schema_key,
                    file_id,
                    row_pk,
                })
                .unwrap_or(Self::Other),
            _ => Self::Other,
        }
    }

    fn file_id(&self) -> Option<&str> {
        match self {
            Self::File { file_id } | Self::Table { file_id, .. } | Self::Row { file_id, .. } => {
                Some(file_id)
            }
            _ => None,
        }
    }

    fn table_identity(&self) -> Option<(&str, &str)> {
        match self {
            Self::Table {
                schema_key,
                file_id,
            }
            | Self::Row {
                schema_key,
                file_id,
                ..
            } => Some((schema_key, file_id)),
            _ => None,
        }
    }

    fn row_identity(&self) -> Option<(&str, &str, &serde_json::Value)> {
        match self {
            Self::Row {
                schema_key,
                file_id,
                row_pk,
            } => Some((schema_key, file_id, row_pk)),
            _ => None,
        }
    }
}

fn parse_table_resource_id(resource_id: &str) -> Option<(String, String)> {
    let serde_json::Value::Array(parts) = serde_json::from_str(resource_id).ok()? else {
        return None;
    };
    match parts.as_slice() {
        [serde_json::Value::String(schema_key), serde_json::Value::String(file_id)] => {
            Some((schema_key.clone(), file_id.clone()))
        }
        _ => None,
    }
}

fn parse_row_resource_id(resource_id: &str) -> Option<(String, String, serde_json::Value)> {
    let serde_json::Value::Array(parts) = serde_json::from_str(resource_id).ok()? else {
        return None;
    };
    match parts.as_slice() {
        [serde_json::Value::String(schema_key), serde_json::Value::String(file_id), row_pk] => {
            Some((schema_key.clone(), file_id.clone(), row_pk.clone()))
        }
        _ => None,
    }
}

fn default_access_level_allows(access_level: &str, action: &str) -> bool {
    let minimum = match action {
        "view" => 0,
        "comment" => 1,
        "create" => 2,
        "edit" | "delete" | "share" | "publish" => 3,
        "createBranch" | "merge" | "manageMembers" | "managePolicies" | "executeSql" => 4,
        _ => return false,
    };
    let actual = match access_level {
        "viewer" => 0,
        "commenter" => 1,
        "contributor" => 2,
        "editor" => 3,
        "manager" => 4,
        _ => return false,
    };
    actual >= minimum
}

fn required_text(
    result: &super::ExecuteResult,
    row: &crate::Row,
    column: &str,
) -> Result<String, LixError> {
    nullable_text(result, row, column)?
        .ok_or_else(|| invalid_document(format!("permission grant {column} is null")))
}

fn nullable_text(
    result: &super::ExecuteResult,
    row: &crate::Row,
    column: &str,
) -> Result<Option<String>, LixError> {
    match result.get(row, column) {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        _ => Err(invalid_document(format!(
            "permission grant {column} is not text or null"
        ))),
    }
}

fn nullable_json(
    result: &super::ExecuteResult,
    row: &crate::Row,
    column: &str,
) -> Result<Option<serde_json::Value>, LixError> {
    match result.get(row, column) {
        Some(Value::Jsonb(value)) => Ok(Some(value.to_value())),
        Some(Value::Null) => Ok(None),
        _ => Err(invalid_document(format!(
            "permission grant {column} is not JSON or null"
        ))),
    }
}

fn documents_from_projection(
    result: &super::ExecuteResult,
) -> Result<Option<OwnedAuthorizationDocuments>, LixError> {
    let mut schema = None;
    let mut entities = None;
    let mut policy_sources = Vec::new();
    for row in result.rows() {
        let Some(Value::Text(kind)) = result.get(row, "kind") else {
            return Err(invalid_document(
                "Cedar projection returned an invalid kind",
            ));
        };
        let Some(Value::Text(path)) = result.get(row, "path") else {
            return Err(invalid_document(
                "Cedar projection returned an invalid path",
            ));
        };
        let Some(Value::Text(source)) = result.get(row, "source") else {
            return Err(invalid_document(format!(
                "Cedar projection for {path} returned invalid source"
            )));
        };
        match kind.as_str() {
            "schema" => schema = Some(source.clone()),
            "entities" => entities = Some(source.clone()),
            "policy" => policy_sources.push((path.clone(), source.clone())),
            _ => {
                return Err(invalid_document(format!(
                    "Cedar projection for {path} returned unknown kind '{kind}'"
                )));
            }
        }
    }
    assemble_documents(schema, entities, policy_sources)
}

fn documents_from_files(
    result: &super::ExecuteResult,
) -> Result<Option<OwnedAuthorizationDocuments>, LixError> {
    let mut schema = None;
    let mut entities = None;
    let mut policy_sources = Vec::new();
    for row in result.rows() {
        let Some(Value::Text(path)) = result.get(row, "path") else {
            return Err(invalid_document(
                "authorization query returned an invalid path",
            ));
        };
        let source = match result.get(row, "content") {
            Some(Value::Blob(content)) => std::str::from_utf8(content.as_ref())
                .map_err(|error| invalid_document(format!("{path} is not UTF-8: {error}")))?
                .to_owned(),
            Some(Value::Null) => String::new(),
            _ => {
                return Err(invalid_document(format!(
                    "{path} did not contain file bytes"
                )));
            }
        };
        if path == SCHEMA_PATH {
            schema = Some(source);
        } else if path == ENTITIES_PATH {
            entities = Some(source);
        } else if path.starts_with(PERMISSIONS_DIRECTORY) && path.ends_with(".cedar") {
            policy_sources.push((path.clone(), source));
        }
    }
    assemble_documents(schema, entities, policy_sources)
}

fn assemble_documents(
    schema: Option<String>,
    entities: Option<String>,
    mut policy_sources: Vec<(String, String)>,
) -> Result<Option<OwnedAuthorizationDocuments>, LixError> {
    let Some(schema) = schema else {
        return Ok(None);
    };
    policy_sources.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut policies = String::new();
    for (path, source) in policy_sources {
        policies.push_str("// source: ");
        policies.push_str(&path);
        policies.push('\n');
        policies.push_str(&source);
        policies.push('\n');
    }
    Ok(Some(OwnedAuthorizationDocuments {
        schema,
        policies,
        entities,
    }))
}

#[derive(Debug)]
struct OwnedAuthorizationDocuments {
    schema: String,
    policies: String,
    entities: Option<String>,
}

fn invalid_document(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PERMISSION_POLICY, message)
}

#[cfg(test)]
mod tests {
    use crate::{ANONYMOUS_ACCOUNT_ID, LixError, Value, open_lix};

    const PUBLIC_ID: &str = "01920000-0000-7000-8000-000000000001";
    const PRIVATE_ID: &str = "01920000-0000-7000-8000-000000000002";

    #[tokio::test]
    async fn exact_file_reads_are_private_by_default_and_policy_driven() {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
             ($1, '/public.md', CAST('public' AS BYTEA)), \
             ($2, '/private.md', CAST('private' AS BYTEA)), \
             ($3, '/.lix/permissions/schema.cedarschema', CAST($4 AS BYTEA)), \
             ($5, '/.lix/permissions/publications.cedar', CAST($6 AS BYTEA))",
            &[
                Value::Text(PUBLIC_ID.into()),
                Value::Text(PRIVATE_ID.into()),
                Value::Text("01920000-0000-7000-8000-000000000003".into()),
                Value::Text(schema().into()),
                Value::Text("01920000-0000-7000-8000-000000000004".into()),
                Value::Text(publication(PUBLIC_ID).into()),
            ],
        )
        .await
        .unwrap();

        let public = lix.read_file_content("/public.md", None).await.unwrap();
        assert_eq!(public.unwrap().into_content().as_ref(), b"public");

        let denied = lix
            .read_file_content("/private.md", None)
            .await
            .unwrap_err();
        assert_eq!(denied.code, LixError::CODE_PERMISSION_DENIED);

        lix.execute(
            "UPDATE lix_file SET content = CAST('' AS BYTEA) \
             WHERE path = '/.lix/permissions/publications.cedar'",
            &[],
        )
        .await
        .unwrap();
        let denied = lix.read_file_content("/public.md", None).await.unwrap_err();
        assert_eq!(denied.code, LixError::CODE_PERMISSION_DENIED);
    }

    #[tokio::test]
    async fn global_permission_grants_drive_default_cedar_file_access() {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
             ($1, '/public.md', CAST('public' AS BYTEA)), \
             ($2, '/private.md', CAST('private' AS BYTEA)), \
             ($3, '/.lix/permissions/schema.cedarschema', CAST($4 AS BYTEA)), \
             ($5, '/.lix/permissions/default.cedar', CAST('' AS BYTEA))",
            &[
                Value::Text(PUBLIC_ID.into()),
                Value::Text(PRIVATE_ID.into()),
                Value::Text("01920000-0000-7000-8000-000000000013".into()),
                Value::Text(schema().into()),
                Value::Text("01920000-0000-7000-8000-000000000014".into()),
            ],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, access_level, resource_type, file_id, lixcol_global) \
             VALUES ('01920000-0000-7000-8000-000000000015', 'anonymous', 'viewer', 'file', $1, true)",
            &[Value::Text(PUBLIC_ID.into())],
        )
        .await
        .unwrap();

        let public = lix.read_file_content("/public.md", None).await.unwrap();
        assert_eq!(public.unwrap().into_content().as_ref(), b"public");
        let denied = lix
            .read_file_content("/private.md", None)
            .await
            .unwrap_err();
        assert_eq!(denied.code, LixError::CODE_PERMISSION_DENIED);

        lix.execute(
            "UPDATE lix_permission_grant SET resource_type = 'repository', file_id = NULL \
             WHERE id = '01920000-0000-7000-8000-000000000015'",
            &[],
        )
        .await
        .unwrap();
        let private = lix.read_file_content("/private.md", None).await.unwrap();
        assert_eq!(private.unwrap().into_content().as_ref(), b"private");
    }

    #[tokio::test]
    async fn directory_grants_cover_descendant_files() {
        let lix = open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES \
             ($1, '/shared/reports/q1.md', CAST('q1' AS BYTEA)), \
             ($2, '/outside.md', CAST('outside' AS BYTEA)), \
             ($3, '/.lix/permissions/schema.cedarschema', CAST($4 AS BYTEA))",
            &[
                Value::Text(PUBLIC_ID.into()),
                Value::Text(PRIVATE_ID.into()),
                Value::Text("01920000-0000-7000-8000-000000000023".into()),
                Value::Text(schema().into()),
            ],
        )
        .await
        .unwrap();
        let shared = lix
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/shared'",
                &[],
            )
            .await
            .unwrap();
        let Value::Text(shared_id) = &shared.rows()[0].values()[0] else {
            panic!("shared directory id should be text");
        };
        lix.execute(
            "INSERT INTO lix_permission_grant \
             (id, principal_type, access_level, resource_type, directory_id, lixcol_global) \
             VALUES ('01920000-0000-7000-8000-000000000025', 'anonymous', 'viewer', 'directory', $1, true)",
            &[Value::Text(shared_id.clone())],
        )
        .await
        .unwrap();

        let q1 = lix
            .read_file_content("/shared/reports/q1.md", None)
            .await
            .unwrap();
        assert_eq!(q1.unwrap().into_content().as_ref(), b"q1");
        let outside = lix
            .read_file_content("/outside.md", None)
            .await
            .unwrap_err();
        assert_eq!(outside.code, LixError::CODE_PERMISSION_DENIED);
    }

    fn schema() -> &'static str {
        r#"
            entity Account;
            entity File;
            entity Repository;
            action "view" appliesTo { principal: Account, resource: [File, Repository] };
            action "share" appliesTo { principal: Account, resource: File };
            action "managePolicies" appliesTo { principal: Account, resource: Repository };
            action "executeSql" appliesTo { principal: Account, resource: Repository };
        "#
    }

    fn publication(file_id: &str) -> String {
        format!(
            r#"permit (
                principal == Account::"{ANONYMOUS_ACCOUNT_ID}",
                action == Action::"view",
                resource == File::"{file_id}"
            );"#
        )
    }
}
