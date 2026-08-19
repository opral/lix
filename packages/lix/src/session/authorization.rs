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
        let Some(documents) = self.load_authorization_documents().await? else {
            return Ok(AuthorizationDecision::Inactive);
        };
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
