//! Repository-owned Cedar policy evaluation.
//!
//! The canonical inputs are ordinary files under `/.lix/permissions`. This
//! module deliberately contains no host or Lixray policy: it only maps Lix's
//! stable principal/action/resource request into Cedar and evaluates the
//! repository's policy set.

use cedar_policy_core::{
    ast::{Context, EntityUID, EntityUIDEntry, Request},
    authorizer::{Authorizer, Decision},
    entities::{Entities, EntityJsonParser, NoEntitiesSchema, TCComputation},
    extensions::Extensions,
    parser,
};

use crate::LixError;

pub(crate) const PERMISSIONS_DIRECTORY: &str = "/.lix/permissions/";
pub(crate) const SCHEMA_PATH: &str = "/.lix/permissions/schema.cedarschema";
pub(crate) const ENTITIES_PATH: &str = "/.lix/permissions/entities.cedar.json";

/// Result of repository authorization. Repositories without a Cedar schema
/// are inactive so existing repositories retain their pre-prototype behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationDecision {
    Inactive,
    Allow,
    Deny,
}

#[derive(Debug)]
pub(crate) struct AuthorizationDocuments<'a> {
    pub schema: &'a str,
    pub policies: &'a str,
    pub entities: Option<&'a str>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AuthorizationRequest<'a> {
    pub principal_id: &'a str,
    pub action: &'a str,
    pub resource_type: &'a str,
    pub resource_id: &'a str,
}

pub(crate) fn authorize(
    documents: AuthorizationDocuments<'_>,
    input: AuthorizationRequest<'_>,
) -> Result<AuthorizationDecision, LixError> {
    // The schema file is the activation marker and is validated by the Cedar
    // projection plugin. Core intentionally links only the Cedar evaluator:
    // the full validator enables `serde_json/preserve_order` transitively and
    // changes Lix's canonical JSON representation through feature unification.
    let _schema = documents.schema;
    let policies = parser::parse_policyset(documents.policies)
        .map_err(|error| invalid_policy("policies", error))?;

    let entities = match documents.entities {
        Some(source) => EntityJsonParser::new(
            None::<&NoEntitiesSchema>,
            Extensions::all_available(),
            TCComputation::ComputeNow,
        )
        .from_json_str(source)
        .map_err(|error| invalid_policy("entities", error))?,
        None => Entities::new(),
    };
    let request = Request::new_unchecked(
        EntityUIDEntry::known(entity_uid("Account", input.principal_id)?, None),
        EntityUIDEntry::known(entity_uid("Action", input.action)?, None),
        EntityUIDEntry::known(entity_uid(input.resource_type, input.resource_id)?, None),
        Some(Context::empty()),
    );
    let response = Authorizer::new().is_authorized(request, &policies, &entities);
    Ok(match response.decision {
        Decision::Allow => AuthorizationDecision::Allow,
        Decision::Deny => AuthorizationDecision::Deny,
    })
}

fn entity_uid(entity_type: &str, id: &str) -> Result<EntityUID, LixError> {
    EntityUID::with_eid_and_type(entity_type, id)
        .map_err(|error| invalid_policy("entity_type", error))
}

fn invalid_policy(kind: &'static str, error: impl std::fmt::Display) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PERMISSION_POLICY,
        format!("invalid Cedar {kind}: {error}"),
    )
    .with_details(serde_json::json!({ "kind": kind }))
}

#[cfg(test)]
mod tests {
    use super::{AuthorizationDecision, AuthorizationDocuments, AuthorizationRequest, authorize};

    const SCHEMA: &str = r#"
        entity Account in [Team];
        entity Team;
        entity File;
        entity Repository;

        action "view" appliesTo { principal: Account, resource: [File, Repository] };
        action "share" appliesTo { principal: Account, resource: File };
        action "managePolicies" appliesTo { principal: Account, resource: Repository };
        action "executeSql" appliesTo { principal: Account, resource: Repository };
    "#;

    fn request<'a>(principal: &'a str, file: &'a str) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal_id: principal,
            action: "view",
            resource_type: "File",
            resource_id: file,
        }
    }

    #[test]
    fn defaults_to_deny_and_allows_an_explicit_publication() {
        let documents = AuthorizationDocuments {
            schema: SCHEMA,
            policies: r#"
                permit (
                    principal == Account::"00000000-0000-7000-8000-000000000002",
                    action == Action::"view",
                    resource == File::"01920000-0000-7000-8000-000000000001"
                );
            "#,
            entities: None,
        };
        assert_eq!(
            authorize(
                documents,
                request(
                    "00000000-0000-7000-8000-000000000002",
                    "01920000-0000-7000-8000-000000000001"
                )
            )
            .unwrap(),
            AuthorizationDecision::Allow
        );

        let documents = AuthorizationDocuments {
            schema: SCHEMA,
            policies: "",
            entities: None,
        };
        assert_eq!(
            authorize(
                documents,
                request(
                    "00000000-0000-7000-8000-000000000002",
                    "01920000-0000-7000-8000-000000000001"
                )
            )
            .unwrap(),
            AuthorizationDecision::Deny
        );
    }

    #[test]
    fn company_defined_parent_relationship_changes_access_without_code() {
        let documents = AuthorizationDocuments {
            schema: SCHEMA,
            policies: r#"
                permit (
                    principal in Team::"reviewers",
                    action == Action::"view",
                    resource
                );
            "#,
            entities: Some(
                r#"[{
                    "uid": { "type": "Account", "id": "01920000-0000-7000-8000-0000000000aa" },
                    "attrs": {},
                    "parents": [{ "type": "Team", "id": "reviewers" }]
                }]"#,
            ),
        };
        assert_eq!(
            authorize(
                documents,
                request(
                    "01920000-0000-7000-8000-0000000000aa",
                    "01920000-0000-7000-8000-000000000001"
                )
            )
            .unwrap(),
            AuthorizationDecision::Allow
        );
    }
}
