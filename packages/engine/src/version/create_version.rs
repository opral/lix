use crate::{errors, Engine, EngineTransaction, ExecuteOptions, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CreateVersionOptions {
    pub id: Option<String>,
    pub name: Option<String>,
    pub inherits_from_version_id: Option<String>,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateVersionResult {
    pub id: String,
    pub name: String,
    pub inherits_from_version_id: String,
}

pub async fn create_version(
    engine: &Engine,
    options: CreateVersionOptions,
) -> Result<CreateVersionResult, LixError> {
    engine
        .transaction(ExecuteOptions::default(), |tx| {
            Box::pin(async move { create_version_in_transaction(tx, options).await })
        })
        .await
}

async fn create_version_in_transaction(
    tx: &mut EngineTransaction<'_>,
    options: CreateVersionOptions,
) -> Result<CreateVersionResult, LixError> {
    let active_version = tx
        .execute(
            "SELECT av.version_id, v.commit_id \
             FROM lix_active_version av \
             JOIN lix_version v ON v.id = av.version_id \
             ORDER BY av.id \
             LIMIT 1",
            &[],
        )
        .await?;
    let [statement] = active_version.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "active version query",
            1,
            active_version.statements.len(),
        ));
    };
    let [row] = statement.rows.as_slice() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "missing active version row".to_string(),
        });
    };
    let active_version_id = text_at(row, 0, "active_version.version_id")?;
    let active_commit_id = text_at(row, 1, "lix_version.commit_id")?;

    let id =
        normalize_optional_non_empty_text(options.id, "id")?.unwrap_or(generate_uuid(tx).await?);
    let name = normalize_optional_non_empty_text(options.name, "name")?.unwrap_or(id.clone());
    let inherits_from_version_id = normalize_optional_non_empty_text(
        options.inherits_from_version_id,
        "inherits_from_version_id",
    )?
    .unwrap_or(active_version_id);
    let hidden = options.hidden;

    tx.execute(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id\
         ) VALUES ($1, $2, $3, $4, $5)",
        &[
            Value::Text(id.clone()),
            Value::Text(name.clone()),
            Value::Text(inherits_from_version_id.clone()),
            Value::Boolean(hidden),
            Value::Text(active_commit_id.clone()),
        ],
    )
    .await?;

    Ok(CreateVersionResult {
        id,
        name,
        inherits_from_version_id,
    })
}

async fn generate_uuid(tx: &mut EngineTransaction<'_>) -> Result<String, LixError> {
    let generated = tx.execute("SELECT lix_uuid_v7()", &[]).await?;
    let [statement] = generated.statements.as_slice() else {
        return Err(errors::unexpected_statement_count_error(
            "generated uuid query",
            1,
            generated.statements.len(),
        ));
    };
    let [row] = statement.rows.as_slice() else {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "missing generated uuid row".to_string(),
        });
    };
    text_at(row, 0, "lix_uuid_v7()")
}

fn text_at(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{field} is empty"),
        }),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text-like value for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("missing {field}"),
        }),
    }
}

fn normalize_optional_non_empty_text(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, LixError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{field} must be a non-empty string when provided"),
        }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::CreateVersionOptions;

    #[test]
    fn create_version_options_deserialize_defaults_hidden() {
        let options: CreateVersionOptions =
            serde_json::from_str(r#"{"id":"test-version"}"#).expect("deserialization should work");
        assert_eq!(options.id.as_deref(), Some("test-version"));
        assert!(!options.hidden);
    }
}
