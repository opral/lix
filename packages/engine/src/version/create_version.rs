use crate::{Engine, EngineTransaction, ExecuteOptions, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct CreateVersionOptions {
    pub id: Option<String>,
    pub name: Option<String>,
    pub inherits_from_version_id: Option<String>,
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
    let row = first_row(&active_version, "active version row")?;
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
    let hidden = if options.hidden { 1 } else { 0 };
    let working_commit_id = generate_uuid(tx).await?;

    tx.execute(
        "INSERT INTO lix_version (\
         id, name, inherits_from_version_id, hidden, commit_id, working_commit_id\
         ) VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            Value::Text(id.clone()),
            Value::Text(name.clone()),
            Value::Text(inherits_from_version_id.clone()),
            Value::Integer(hidden),
            Value::Text(active_commit_id),
            Value::Text(working_commit_id),
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
    let row = first_row(&generated, "generated uuid")?;
    text_at(row, 0, "lix_uuid_v7()")
}

fn first_row<'a>(result: &'a QueryResult, context: &str) -> Result<&'a [Value], LixError> {
    result
        .rows
        .first()
        .map(std::vec::Vec::as_slice)
        .ok_or_else(|| LixError {
            message: format!("missing {context}"),
        })
}

fn text_at(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError {
            message: format!("{field} is empty"),
        }),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError {
            message: format!("expected text-like value for {field}, got {other:?}"),
        }),
        None => Err(LixError {
            message: format!("missing {field}"),
        }),
    }
}

fn normalize_optional_non_empty_text(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, LixError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(LixError {
            message: format!("{field} must be a non-empty string when provided"),
        }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}
