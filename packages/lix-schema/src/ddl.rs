use serde_json::Value;

use crate::{DataType, Error, ErrorKind, Schema};

/// Compile Schema v1 to executable PostgreSQL `CREATE TABLE` DDL.
///
/// Keeping this compiler in the defining crate makes PostgreSQL compatibility
/// testable instead of relying on a resemblance between two JSON formats.
pub fn to_postgres_ddl(schema: &Schema) -> Result<String, Error> {
    schema.validate()?;
    let mut declarations = Vec::new();
    for column in &schema.columns {
        let mut declaration = format!("  {} {}", column.name, column.data_type.postgres_name());
        if !column.nullable {
            declaration.push_str(" NOT NULL");
        }
        if let Some(value) = &column.default_value {
            declaration.push_str(" DEFAULT ");
            declaration.push_str(&postgres_literal(column.data_type, value)?);
        } else if let Some(expression) = &column.default_expression {
            declaration.push_str(" DEFAULT ");
            declaration.push_str(expression);
        }
        declarations.push(declaration);
    }
    declarations.push(format!("  PRIMARY KEY ({})", schema.primary_key.join(", ")));
    declarations.extend(
        schema
            .unique
            .iter()
            .map(|columns| format!("  UNIQUE ({})", columns.join(", "))),
    );
    declarations.extend(schema.foreign_keys.iter().map(|foreign_key| {
        format!(
            "  FOREIGN KEY ({}) REFERENCES {} ({})",
            foreign_key.columns.join(", "),
            foreign_key.references.schema_key,
            foreign_key.references.columns.join(", ")
        )
    }));
    Ok(format!(
        "CREATE TABLE {} (\n{}\n);",
        schema.key,
        declarations.join(",\n")
    ))
}

fn postgres_literal(data_type: DataType, value: &Value) -> Result<String, Error> {
    if value.is_null() {
        return Ok("NULL".to_string());
    }
    match data_type {
        DataType::Text | DataType::Uuid => value
            .as_str()
            .map(quote_string)
            .ok_or_else(|| ddl_error("string default became invalid after validation")),
        DataType::Int8 | DataType::Float8 => Ok(value.to_string()),
        DataType::Boolean => Ok(if value.as_bool() == Some(true) {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        }),
        DataType::Jsonb => Ok(format!("{}::jsonb", quote_string(&value.to_string()))),
        DataType::Timestamptz => value
            .as_str()
            .map(|value| format!("{}::timestamptz", quote_string(value)))
            .ok_or_else(|| ddl_error("timestamptz default became invalid after validation")),
    }
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn ddl_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Serialization, "$", message)
}

#[cfg(test)]
mod tests {
    use crate::{from_json, to_postgres_ddl};

    #[test]
    fn emits_postgresql_types_constraints_and_defaults() {
        let schema = from_json(
            r#"{
              "$schema":"https://lix.dev/schema-v1.json",
              "key":"acme_note",
              "columns":[
                {"name":"id","type":"uuid","nullable":false,"default_expression":"uuidv7()"},
                {"name":"title","type":"text","nullable":false,"default_value":"it's ready"},
                {"name":"payload","type":"jsonb","nullable":false,"default_value":{"a":1}}
              ],
              "primary_key":["id"],
              "unique":[["title"]]
            }"#,
        )
        .unwrap();
        let ddl = to_postgres_ddl(&schema).unwrap();
        assert!(ddl.contains("id uuid NOT NULL DEFAULT uuidv7()"));
        assert!(ddl.contains("title text NOT NULL DEFAULT 'it''s ready'"));
        assert!(ddl.contains("payload jsonb NOT NULL DEFAULT '{\"a\":1}'::jsonb"));
        assert!(ddl.contains("PRIMARY KEY (id)"));
        assert!(ddl.contains("UNIQUE (title)"));
    }
}
