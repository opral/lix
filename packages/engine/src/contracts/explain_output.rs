use std::time::Duration;

use crate::contracts::{PreparedAnalyzedRuntime, PreparedExplainTemplate};
use crate::{LixError, QueryResult, Value};

pub fn render_plain_explain_result(
    template: &PreparedExplainTemplate,
) -> Result<QueryResult, LixError> {
    match template {
        PreparedExplainTemplate::Text { sections } => Ok(text_query_result(sections.clone())),
        PreparedExplainTemplate::Json { base_json } => Ok(json_query_result(base_json.clone())),
    }
}

pub fn render_analyzed_explain_result(
    template: &PreparedExplainTemplate,
    result: &QueryResult,
    execution_duration: Duration,
) -> Result<QueryResult, LixError> {
    let runtime = PreparedAnalyzedRuntime {
        execution_duration_us: execution_duration.as_micros().min(u64::MAX as u128) as u64,
        output_row_count: result.rows.len(),
        output_column_count: result.columns.len(),
        output_columns: result.columns.clone(),
    };

    match template {
        PreparedExplainTemplate::Text { sections } => {
            let mut rows = sections
                .iter()
                .map(|(key, value)| vec![Value::Text(key.clone()), Value::Text(value.clone())])
                .collect::<Vec<_>>();
            rows.push(vec![
                Value::Text("analyzed_runtime".to_string()),
                Value::Text(format!(
                    "execution_duration_us: {}\noutput_row_count: {}\noutput_column_count: {}\noutput_columns: {}",
                    runtime.execution_duration_us,
                    runtime.output_row_count,
                    runtime.output_column_count,
                    runtime.output_columns.join(", "),
                )),
            ]);
            Ok(QueryResult {
                columns: vec!["explain_key".to_string(), "explain_value".to_string()],
                rows,
            })
        }
        PreparedExplainTemplate::Json { base_json } => {
            let mut json = base_json.clone();
            let Some(object) = json.as_object_mut() else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "prepared explain template expected a JSON object",
                ));
            };
            object.insert(
                "analyzed_runtime".to_string(),
                serde_json::to_value(runtime).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("failed to serialize analyzed explain runtime: {error}"),
                    )
                })?,
            );
            Ok(json_query_result(json))
        }
    }
}

fn text_query_result(sections: Vec<(String, String)>) -> QueryResult {
    let rows = sections
        .into_iter()
        .map(|(key, value)| vec![Value::Text(key), Value::Text(value)])
        .collect();
    QueryResult {
        columns: vec!["explain_key".to_string(), "explain_value".to_string()],
        rows,
    }
}

fn json_query_result(json: serde_json::Value) -> QueryResult {
    QueryResult {
        columns: vec!["explain_json".to_string()],
        rows: vec![vec![Value::Json(json)]],
    }
}
