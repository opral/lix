use std::{any::Any, sync::Arc};

use datafusion::arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixOrderBetween {
    signature: Signature,
}

impl LixOrderBetween {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LixOrderBetween {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &'static str {
        "lix_order_between"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "lix_order_between requires two arguments".into(),
            ));
        }
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let bounds = arrays
            .iter()
            .map(|array| {
                array.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("lix_order_between requires text bounds".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = (0..bounds[0].len())
            .map(|row| {
                let previous = (!bounds[0].is_null(row)).then(|| bounds[0].value(row));
                let next = (!bounds[1].is_null(row)).then(|| bounds[1].value(row));
                crate::plugin::runtime::order_between(previous, next).map_err(|error| {
                    DataFusionError::Execution(format!("lix_order_between: {error}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                keys.into_iter().next(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(keys))))
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn sql_order_between_works_in_public_bound_writes() {
        let lix = crate::open_lix().await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES (lix_order_between(NULL, NULL), CAST('true' AS JSONB))",
            &[],
        ).await.unwrap();
        let rows = lix
            .execute("SELECT key FROM lix_key_value WHERE key = '80'", &[])
            .await
            .unwrap();
        assert_eq!(rows.rows()[0].values(), &[crate::Value::Text("80".into())]);
        let updated = lix.execute(
            "UPDATE lix_key_value SET value = CAST('false' AS JSONB) WHERE key = lix_order_between($1, $2) RETURNING lix_order_between(key, NULL) AS next_key",
            &[crate::Value::Null, crate::Value::Null],
        ).await.unwrap();
        assert_eq!(
            updated.rows()[0].values(),
            &[crate::Value::Text(
                crate::plugin::runtime::order_between(Some("80"), None).unwrap()
            )]
        );
        let invalid = lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES (lix_order_between($1, $2), CAST('true' AS JSONB))",
            &[crate::Value::Text("c0".into()), crate::Value::Text("80".into())],
        ).await.unwrap_err();
        assert_eq!(invalid.code, crate::LixError::CODE_INVALID_PARAM);
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ('/80', CAST('' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
        lix.execute(
            "UPDATE lix_file SET path = '/' || lix_order_between('80', NULL) WHERE path = '/80'",
            &[],
        )
        .await
        .unwrap();
        let rows = lix
            .execute(
                "SELECT path FROM lix_file WHERE path = $1",
                &[crate::Value::Text(format!(
                    "/{}",
                    crate::plugin::runtime::order_between(Some("80"), None).unwrap()
                ))],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.rows()[0].values(),
            &[crate::Value::Text(format!(
                "/{}",
                crate::plugin::runtime::order_between(Some("80"), None).unwrap()
            ))]
        );
    }

    #[tokio::test]
    async fn sql_order_between_supports_open_bounds_and_column_inputs() {
        let ctx = crate::sql2::session::new_sql_session_context();
        let batches = ctx.sql("SELECT lix_order_between(previous, following) FROM (VALUES (NULL, NULL), ('80', NULL), (NULL, '80'), ('80', 'c0')) AS bounds(previous, following)")
            .await.unwrap().collect().await.unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "80");
        assert!(values.value(1) > "80");
        assert!(values.value(2) < "80");
        assert!(values.value(3) > "80" && values.value(3) < "c0");
    }

    #[tokio::test]
    async fn sql_order_between_rejects_invalid_bounds_without_panicking() {
        let ctx = crate::sql2::session::new_sql_session_context();
        for sql in [
            "SELECT lix_order_between('c0', '80')",
            "SELECT lix_order_between('80', '80')",
            "SELECT lix_order_between('xyz', NULL)",
        ] {
            let result = match ctx.sql(sql).await {
                Ok(frame) => frame.collect().await.map(|_| ()),
                Err(error) => Err(error),
            };
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("lix_order_between")
            );
        }
        assert_eq!(
            super::super::test_support::single_text("SELECT lix_order_between(NULL, NULL)")
                .await
                .as_deref(),
            Some("80")
        );
    }
}
