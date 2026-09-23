use super::assert_rows_eq;
use lix::Value;

simulation_test!(
    array_quantifiers_use_datafusion_native_planning,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for (expression, expected) in [
            ("2 > ANY(ARRAY[1, 3])", Value::Boolean(true)),
            ("2 = ALL(ARRAY[2, 2])", Value::Boolean(true)),
            ("2 = ALL(ARRAY[2, 3])", Value::Boolean(false)),
            ("array_has(ARRAY[1, NULL], 1)", Value::Boolean(true)),
        ] {
            let rows = session
                .execute(&format!("SELECT {expression}"), &[])
                .await
                .unwrap_or_else(|error| panic!("{expression}: {error:?}"));
            assert_rows_eq(rows, vec![vec![expected]]);
        }

        // DataFusion plans non-equality ANY(array); Lix must not impose a
        // private equality-only whitelist on a construct its SQL planner owns.
        let result = session
            .execute("SELECT 1 > ANY(ARRAY[NULL::BIGINT])", &[])
            .await
            .unwrap();
        assert_eq!(result.rows().len(), 1);
    }
);

simulation_test!(
    logical_value_equality_all_uses_datafusion_quantifier_semantics,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        for (expression, expected) in [
            ("'1'::jsonb = ALL(ARRAY['1'::jsonb])", Value::Boolean(true)),
            (
                "'{\"a\":1.00,\"b\":2}'::jsonb = ALL(ARRAY['{\"b\":2.0,\"a\":1}'::jsonb])",
                Value::Boolean(true),
            ),
            ("'1'::jsonb = ALL(ARRAY['1'::jsonb, NULL::jsonb])", Value::Null),
            (
                "'1'::jsonb = ALL(ARRAY['2'::jsonb, NULL::jsonb])",
                Value::Boolean(false),
            ),
            (
                "NULL::jsonb = ALL(array_slice(ARRAY['1'::jsonb], 1, 0))",
                Value::Boolean(true),
            ),
            ("NULL::jsonb = ALL(ARRAY['1'::jsonb])", Value::Null),
            (
                "'1'::jsonb = ALL(array_slice(ARRAY['1'::jsonb], 1, 0))",
                Value::Boolean(true),
            ),
        ] {
            let rows = session
                .execute(&format!("SELECT {expression}"), &[])
                .await
                .unwrap_or_else(|error| panic!("{expression}: {error:?}"));
            assert_rows_eq(rows, vec![vec![expected]]);
        }

        let matching_ref = "lix_row_ref('lix_file', NULL, '01991b1d-6d8b-7000-8000-0000000000f1')";
        let distinct_ref = "lix_row_ref('lix_file', NULL, '01991b1d-6d8b-7000-8000-0000000000f2')";
        for (expression, expected) in [
            (
                format!("{matching_ref} = ALL(ARRAY[{matching_ref}])"),
                Value::Boolean(true),
            ),
            (
                format!("{matching_ref} = ALL(ARRAY[{matching_ref}, {distinct_ref}])"),
                Value::Boolean(false),
            ),
        ] {
            let rows = session
                .execute(&format!("SELECT {expression}"), &[])
                .await
                .unwrap_or_else(|error| panic!("{expression}: {error:?}"));
            assert_rows_eq(rows, vec![vec![expected]]);
        }

        for expression in [
            "array_min(ARRAY['1'::jsonb])",
            "array_max(ARRAY[lix_row_ref('lix_file', NULL, '01991b1d-6d8b-7000-8000-0000000000f1')])",
            "CASE WHEN NOT (array_min(a) = '1'::jsonb AND array_max(a) = '1'::jsonb) THEN false ELSE array_min(a) = '1'::jsonb END FROM (SELECT ARRAY['1'::jsonb, '2'::jsonb] AS a) q",
        ] {
            let error = session
                .execute(&format!("SELECT {expression}"), &[])
                .await
                .expect_err(expression);
            assert_eq!(error.code, lix::LixError::CODE_UNSUPPORTED_SQL);
        }
    }
);
