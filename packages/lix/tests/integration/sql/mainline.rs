use lix::Value;

#[tokio::test]
async fn mainline_checkpoint_history_uses_actual_endpoints_and_keeps_empty_log_entries() {
    let lix = crate::open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('demo', 'one')",
        &[],
    )
    .await
    .unwrap();
    let checkpoint = lix.create_checkpoint().await.unwrap().commit_id;
    let empty = lix.create_checkpoint().await.unwrap().commit_id;
    let log = lix.execute("SELECT commit_id, parent_commit_id, is_checkpoint, position FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 2", &[]).await.unwrap();
    assert_eq!(log.len(), 2);
    assert_eq!(log.rows()[0].get::<String>("commit_id").unwrap(), empty);
    assert_eq!(
        log.rows()[0].get::<String>("parent_commit_id").unwrap(),
        checkpoint
    );
    let history = lix.execute("SELECT key, diff_type, lixcol_to_commit_id FROM lix_history('lix_key_value') WHERE key = 'demo' AND lixcol_commit_is_checkpoint ORDER BY lixcol_position", &[]).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(
        history.rows()[0]
            .get::<String>("lixcol_to_commit_id")
            .unwrap(),
        checkpoint
    );
    assert_eq!(
        history.rows()[0].get::<String>("diff_type").unwrap(),
        "added"
    );
    let page = lix.execute("WITH page AS (SELECT commit_id, position FROM lix_log($1) WHERE is_checkpoint ORDER BY position LIMIT 2) SELECT p.commit_id, h.key FROM page p LEFT JOIN lix_history('lix_key_value', $1) h ON h.lixcol_to_commit_id = p.commit_id ORDER BY p.position", &[Value::Text(empty)]).await.unwrap();
    assert_eq!(page.len(), 2);
    let state = lix
        .execute(
            "SELECT value FROM lix_as_of('lix_key_value', $1) WHERE key = 'demo'",
            &[Value::Text(checkpoint)],
        )
        .await
        .unwrap();
    assert_eq!(
        state.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("one")
    );
    lix.close().await.unwrap();
}

#[tokio::test]
async fn mainline_working_context_and_global_checkpoint_flag() {
    let lix = crate::open_lix().await.unwrap();
    let initial = lix.execute("SELECT commit_id, working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()", &[]).await.unwrap();
    assert_eq!(
        initial.rows()[0].get::<String>("commit_id").unwrap(),
        initial.rows()[0]
            .get::<String>("working_base_commit_id")
            .unwrap()
    );
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('demo', 'one')",
        &[],
    )
    .await
    .unwrap();
    let diff = lix.execute("SELECT lixcol_from_commit_id, lixcol_to_commit_id FROM lix_diff('lix_key_value') WHERE key = 'demo'", &[]).await.unwrap();
    assert_eq!(
        diff.rows()[0]
            .get::<String>("lixcol_from_commit_id")
            .unwrap(),
        initial.rows()[0]
            .get::<String>("working_base_commit_id")
            .unwrap()
    );
    let c = lix.create_checkpoint().await.unwrap().commit_id;
    let commits = lix
        .execute(
            "SELECT id FROM lix_commit WHERE is_checkpoint AND id = $1",
            &[Value::Text(c.clone())],
        )
        .await
        .unwrap();
    assert_eq!(commits.rows()[0].get::<String>("id").unwrap(), c);
    assert!(
        lix.execute("SELECT * FROM lix_checkpoint", &[])
            .await
            .is_err()
    );
    assert!(
        lix.execute(
            "SELECT * FROM lix_state_at('lix_file', $1)",
            &[Value::Text(c)]
        )
        .await
        .is_err()
    );
    lix.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn mainline_page_work_is_independent_of_older_retained_history() {
    let lix = crate::open_lix().await.unwrap();
    let mut ids = Vec::new();
    for index in 0..60 {
        lix.execute("INSERT INTO lix_key_value (key, value) VALUES ('profile', $1) ON CONFLICT (key) DO UPDATE SET value = excluded.value", &[Value::Text(index.to_string())]).await.unwrap();
        ids.push(lix.create_checkpoint().await.unwrap().commit_id);
        if ![9, 29, 59].contains(&index) {
            continue;
        }
        let anchor = ids.last().unwrap().clone();
        crate::sql2::take_mainline_work();
        let start = std::time::Instant::now();
        let log = lix
            .execute(
                "SELECT commit_id FROM lix_log($1) WHERE is_checkpoint ORDER BY position LIMIT 5",
                &[Value::Text(anchor.clone())],
            )
            .await
            .unwrap();
        let log_work = crate::sql2::take_mainline_work();
        assert_eq!(log.len(), 5);
        assert!(log_work.0 <= 6, "log must stop at its page: {log_work:?}");
        let page = lix.execute("WITH page AS (SELECT commit_id, position FROM lix_log($1) WHERE is_checkpoint ORDER BY position LIMIT 5) SELECT p.commit_id, h.key FROM page p LEFT JOIN lix_history('lix_key_value', $1) h ON h.lixcol_to_commit_id = p.commit_id ORDER BY p.position", &[Value::Text(anchor.clone())]).await.unwrap();
        let work = crate::sql2::take_mainline_work();
        assert_eq!(page.len(), 5);
        assert!(
            work.1 <= 5,
            "preview must diff only selected commits: {work:?}"
        );
        assert!(
            work.0 <= 12,
            "preview must not walk older history: {work:?}"
        );
        eprintln!(
            "mainline profile retained={} page=5 elapsed_us={} log_work={log_work:?} preview_work={work:?}",
            ids.len(),
            start.elapsed().as_micros()
        );
    }
    lix.close().await.unwrap();
}
