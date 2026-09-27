use lix::{CreateBranchOptions, Value};

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
    let history = lix.execute("SELECT key, diff_type, lixcol_to_commit_id FROM lix_log() l JOIN lix_history('lix_key_value') h ON h.lixcol_to_commit_id = l.commit_id WHERE key = 'demo' AND l.is_checkpoint ORDER BY l.position", &[]).await.unwrap();
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
async fn mainline_working_context_and_checkpoint_log() {
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
            "SELECT commit_id FROM lix_log() WHERE is_checkpoint AND commit_id = $1",
            &[Value::Text(c.clone())],
        )
        .await
        .unwrap();
    assert_eq!(commits.rows()[0].get::<String>("commit_id").unwrap(), c);
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

#[tokio::test(flavor = "current_thread")]
async fn mainline_checkpoint_retirement_batches_count_windows_and_keeps_pages_lazy() {
    let lix = crate::open_lix().await.unwrap();
    crate::sql2::take_checkpoint_retirement_work();
    let baseline_nodes = lix
        .execute("SELECT COUNT(*) AS nodes FROM lix_log()", &[])
        .await
        .unwrap();
    let baseline_checkpoints = lix
        .execute(
            "SELECT COUNT(*) AS checkpoints FROM lix_log() WHERE is_checkpoint",
            &[],
        )
        .await
        .unwrap();
    let baseline_retirement_work = crate::sql2::take_checkpoint_retirement_work();
    let baseline_nodes = baseline_nodes.rows()[0].get::<i64>("nodes").unwrap() as usize;
    let baseline_checkpoints = baseline_checkpoints.rows()[0]
        .get::<i64>("checkpoints")
        .unwrap() as usize;
    let mut checkpoint_ids = Vec::new();
    for _ in 0..70 {
        checkpoint_ids.push(lix.create_checkpoint().await.unwrap().commit_id);
    }

    crate::sql2::take_checkpoint_retirement_work();
    crate::sql2::take_mainline_metadata_work();
    let expected_nodes = baseline_nodes + checkpoint_ids.len();
    let expected_checkpoints = baseline_checkpoints + checkpoint_ids.len();
    let expected_retirement_keys = baseline_retirement_work.1 + checkpoint_ids.len();
    let counted = lix
        .execute(
            "SELECT commit_id, position, count(*) OVER() AS total_count \
             FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 10",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(counted.len(), 10);
    for (index, row) in counted.rows().iter().enumerate() {
        assert_eq!(row.get::<i64>("position").unwrap(), index as i64);
        assert_eq!(
            row.get::<i64>("total_count").unwrap(),
            expected_checkpoints as i64
        );
        assert_eq!(
            row.get::<String>("commit_id").unwrap(),
            checkpoint_ids[checkpoint_ids.len() - 1 - index]
        );
    }
    let retirement_work = crate::sql2::take_checkpoint_retirement_work();
    let expected_windows = expected_nodes.div_ceil(64);
    assert_eq!(
        retirement_work,
        (expected_windows, expected_retirement_keys)
    );
    assert_eq!(
        crate::sql2::take_mainline_metadata_work(),
        (expected_windows, expected_nodes)
    );

    crate::sql2::take_checkpoint_retirement_work();
    crate::sql2::take_mainline_metadata_work();
    let page = lix
        .execute(
            "SELECT commit_id, position FROM lix_log() \
             WHERE is_checkpoint ORDER BY position LIMIT 5",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(page.len(), 5);
    for (index, row) in page.rows().iter().enumerate() {
        assert_eq!(row.get::<i64>("position").unwrap(), index as i64);
        assert_eq!(
            row.get::<String>("commit_id").unwrap(),
            checkpoint_ids[checkpoint_ids.len() - 1 - index]
        );
    }
    assert_eq!(crate::sql2::take_checkpoint_retirement_work(), (5, 5));
    assert_eq!(crate::sql2::take_mainline_metadata_work(), (5, 5));

    // A plain COUNT(*) asks DataFusion for no log columns. The zero-column
    // projection still needs to preserve the source's row count while using
    // the same bounded checkpoint windows.
    crate::sql2::take_checkpoint_retirement_work();
    crate::sql2::take_mainline_metadata_work();
    let plain_count = lix
        .execute(
            "SELECT COUNT(*) AS checkpoints FROM lix_log() WHERE is_checkpoint",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        plain_count.rows()[0].get::<i64>("checkpoints").unwrap(),
        expected_checkpoints as i64
    );
    assert_eq!(
        crate::sql2::take_checkpoint_retirement_work(),
        (expected_windows, expected_retirement_keys)
    );
    assert_eq!(
        crate::sql2::take_mainline_metadata_work(),
        (expected_windows, expected_nodes)
    );

    crate::sql2::take_checkpoint_retirement_work();
    crate::sql2::take_mainline_metadata_work();
    let empty = lix
        .execute(
            "SELECT is_checkpoint FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 0",
            &[],
        )
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(crate::sql2::take_checkpoint_retirement_work(), (0, 0));
    assert_eq!(crate::sql2::take_mainline_metadata_work(), (0, 0));
    lix.close().await.unwrap();
}

#[tokio::test]
async fn checkpoint_status_has_one_public_home_and_is_pinned_to_log_anchor() {
    let lix = crate::open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key,value) VALUES ('anchor-test','one')",
        &[],
    )
    .await
    .unwrap();
    let c = lix.create_checkpoint().await.unwrap().commit_id;
    let fork_branch = lix
        .create_branch(CreateBranchOptions {
            id: None,
            name: "checkpoint status fork".into(),
            from_commit_id: Some(c.clone()),
        })
        .await
        .unwrap();
    let fork = lix
        .open_another_session()
        .with_branch(fork_branch.id)
        .await
        .unwrap();
    let fork_undone = fork
        .execute(
            "SELECT commit_id FROM lix_undo($1)",
            &[Value::Text(c.clone())],
        )
        .await
        .unwrap();
    let fork_undo_anchor = fork_undone.rows()[0].get::<String>("commit_id").unwrap();
    let undone = lix
        .execute(
            "SELECT commit_id FROM lix_undo($1)",
            &[Value::Text(c.clone())],
        )
        .await
        .unwrap();
    let u = undone.rows()[0].get::<String>("commit_id").unwrap();
    let redone = lix
        .execute(
            "SELECT commit_id FROM lix_redo($1)",
            &[Value::Text(u.clone())],
        )
        .await
        .unwrap();
    let r = redone.rows()[0].get::<String>("commit_id").unwrap();
    for (anchor, active) in [(&c, true), (&u, false), (&r, true)] {
        let log = lix
            .execute(
                "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id=$2",
                &[Value::Text(anchor.clone()), Value::Text(c.clone())],
            )
            .await
            .unwrap();
        assert_eq!(log.rows()[0].get::<bool>("is_checkpoint").unwrap(), active);
        let history = lix.execute("SELECT h.diff_type, h.lixcol_to_commit_id FROM lix_log($1) l JOIN lix_history('lix_key_value',$1) h ON h.lixcol_to_commit_id=l.commit_id WHERE l.is_checkpoint AND h.key='anchor-test'", &[Value::Text(anchor.clone())]).await.unwrap();
        assert_eq!(history.len(), usize::from(active));
        if active {
            assert_eq!(
                history.rows()[0]
                    .get::<String>("lixcol_to_commit_id")
                    .unwrap(),
                c
            );
            assert_eq!(
                history.rows()[0].get::<String>("diff_type").unwrap(),
                "added"
            );
        }
    }
    let fork_log = lix
        .execute(
            "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id=$2",
            &[Value::Text(fork_undo_anchor), Value::Text(c.clone())],
        )
        .await
        .unwrap();
    assert!(!fork_log.rows()[0].get::<bool>("is_checkpoint").unwrap());
    for sql in [
        "SELECT is_checkpoint FROM lix_commit",
        "SELECT is_checkpoint_active FROM lix_log()",
        "SELECT lixcol_commit_is_checkpoint FROM lix_history('lix_key_value')",
    ] {
        assert!(
            lix.execute(sql, &[]).await.is_err(),
            "retired SQL must fail: {sql}"
        );
    }
    fork.close().await.unwrap();
    lix.close().await.unwrap();
}
