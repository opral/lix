use super::*;

#[tokio::test]
async fn explicit_transaction_hydrates_native_inputs_without_moving_its_snapshot() {
    explicit_transaction_hydration(false).await;
}

#[tokio::test]
async fn explicit_transaction_hydrates_cold_write_and_commits_once() {
    explicit_transaction_hydration(true).await;
}

async fn explicit_transaction_hydration(commit: bool) {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let (authority, engine, session, old) = publication::fixture().await;
        let storage = engine.storage();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::sync::SyncDemand>(4);
        let calls = async {
            let mut retry = crate::sync::SyncDemandRetry::default();
            let opened = loop {
                match session.begin_transaction().await {
                    Ok(transaction) => break transaction,
                    Err(error) => retry.hydrate_for_retry(Some(&sender), error).await.unwrap(),
                }
            };
            let mut transaction = opened.with_sync_demand_sender(Some(sender));
            let result = transaction
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap();
            assert_eq!(
                result.rows()[0].get::<serde_json::Value>("value").unwrap(),
                "before"
            );
            if !commit {
                let outside = engine
                    .open_session_at_with_account(
                        old.descriptor().selected_branch.branch_id.clone(),
                        old.active_account_id().to_owned(),
                    )
                    .await
                    .unwrap();
                execute_hydrating(
                    &outside,
                    &storage,
                    &old,
                    &authority,
                    "UPDATE lix_key_value SET value='outside' WHERE key='resident'",
                    &[],
                    &mut Fetches::default(),
                )
                .await
                .unwrap();
            }
            let pinned = transaction
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap();
            assert_eq!(
                pinned.rows()[0].get::<serde_json::Value>("value").unwrap(),
                "before",
                "hydration must not advance the transaction's mutable snapshot"
            );
            transaction
                .execute(
                    "UPDATE lix_key_value SET value='staged' WHERE key='resident'",
                    &[],
                )
                .await
                .unwrap();
            let result = transaction
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap();
            assert_eq!(
                result.rows()[0].get::<serde_json::Value>("value").unwrap(),
                "staged"
            );
            if commit {
                transaction
                    .commit()
                    .await
                    .expect("cold explicit write commits after hydration");
            } else {
                transaction.rollback().await.unwrap();
            }
        };
        let hydrate = async {
            let mut count = 0;
            while let Some(demand) = receiver.recv().await {
                use crate::sync::runtime::SyncDemandRequest;
                let request = match demand.request {
                    SyncDemandRequest::Pinned(request) => *request,
                    request => request,
                };
                let objects = match request {
                    SyncDemandRequest::NativeObject(address, _) => vec![address],
                    SyncDemandRequest::NativeObjects(addresses, _) => addresses,
                    SyncDemandRequest::NativeMetadata(addresses, _) => {
                        for address in addresses {
                            hydrate_metadata(
                                &storage,
                                &old,
                                &authority,
                                address,
                                &mut Fetches::default(),
                            )
                            .await
                            .unwrap();
                        }
                        count += 1;
                        demand.response.send(Ok(())).unwrap();
                        continue;
                    }
                    _ => panic!("explicit transaction only requests pinned native inputs"),
                };
                for address in objects {
                    hydrate_native_object(&storage, &old, address, 32 * 1024 * 1024, |request| {
                        let authority = &authority;
                        async move { authority.read_sync_native_object_range(&request).await }
                    })
                    .await
                    .unwrap();
                }
                count += 1;
                demand.response.send(Ok(())).unwrap();
            }
            assert!(
                count > 0,
                "cold explicit SQL must fetch through its internal demand handler"
            );
        };
        futures_util::join!(calls, hydrate);
        assert_eq!(
            execute_hydrating(
                &session,
                &storage,
                &old,
                &authority,
                "SELECT value FROM lix_key_value WHERE key='resident'",
                &[],
                &mut Fetches::default()
            )
            .await
            .unwrap()
            .rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            if commit { "staged" } else { "outside" }
        );
    })
    .await
    .expect("cold explicit transaction must complete");
}

#[tokio::test]
#[ignore = "manual warm explicit transaction profile"]
async fn profile_warm_partial_explicit_transaction() {
    for enabled in [false, true] {
        let (authority, engine, session, old) = publication::fixture().await;
        execute_hydrating(
            &session,
            &engine.storage(),
            &old,
            &authority,
            "UPDATE lix_key_value SET value='warm' WHERE key='resident'",
            &[],
            &mut Fetches::default(),
        )
        .await
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::sync::SyncDemand>(4);
        let mut transaction = session
            .begin_transaction()
            .await
            .unwrap()
            .with_sync_demand_sender(if enabled { Some(sender) } else { None });
        for _ in 0..10 {
            transaction
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap();
            transaction
                .execute(
                    "UPDATE lix_key_value SET value='warm' WHERE key='resident'",
                    &[],
                )
                .await
                .unwrap();
        }
        let started = Instant::now();
        for _ in 0..100 {
            transaction
                .execute("SELECT value FROM lix_key_value WHERE key='resident'", &[])
                .await
                .unwrap();
        }
        let select_us = started.elapsed().as_micros();
        let started = Instant::now();
        for index in 0..100 {
            transaction
                .execute(
                    "UPDATE lix_key_value SET value=$1 WHERE key='resident'",
                    &[Value::Text(format!("warm-{index}"))],
                )
                .await
                .unwrap();
        }
        let update_us = started.elapsed().as_micros();
        assert!(
            receiver.try_recv().is_err(),
            "warm transaction SQL must never demand network inputs"
        );
        transaction.rollback().await.unwrap();
        eprintln!(
            "{}",
            serde_json::json!({"profile":"warm_partial_explicit_transaction", "enabled":enabled, "iterations":100,"select_us":select_us,"update_us":update_us})
        );
    }
}
