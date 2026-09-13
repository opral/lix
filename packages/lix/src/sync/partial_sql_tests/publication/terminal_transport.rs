use super::*;
use crate::sync::SyncTransportFuture;
use crate::sync::http::{HttpSyncTransport, RawHttpClient, RawHttpRequest, RawHttpResponse};
use crate::sync::runtime::SyncShutdown;

#[derive(Clone)]
struct UnusedClient;

impl RawHttpClient for UnusedClient {
    fn send(&self, _: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        panic!("incompatible handshake must not produce an admitted transport")
    }
}

#[tokio::test]
async fn terminal_sync_mismatch_blocks_warm_sql_and_preserves_pending_edits_on_reopen() {
    let mismatch = crate::sync::sync_server_protocol_mismatch(Some(13));
    assert!(mismatch.message.contains("upgrade"));
    terminal_error_preserves_pending_edits(mismatch).await;
}

#[tokio::test]
async fn terminal_missing_incorporation_proof_preserves_pending_edits_on_reopen() {
    terminal_error_preserves_pending_edits(
        LixError::new(
            "LIX_PARTIAL_MERGE_PROOF_UNAVAILABLE",
            "legacy checkpoint incorporation provenance is unavailable",
        )
        .with_hint("retain replica storage; reconciliation needs a provable incorporation path, and fetching row payloads alone cannot restore erased legacy provenance"),
    )
    .await;
}

async fn terminal_error_preserves_pending_edits(mismatch: LixError) {
    let (authority, engine, session, state) = fixture().await;
    let storage = engine.storage();
    let select = "SELECT value FROM lix_key_value WHERE key='resident'";
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        select,
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();
    execute_hydrating(
        &session,
        &storage,
        &state,
        &authority,
        "UPDATE lix_key_value SET value='pending' WHERE key='resident'",
        &[],
        &mut Fetches::default(),
    )
    .await
    .unwrap();

    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
    let (_demand, demand_rx) = tokio::sync::mpsc::channel(1);
    let expected = mismatch.clone();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::sync::partial_runtime::run_partial_worker_with_engine(
            storage.clone(),
            state.clone(),
            None::<HttpSyncTransport<UnusedClient>>,
            move || {
                let error = mismatch.clone();
                Box::pin(async move { Err(error) })
            },
            shutdown_rx,
            demand_rx,
            Some(engine.sync_mode().change_watcher()),
            Some(engine.clone()),
        ),
    )
    .await
    .expect("terminal mismatch must stop instead of retrying")
    .unwrap_err();
    assert_eq!(result.code, expected.code);
    for sql in [
        select,
        "UPDATE lix_key_value SET value='blocked' WHERE key='resident'",
    ] {
        let error = session.execute(sql, &[]).await.unwrap_err();
        assert_eq!(error.code, expected.code);
        assert_eq!(error.message, expected.message);
        assert_eq!(error.hint, expected.hint);
    }

    drop(session);
    drop(engine);
    let (reopened, fresh) = Engine::new_partial_replica(storage, EngineOptions::new(), &state)
        .await
        .unwrap();
    reopened
        .sync_mode()
        .admit_partial_replica(state, crate::sync::partial_replica_write_capability());
    assert!(value(fresh.execute(select, &[]).await.unwrap()).contains("pending"));
    assert!(value(authority.execute(select, &[]).await.unwrap()).contains("before"));
}
