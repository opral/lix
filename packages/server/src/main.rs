use anyhow::Context;
use lix_server::{Config, LixRuntimeManager, router, telemetry};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let telemetry = telemetry::init();

    let config = Config::from_env()?;
    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .with_context(|| format!("bind {}", config.bind_addr))?;
    let manager = LixRuntimeManager::new(&config, telemetry.lix_sink)?;
    let internal_token = config.internal_token.clone();
    let shutdown_manager = manager.clone();
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    let shutdown_task = tokio::spawn(async move {
        if shutdown_receiver.await.is_ok() {
            shutdown_manager.shutdown().await
        } else {
            Ok(())
        }
    });
    tracing::info!(
        addr = %config.bind_addr,
        max_open_lixes = config.max_open_lixes,
        internal_auth = internal_token.is_some(),
        "lix lix service ready"
    );
    let result = axum::serve(
        listener,
        router(
            manager,
            internal_token,
            config.protocol_timeout,
            telemetry.in_flight_sql,
        ),
    )
    .with_graceful_shutdown(async move {
        shutdown_signal().await;
        if shutdown_sender.send(()).is_err() {
            tracing::error!("Lix protocol shutdown task stopped before the shutdown signal");
        }
    })
    .await
    .context("serve HTTP");
    let shutdown_result = shutdown_task
        .await
        .context("join Lix protocol shutdown task")
        .and_then(|result| result.context("close Lix protocols during shutdown"));
    match tokio::task::spawn_blocking(move || telemetry.trace_provider.shutdown()).await {
        Ok(Err(error)) => tracing::warn!(%error, "failed to flush traces during shutdown"),
        Err(error) => tracing::warn!(%error, "failed to join trace shutdown task"),
        Ok(Ok(())) => {}
    }
    result?;
    shutdown_result
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
