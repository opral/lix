//! Multiplex observation hub with recover-once on session death.

use super::http::{ProtocolHttp, ProtocolHttpRequest};
use super::sse::{SseReader, sse_protocol_error};
use super::wire::{
    MultiplexObserveError, MultiplexObserveEvent, MultiplexObserveRequest,
    MultiplexObserveSubscription, ObserveDelta,
};
use super::{
    ClientInner, RemoteExecuteOptions, decode_execute_response, is_recoverable_session_error,
    protocol_error, request_headers, with_status,
};
use crate::{ExecuteResult, LixError, ObserveEvent, Value, WireValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

const OBSERVE_RETRY_BASE_MS: u64 = 100;
const OBSERVE_RETRY_MAX_MS: u64 = 5_000;

type ObserveOutcome = Result<ObserveEvent, LixError>;

/// Latest-wins inbox: unread successful events are replaced, matching the JS
/// observation hub so a consumer that polls after a burst sees only the tail.
struct CoalescedOutcomes {
    items: Mutex<VecDeque<ObserveOutcome>>,
    terminal: Mutex<Option<LixError>>,
    notify: Notify,
}

impl CoalescedOutcomes {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            items: Mutex::new(VecDeque::new()),
            terminal: Mutex::new(None),
            notify: Notify::new(),
        })
    }

    async fn push_ok(&self, event: ObserveEvent) {
        if self.terminal.lock().await.is_some() {
            return;
        }
        let mut items = self.items.lock().await;
        items.retain(|item| item.is_err());
        items.push_back(Ok(event));
        self.notify.notify_waiters();
    }

    async fn push_recoverable_error(&self, error: LixError) {
        if self.terminal.lock().await.is_some() {
            return;
        }
        let mut items = self.items.lock().await;
        if items.iter().any(Result::is_err) {
            return;
        }
        items.push_back(Err(error));
        self.notify.notify_waiters();
    }

    async fn fail(&self, error: LixError) {
        let mut terminal = self.terminal.lock().await;
        if terminal.is_some() {
            return;
        }
        *terminal = Some(error);
        self.notify.notify_waiters();
    }

    async fn next(&self, closed: &AtomicBool) -> Result<Option<ObserveEvent>, LixError> {
        loop {
            if closed.load(Ordering::SeqCst) {
                return Ok(None);
            }
            {
                let mut items = self.items.lock().await;
                if let Some(item) = items.pop_front() {
                    return item.map(Some);
                }
                if let Some(error) = self.terminal.lock().await.clone() {
                    return Err(error);
                }
            }
            if closed.load(Ordering::SeqCst) {
                return Ok(None);
            }
            self.notify.notified().await;
        }
    }

    fn close(&self) {
        self.notify.notify_waiters();
    }
}

pub struct RemoteObserveEvents {
    outcomes: Arc<CoalescedOutcomes>,
    closed: Arc<AtomicBool>,
    on_close: Arc<dyn Fn() + Send + Sync>,
}

impl RemoteObserveEvents {
    pub async fn next(&self) -> Result<Option<ObserveEvent>, LixError> {
        self.outcomes.next(&self.closed).await
    }

    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.outcomes.close();
        (self.on_close)();
    }
}

impl Drop for RemoteObserveEvents {
    fn drop(&mut self) {
        self.close();
    }
}

pub(crate) struct ObservationHub {
    observations: Mutex<HashMap<String, ObservationState>>,
    next_id: AtomicU64,
    generation: AtomicU64,
    closed: AtomicBool,
    start_queued: AtomicBool,
    /// After one recover-once, a second 410/SERVER_CLOSED fails the stream
    /// instead of scheduling another reconnect.
    session_recovered: AtomicBool,
    abort: Mutex<Option<Arc<Notify>>>,
    retry_ms: Mutex<Option<u64>>,
    retry_attempt: AtomicU64,
}

struct ObservationState {
    id: String,
    sql: String,
    params: Vec<WireValue>,
    outcomes: Arc<CoalescedOutcomes>,
    closed: Arc<AtomicBool>,
    last_rows: Option<ExecuteResult>,
    last_sequence: i64,
    last_transport: Option<ObserveEvent>,
}

impl ObservationHub {
    pub(crate) fn new() -> Self {
        Self {
            observations: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            start_queued: AtomicBool::new(false),
            session_recovered: AtomicBool::new(false),
            abort: Mutex::new(None),
            retry_ms: Mutex::new(None),
            retry_attempt: AtomicU64::new(0),
        }
    }

    pub(crate) async fn observe<H: ProtocolHttp + 'static>(
        inner: Arc<ClientInner<H>>,
        sql: String,
        params: Vec<Value>,
    ) -> Result<RemoteObserveEvents, LixError> {
        if inner.hub.closed.load(Ordering::SeqCst) {
            return Err(LixError::new(LixError::CODE_CLOSED, "Lix is closed"));
        }
        let id = format!(
            "observe-{}",
            inner.hub.next_id.fetch_add(1, Ordering::SeqCst) + 1
        );
        let wire_params = params
            .iter()
            .map(WireValue::try_from_engine)
            .collect::<Result<Vec<_>, _>>()?;
        let outcomes = CoalescedOutcomes::new();
        let closed = Arc::new(AtomicBool::new(false));
        inner.hub.observations.lock().await.insert(
            id.clone(),
            ObservationState {
                id: id.clone(),
                sql,
                params: wire_params,
                outcomes: Arc::clone(&outcomes),
                closed: Arc::clone(&closed),
                last_rows: None,
                last_sequence: -1,
                last_transport: None,
            },
        );
        let close_inner = Arc::clone(&inner);
        inner.hub.restart(Arc::clone(&inner));
        Ok(RemoteObserveEvents {
            outcomes,
            closed,
            on_close: Arc::new(move || {
                let inner = Arc::clone(&close_inner);
                let spawned = Arc::clone(&inner);
                let id = id.clone();
                inner.http.spawn(Box::pin(async move {
                    spawned.hub.observations.lock().await.remove(&id);
                    spawned.hub.restart(Arc::clone(&spawned));
                }));
            }),
        })
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.stop();
        if let Ok(mut observations) = self.observations.try_lock() {
            for observation in observations.values() {
                observation.closed.store(true, Ordering::SeqCst);
                observation.outcomes.close();
            }
            observations.clear();
        }
    }

    pub(crate) fn restart<H: ProtocolHttp + 'static>(&self, inner: Arc<ClientInner<H>>) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        self.stop();
        if self
            .start_queued
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let spawned = Arc::clone(&inner);
        inner.http.spawn(Box::pin(async move {
            spawned.hub.start_queued.store(false, Ordering::SeqCst);
            spawned.hub.start_stream(Arc::clone(&spawned)).await;
        }));
    }

    fn stop(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        if let Ok(mut abort) = self.abort.try_lock()
            && let Some(notify) = abort.take()
        {
            notify.notify_waiters();
        }
        self.retry_attempt.store(0, Ordering::SeqCst);
        if let Ok(mut retry) = self.retry_ms.try_lock() {
            *retry = None;
        }
    }

    async fn start_stream<H: ProtocolHttp + 'static>(&self, inner: Arc<ClientInner<H>>) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        let subscriptions = {
            let observations = self.observations.lock().await;
            if observations.is_empty() {
                return;
            }
            observations
                .values()
                .map(|observation| MultiplexObserveSubscription {
                    id: observation.id.clone(),
                    sql: observation.sql.clone(),
                    params: observation.params.clone(),
                })
                .collect::<Vec<_>>()
        };
        let generation = self.generation.load(Ordering::SeqCst);
        let abort = Arc::new(Notify::new());
        *self.abort.lock().await = Some(Arc::clone(&abort));
        let consume_inner = Arc::clone(&inner);
        let result = tokio::select! {
            _ = abort.notified() => ConsumeResult::Stop,
            result = self.consume(Arc::clone(&consume_inner), generation, subscriptions) => result,
        };
        if generation != self.generation.load(Ordering::SeqCst) {
            return;
        }
        *self.abort.lock().await = None;
        match result {
            ConsumeResult::Stop => {}
            ConsumeResult::Reconnect => {
                ObservationHub::spawn_reconnect(inner, generation);
            }
            ConsumeResult::Fail(error) => self.fail_all(error).await,
        }
    }

    async fn consume<H: ProtocolHttp + 'static>(
        &self,
        inner: Arc<ClientInner<H>>,
        generation: u64,
        subscriptions: Vec<MultiplexObserveSubscription>,
    ) -> ConsumeResult {
        let mut initial = subscriptions
            .iter()
            .map(|subscription| subscription.id.clone())
            .collect::<HashSet<_>>();
        let body = match serde_json::to_vec(&MultiplexObserveRequest { subscriptions }) {
            Ok(body) => body,
            Err(error) => {
                return ConsumeResult::Fail(protocol_error(format!(
                    "encode observe request: {error}"
                )));
            }
        };
        let mut headers = match request_headers(inner.as_ref(), "text/event-stream", Some(&body))
            .await
        {
            Ok(headers) => headers,
            Err(error) => return ConsumeResult::Fail(error),
        };
        headers.push(("content-type".into(), "application/json".into()));
        let (body, compressed) = super::gzip::maybe_gzip_json(&body);
        if compressed {
            headers.push(("content-encoding".into(), "gzip".into()));
        }
        let request = ProtocolHttpRequest {
            method: "POST",
            path: "observe/multiplex".into(),
            query: Vec::new(),
            headers,
            body: Some(body),
        };
        let response = match inner.http.request_stream(request).await {
            Ok(response) => response,
            Err(error) => {
                if is_recoverable_session_error(&error) {
                    return self.recover_or_fail(inner.as_ref(), error).await;
                }
                if error.code == super::wire::REMOTE_UNAVAILABLE {
                    return ConsumeResult::Reconnect;
                }
                return ConsumeResult::Fail(error);
            }
        };
        if !self.is_current(generation) {
            return ConsumeResult::Stop;
        }
        if !(200..300).contains(&response.status) {
            let error = error_from_observe_status(response.status);
            if is_recoverable_session_error(&error) || response.status == 410 {
                return self.recover_or_fail(inner.as_ref(), error).await;
            }
            if is_retryable_observe_status(response.status) {
                return ConsumeResult::Reconnect;
            }
            return ConsumeResult::Fail(error);
        }
        let content_type = response
            .header("content-type")
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if content_type != "text/event-stream" {
            return ConsumeResult::Fail(protocol_error(
                "remote observe response must be text/event-stream",
            ));
        }

        let mut reader = SseReader::new(response.body);
        loop {
            if !self.is_current(generation) {
                return ConsumeResult::Stop;
            }
            let event = match reader.next_event().await {
                Ok(Some(event)) => event,
                Ok(None) => return ConsumeResult::Reconnect,
                Err(error) => {
                    if is_recoverable_session_error(&error) {
                        return self.recover_or_fail(inner.as_ref(), error).await;
                    }
                    return ConsumeResult::Reconnect;
                }
            };
            if let Some(retry) = event.retry {
                *self.retry_ms.lock().await = Some(retry);
            }
            match event.event.as_str() {
                "next" => {
                    if let Err(error) = self.accept_next(inner.as_ref(), &event.data, &mut initial).await
                    {
                        return ConsumeResult::Fail(error);
                    }
                    self.retry_attempt.store(0, Ordering::SeqCst);
                    self.session_recovered.store(false, Ordering::SeqCst);
                }
                "error" => return self.accept_error(inner.as_ref(), &event.data).await,
                "message" if event.data.is_empty() => {}
                other => {
                    return ConsumeResult::Fail(protocol_error(format!(
                        "unknown remote observe event: {other}"
                    )));
                }
            }
        }
    }

    async fn accept_next<H: ProtocolHttp + 'static>(
        &self,
        inner: &ClientInner<H>,
        data: &str,
        initial: &mut HashSet<String>,
    ) -> Result<(), LixError> {
        let payload: MultiplexObserveEvent = serde_json::from_str(data).map_err(|error| {
            sse_protocol_error(format!(
                "remote observe next event contains invalid data: {error}"
            ))
        })?;
        let transport_delta = payload.delta.is_some();
        let event = {
            let observations = self.observations.lock().await;
            let observation = observations.get(&payload.subscription_id).ok_or_else(|| {
                protocol_error(format!(
                    "unknown remote observe subscription: {}",
                    payload.subscription_id
                ))
            })?;
            decode_observe_event(&payload, observation.last_transport.as_ref())?
        };
        {
            let mut observations = self.observations.lock().await;
            if let Some(observation) = observations.get_mut(&payload.subscription_id) {
                observation.last_transport = Some(event.clone());
            }
        }
        let event = if initial.remove(&payload.subscription_id) {
            let rows = self.refresh(inner, &payload.subscription_id).await?;
            ObserveEvent {
                sequence: event.sequence,
                mutation_sequence: event.mutation_sequence,
                rows,
            }
        } else {
            event
        };
        let mut observations = self.observations.lock().await;
        if let Some(observation) = observations.get_mut(&payload.subscription_id) {
            accept_event(observation, event, transport_delta).await;
        }
        Ok(())
    }

    async fn refresh<H: ProtocolHttp + 'static>(
        &self,
        inner: &ClientInner<H>,
        subscription_id: &str,
    ) -> Result<ExecuteResult, LixError> {
        let (sql, params) = {
            let observations = self.observations.lock().await;
            let observation = observations
                .get(subscription_id)
                .ok_or_else(|| protocol_error("remote observe event requires subscriptionId"))?;
            (
                observation.sql.clone(),
                observation
                    .params
                    .iter()
                    .cloned()
                    .map(WireValue::try_into_engine)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };
        inner
            .enqueue(async {
                inner
                    .execute_inner(&sql, &params, RemoteExecuteOptions::default(), false)
                    .await
            })
            .await
    }

    async fn accept_error<H: ProtocolHttp + 'static>(
        &self,
        inner: &ClientInner<H>,
        data: &str,
    ) -> ConsumeResult {
        let payload: MultiplexObserveError = match serde_json::from_str(data) {
            Ok(payload) => payload,
            Err(error) => {
                return ConsumeResult::Fail(sse_protocol_error(format!(
                    "remote observe error event contains invalid data: {error}"
                )));
            }
        };
        let error = error_from_body(&payload.error);
        if let Some(subscription_id) = payload.subscription_id {
            if payload.retryable == Some(true) {
                self.push_error(&subscription_id, error).await;
                return ConsumeResult::Reconnect;
            }
            self.fail_one(&subscription_id, error).await;
            return ConsumeResult::Stop;
        }
        if payload.retryable == Some(true) {
            self.fail_all(error).await;
            return ConsumeResult::Reconnect;
        }
        if is_recoverable_session_error(&error) {
            return self.recover_or_fail(inner, error).await;
        }
        ConsumeResult::Fail(error)
    }

    async fn recover_or_fail<H: ProtocolHttp + 'static>(
        &self,
        inner: &ClientInner<H>,
        error: LixError,
    ) -> ConsumeResult {
        if self.session_recovered.load(Ordering::SeqCst) {
            return ConsumeResult::Fail(error);
        }
        match inner.recover_session().await {
            Ok(()) => {
                self.session_recovered.store(true, Ordering::SeqCst);
                ConsumeResult::Reconnect
            }
            Err(recover_error) => ConsumeResult::Fail(recover_error),
        }
    }

    async fn push_error(&self, id: &str, error: LixError) {
        if let Some(observation) = self.observations.lock().await.get(id) {
            observation.outcomes.push_recoverable_error(error).await;
        }
    }

    async fn fail_one(&self, id: &str, error: LixError) {
        if let Some(observation) = self.observations.lock().await.remove(id) {
            observation.outcomes.fail(error).await;
        }
    }

    async fn fail_all(&self, error: LixError) {
        let observations = self.observations.lock().await;
        for observation in observations.values() {
            observation.outcomes.fail(error.clone()).await;
        }
    }

    fn spawn_reconnect<H: ProtocolHttp + 'static>(
        inner: Arc<ClientInner<H>>,
        generation: u64,
    ) {
        let spawned = Arc::clone(&inner);
        inner.http.spawn(Box::pin(async move {
            if spawned.hub.closed.load(Ordering::SeqCst)
                || generation != spawned.hub.generation.load(Ordering::SeqCst)
            {
                return;
            }
            if spawned.hub.observations.lock().await.is_empty() {
                return;
            }
            let server_retry = *spawned.hub.retry_ms.lock().await;
            let attempt = spawned.hub.retry_attempt.fetch_add(1, Ordering::SeqCst);
            let delay = server_retry.map_or_else(
                || {
                    OBSERVE_RETRY_BASE_MS
                        .saturating_mul(
                            2u64.saturating_pow(u32::try_from(attempt.min(16)).unwrap_or(16)),
                        )
                        .min(OBSERVE_RETRY_MAX_MS)
                },
                |retry| retry.max(OBSERVE_RETRY_BASE_MS).min(OBSERVE_RETRY_MAX_MS),
            );
            spawned.http.sleep(Duration::from_millis(delay)).await;
            if generation == spawned.hub.generation.load(Ordering::SeqCst) {
                let again = Arc::clone(&spawned);
                spawned
                    .hub
                    .start_stream(again)
                    .await;
            }
        }));
    }

    fn is_current(&self, generation: u64) -> bool {
        !self.closed.load(Ordering::SeqCst) && generation == self.generation.load(Ordering::SeqCst)
    }
}

enum ConsumeResult {
    Stop,
    Reconnect,
    Fail(LixError),
}

async fn accept_event(
    observation: &mut ObservationState,
    event: ObserveEvent,
    transport_delta: bool,
) {
    if observation.closed.load(Ordering::SeqCst) {
        return;
    }
    if !transport_delta
        && observation
            .last_rows
            .as_ref()
            .is_some_and(|rows| rows == &event.rows)
    {
        return;
    }
    let normalized = ObserveEvent {
        sequence: u64::try_from(observation.last_sequence + 1).unwrap_or(0),
        mutation_sequence: event.mutation_sequence,
        rows: event.rows.clone(),
    };
    observation.last_rows = Some(event.rows);
    observation.last_sequence = i64::try_from(normalized.sequence).unwrap_or(i64::MAX);
    observation.outcomes.push_ok(normalized).await;
}

fn decode_observe_event(
    payload: &MultiplexObserveEvent,
    base: Option<&ObserveEvent>,
) -> Result<ObserveEvent, LixError> {
    let has_result = payload.result.is_some();
    let has_delta = payload.delta.is_some();
    if has_result == has_delta {
        return Err(protocol_error(
            "observe event requires exactly one of result or delta",
        ));
    }
    let rows = if let Some(result) = &payload.result {
        decode_execute_response(result)?
    } else {
        apply_observe_delta(
            payload.delta.as_ref().expect("delta"),
            payload.sequence,
            base,
        )?
    };
    Ok(ObserveEvent {
        sequence: payload.sequence,
        mutation_sequence: payload.mutation_sequence,
        rows,
    })
}

fn apply_observe_delta(
    delta: &ObserveDelta,
    sequence: u64,
    base: Option<&ObserveEvent>,
) -> Result<ExecuteResult, LixError> {
    match delta {
        ObserveDelta::SingleBlobSplice {
            base_sequence,
            prefix_bytes,
            suffix_bytes,
            insert_base64,
        } => apply_blob_delta(
            *base_sequence,
            *prefix_bytes,
            *suffix_bytes,
            insert_base64,
            sequence,
            base,
        ),
        ObserveDelta::RowSplice {
            base_sequence,
            prefix_rows,
            delete_rows,
            insert_rows,
        } => apply_row_delta(
            *base_sequence,
            *prefix_rows,
            *delete_rows,
            insert_rows,
            sequence,
            base,
        ),
    }
}

fn apply_blob_delta(
    base_sequence: u64,
    prefix_bytes: u64,
    suffix_bytes: u64,
    insert_base64: &str,
    sequence: u64,
    base: Option<&ObserveEvent>,
) -> Result<ExecuteResult, LixError> {
    let Some(base) = base else {
        return Err(protocol_error(
            "observe blob delta does not match its transport base",
        ));
    };
    if base.sequence != base_sequence || sequence != base_sequence + 1 {
        return Err(protocol_error(
            "observe blob delta does not match its transport base",
        ));
    }
    let rows = base.rows.rows();
    let columns = base.rows.columns();
    let Some(crate::Value::Blob(blob)) = rows.first().and_then(|row| row.values().first()) else {
        return Err(protocol_error(
            "observe blob delta base is not a point blob result",
        ));
    };
    if columns != ["content"]
        || rows.len() != 1
        || rows[0].values().len() != 1
        || base.rows.rows_affected() != 0
        || !base.rows.notices().is_empty()
    {
        return Err(protocol_error(
            "observe blob delta base is not a point blob result",
        ));
    }
    let prefix = usize::try_from(prefix_bytes).unwrap_or(usize::MAX);
    let suffix = usize::try_from(suffix_bytes).unwrap_or(usize::MAX);
    if prefix.saturating_add(suffix) > blob.len() {
        return Err(protocol_error(
            "observe blob delta prefix and suffix overlap",
        ));
    }
    let insert = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, insert_base64)
        .map_err(|_| protocol_error("observe blob delta insertBase64 is invalid"))?;
    let mut next = Vec::with_capacity(prefix + insert.len() + suffix);
    next.extend_from_slice(&blob[..prefix]);
    next.extend_from_slice(&insert);
    next.extend_from_slice(&blob[blob.len() - suffix..]);
    Ok(ExecuteResult::from_protocol_response(
        None,
        None,
        vec!["content".into()],
        vec![vec![Value::Blob(next.into())]],
        0,
        Vec::new(),
    ))
}

fn apply_row_delta(
    base_sequence: u64,
    prefix_rows: u64,
    delete_rows: u64,
    insert_rows: &[Vec<WireValue>],
    sequence: u64,
    base: Option<&ObserveEvent>,
) -> Result<ExecuteResult, LixError> {
    let Some(base) = base else {
        return Err(protocol_error(
            "observe row delta does not match its transport base",
        ));
    };
    if base.sequence != base_sequence || sequence != base_sequence + 1 {
        return Err(protocol_error(
            "observe row delta does not match its transport base",
        ));
    }
    let prefix = usize::try_from(prefix_rows).unwrap_or(usize::MAX);
    let delete = usize::try_from(delete_rows).unwrap_or(usize::MAX);
    let rows = base.rows.rows();
    if prefix.saturating_add(delete) > rows.len() {
        return Err(protocol_error("observe row delta window is out of range"));
    }
    let mut decoded_insert = Vec::with_capacity(insert_rows.len());
    for row in insert_rows {
        decoded_insert.push(
            row.iter()
                .cloned()
                .map(WireValue::try_into_engine)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    let mut next = Vec::with_capacity(rows.len() - delete + decoded_insert.len());
    next.extend(rows[..prefix].iter().map(|row| row.values().to_vec()));
    next.extend(decoded_insert);
    next.extend(
        rows[prefix + delete..]
            .iter()
            .map(|row| row.values().to_vec()),
    );
    Ok(ExecuteResult::from_protocol_response(
        None,
        None,
        base.rows.columns().to_vec(),
        next,
        base.rows.rows_affected(),
        base.rows.notices().to_vec(),
    ))
}

fn error_from_body(body: &super::wire::ErrorBody) -> LixError {
    let mut error = LixError::new(
        body.code
            .clone()
            .unwrap_or_else(|| super::wire::REMOTE_REQUEST_FAILED.to_owned()),
        body.message
            .clone()
            .unwrap_or_else(|| "Remote Lix operation failed".to_owned()),
    );
    error.hint = body.hint.clone();
    error.details = body.details.clone();
    error
}

fn error_from_observe_status(status: u16) -> LixError {
    let code = match status {
        410 => super::wire::SESSION_GONE,
        503 => super::wire::SERVER_CLOSED,
        _ => super::wire::REMOTE_REQUEST_FAILED,
    };
    with_status(
        LixError::new(
            code,
            format!("Remote Lix request failed with status {status}"),
        ),
        status,
    )
}

fn is_retryable_observe_status(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}
