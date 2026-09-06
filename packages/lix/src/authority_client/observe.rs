use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::Engine as _;
use futures_util::{
    StreamExt,
    future::{select_all, try_join_all},
};
use tokio::sync::{Mutex, Notify};

use crate::{ExecuteResult, LixError, ObserveEvent, Value, WireValue};

use super::http::{ProtocolHttp, ProtocolHttpRequest};
use super::sse::next_sse_event;
use super::wire::{
    ErrorEnvelope, MultiplexObserveNext, MultiplexObserveRequest, MultiplexObserveSubscription,
    ObserveDelta, encode_engine_wire_values, is_recoverable_session_error, protocol_error,
    remote_error,
};

const OBSERVE_RETRY_BASE_MS: u64 = 100;
const OBSERVE_RETRY_MAX_MS: u64 = 5_000;
const MAX_SUBSCRIPTIONS_PER_STREAM: usize = 32;

#[cfg(not(target_arch = "wasm32"))]
type ConsumeFuture<'a> = Pin<Box<dyn Future<Output = ConsumeResult> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
type ConsumeFuture<'a> = Pin<Box<dyn Future<Output = ConsumeResult> + 'a>>;

#[cfg(not(target_arch = "wasm32"))]
pub trait ObserveTransport: Clone + Send + Sync + 'static {
    fn open_observe_stream(
        &self,
        subscriptions: Vec<MultiplexObserveSubscription>,
    ) -> impl Future<Output = Result<super::http::ProtocolHttpStream, LixError>> + Send;

    fn recover_session(&self) -> impl Future<Output = Result<(), LixError>> + Send;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>);
}

#[cfg(target_arch = "wasm32")]
pub trait ObserveTransport: Clone + 'static {
    fn open_observe_stream(
        &self,
        subscriptions: Vec<MultiplexObserveSubscription>,
    ) -> impl Future<Output = Result<super::http::ProtocolHttpStream, LixError>>;

    fn recover_session(&self) -> impl Future<Output = Result<(), LixError>>;

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()>;

    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()>>>);
}

#[derive(Debug)]
struct SubscriptionState {
    id: String,
    sql: String,
    params: Vec<Value>,
    events: Mutex<VecDeque<Result<Option<ObserveEvent>, LixError>>>,
    notify: Notify,
    closed: AtomicBool,
    last_error: Mutex<Option<LixError>>,
    last_rows: Mutex<Option<ExecuteResult>>,
    last_sequence: Mutex<i64>,
}

enum SessionRecoveryState {
    Idle,
    Recovering,
    Succeeded,
    Failed(LixError),
}

struct HubState {
    closed: bool,
    next_id: u64,
    subscriptions: HashMap<String, Arc<SubscriptionState>>,
    subscription_order: Vec<String>,
    retry_attempt: u32,
    server_retry_ms: Option<u64>,
    session_recovery: SessionRecoveryState,
    generation: u64,
    driver_running: bool,
    failed_generation: Option<u64>,
    current_cancels: Vec<super::http::StreamCancel>,
    pending_initial: std::collections::HashSet<String>,
}

impl std::fmt::Debug for HubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubState")
            .field("closed", &self.closed)
            .field("subscription_count", &self.subscriptions.len())
            .field("generation", &self.generation)
            .field("driver_running", &self.driver_running)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ObservationHub<T> {
    transport: T,
    state: Arc<Mutex<HubState>>,
    interrupt: Arc<Notify>,
    recovery_finished: Arc<Notify>,
}

impl<T> std::fmt::Debug for ObservationHub<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObservationHub").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ProtocolObserveEvents<T> {
    hub: ObservationHub<T>,
    subscription: Arc<SubscriptionState>,
}

impl<T: ObserveTransport> ObservationHub<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            interrupt: Arc::new(Notify::new()),
            recovery_finished: Arc::new(Notify::new()),
            state: Arc::new(Mutex::new(HubState {
                closed: false,
                next_id: 0,
                subscriptions: HashMap::new(),
                subscription_order: Vec::new(),
                retry_attempt: 0,
                server_retry_ms: None,
                session_recovery: SessionRecoveryState::Idle,
                generation: 0,
                driver_running: false,
                failed_generation: None,
                current_cancels: Vec::new(),
                pending_initial: std::collections::HashSet::new(),
            })),
        }
    }

    pub async fn observe(
        &self,
        sql: impl Into<String>,
        params: Vec<Value>,
    ) -> Result<ProtocolObserveEvents<T>, LixError> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(super::wire::closed_error());
        }
        if state.subscriptions.is_empty()
            && matches!(state.session_recovery, SessionRecoveryState::Failed(_))
        {
            state.session_recovery = SessionRecoveryState::Idle;
            state.retry_attempt = 0;
        }
        state.next_id += 1;
        let id = format!("observe-{}", state.next_id);
        let subscription = Arc::new(SubscriptionState {
            id: id.clone(),
            sql: sql.into(),
            params,
            events: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_rows: Mutex::new(None),
            last_sequence: Mutex::new(-1),
        });
        state.subscriptions.insert(id.clone(), subscription.clone());
        state.subscription_order.push(id);
        let should_start = !state.driver_running;
        // A driver may be opening one or more multiplex streams while membership
        // is changing. Invalidate that snapshot under the same
        // lock as the membership update. Deferring invalidation to another
        // spawned task lets observe() return a registered subscription that the
        // active driver has not been told to serve yet.
        if should_start {
            state.driver_running = true;
            state.generation += 1;
        } else {
            state.generation += 1;
            state.retry_attempt = 0;
            state.server_retry_ms = None;
            abort_current_stream(&mut state);
            self.interrupt.notify_waiters();
        }
        drop(state);
        if should_start {
            self.start_driver();
        }
        Ok(ProtocolObserveEvents {
            hub: self.clone(),
            subscription,
        })
    }

    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        state.closed = true;
        state.generation += 1;
        abort_current_stream(&mut state);
        self.interrupt.notify_waiters();
        state.subscription_order.clear();
        let subscriptions = std::mem::take(&mut state.subscriptions);
        drop(state);
        for subscription in subscriptions.into_values() {
            close_subscription(&subscription, None).await;
        }
    }

    pub fn restart(&self) {
        let hub = self.clone();
        self.transport.spawn(Box::pin(async move {
            let mut state = hub.state.lock().await;
            if state.closed {
                return;
            }
            state.generation += 1;
            state.retry_attempt = 0;
            state.server_retry_ms = None;
            abort_current_stream(&mut state);
            hub.interrupt.notify_waiters();
            if !state.driver_running && !state.subscriptions.is_empty() {
                state.driver_running = true;
                drop(state);
                hub.drive().await;
            }
        }));
    }

    fn start_driver(&self) {
        let hub = self.clone();
        self.transport
            .spawn(Box::pin(async move { hub.drive().await }));
    }

    async fn drive(&self) {
        'drive: loop {
            let (generation, subscriptions) = {
                let mut state = self.state.lock().await;
                if state.closed || state.subscriptions.is_empty() {
                    state.driver_running = false;
                    return;
                }
                (
                    state.generation,
                    state
                        .subscription_order
                        .iter()
                        .filter_map(|id| state.subscriptions.get(id))
                        .map(|subscription| MultiplexObserveSubscription {
                            id: subscription.id.clone(),
                            sql: subscription.sql.clone(),
                            params: encode_engine_wire_values(&subscription.params)
                                .unwrap_or_default(),
                        })
                        .collect::<Vec<_>>(),
                )
            };

            // Arm the waiter before the final generation check. `notify_waiters`
            // does not retain a permit, so registering only inside `select!`
            // leaves a gap where a membership change can invalidate this
            // snapshot without waking the driver.
            let mut interrupted = std::pin::pin!(self.interrupt.notified());
            interrupted.as_mut().enable();
            if !self.is_current(generation).await {
                continue;
            }
            let opening = try_join_all(
                subscriptions
                    .chunks(MAX_SUBSCRIPTIONS_PER_STREAM)
                    .map(|batch| self.open_stream_for_generation(batch.to_vec(), generation)),
            );
            let open_result = tokio::select! {
                _ = interrupted.as_mut() => {
                    let mut state = self.state.lock().await;
                    abort_current_stream(&mut state);
                    continue;
                }
                results = opening => results,
            };
            if !self.is_current(generation).await {
                let mut state = self.state.lock().await;
                abort_current_stream(&mut state);
                continue;
            }

            if let Err(error) = &open_result {
                let error = error.clone();
                {
                    let mut state = self.state.lock().await;
                    abort_current_stream(&mut state);
                }
                if is_recoverable_session_error(&error) {
                    match self.recover_session_or_fail(&error).await {
                        Ok(()) => continue,
                        Err(error) => {
                            if !self.is_current(generation).await {
                                continue;
                            }
                            self.fail_all_for_generation(error, generation).await;
                            return;
                        }
                    }
                } else if is_retryable_observe_error(&error) {
                    let mut state = self.state.lock().await;
                    if state.closed || state.subscriptions.is_empty() {
                        state.driver_running = false;
                        return;
                    }
                } else {
                    if !self.is_current(generation).await {
                        continue;
                    }
                    self.fail_all_for_generation(error, generation).await;
                    return;
                }
            } else {
                let streams = open_result
                    .expect("successful stream opening result")
                    .into_iter()
                    .collect::<Option<Vec<_>>>();
                let Some(streams) = streams else {
                    let mut state = self.state.lock().await;
                    abort_current_stream(&mut state);
                    continue 'drive;
                };
                let stream_cancels = streams
                    .iter()
                    .map(|stream| stream.cancel.clone())
                    .collect::<Vec<_>>();
                {
                    let mut state = self.state.lock().await;
                    if state.generation != generation
                        || state.closed
                        || state.subscriptions.is_empty()
                    {
                        abort_current_stream(&mut state);
                        continue;
                    }
                    state.pending_initial = subscriptions
                        .iter()
                        .map(|subscription| subscription.id.clone())
                        .collect();
                    // Each open registered its cancel handle immediately, so a
                    // close or membership restart could interrupt setup.
                    debug_assert_eq!(state.current_cancels.len(), stream_cancels.len());
                }

                let mut consumers = Vec::<ConsumeFuture<'_>>::with_capacity(streams.len());
                for stream in streams {
                    consumers.push(Box::pin(self.consume_stream(stream, generation)));
                }
                let (consume, _, remaining) = select_all(consumers).await;
                drop(remaining);
                {
                    let mut state = self.state.lock().await;
                    if state.failed_generation == Some(generation) {
                        return;
                    }
                    abort_current_stream(&mut state);
                }
                match consume {
                    ConsumeResult::Continue => {}
                    ConsumeResult::ReconnectNow => continue,
                    ConsumeResult::Stop => {
                        let mut state = self.state.lock().await;
                        if state.closed || state.subscriptions.is_empty() {
                            state.driver_running = false;
                            return;
                        }
                        if state.generation != generation {
                            continue;
                        }
                        state.driver_running = false;
                        return;
                    }
                }
            }

            let delay = {
                let mut state = self.state.lock().await;
                if state.closed || state.subscriptions.is_empty() {
                    state.driver_running = false;
                    return;
                }
                if state.generation != generation {
                    None
                } else {
                    let delay = reconnect_delay(&state);
                    state.retry_attempt = state.retry_attempt.saturating_add(1);
                    Some(delay)
                }
            };
            match delay {
                None => continue,
                Some(delay) => {
                    let mut interrupted = std::pin::pin!(self.interrupt.notified());
                    interrupted.as_mut().enable();
                    if !self.is_current(generation).await {
                        continue;
                    }
                    tokio::select! {
                        _ = interrupted.as_mut() => continue,
                        _ = self.transport.sleep(delay) => {}
                    }
                }
            }
        }
    }

    async fn open_stream_for_generation(
        &self,
        subscriptions: Vec<MultiplexObserveSubscription>,
        generation: u64,
    ) -> Result<Option<super::http::ProtocolHttpStream>, LixError> {
        let stream = self.transport.open_observe_stream(subscriptions).await?;
        let mut cancel_on_drop = CancelOnDrop(Some(stream.cancel.clone()));
        let mut state = self.state.lock().await;
        if state.closed || state.generation != generation {
            return Ok(None);
        }
        state.current_cancels.push(stream.cancel.clone());
        cancel_on_drop.0 = None;
        Ok(Some(stream))
    }

    async fn recover_session_or_fail(&self, error: &LixError) -> Result<(), LixError> {
        let mut joined_recovery = false;
        loop {
            let mut state = self.state.lock().await;
            match &state.session_recovery {
                SessionRecoveryState::Idle => {
                    state.session_recovery = SessionRecoveryState::Recovering;
                    drop(state);
                    joined_recovery = true;
                    // Register before spawn: `notify_waiters()` does not keep a
                    // permit, so a finished recovery would otherwise hang us.
                    let mut recovery_finished = std::pin::pin!(self.recovery_finished.notified());
                    recovery_finished.as_mut().enable();
                    let hub = self.clone();
                    let transport = self.transport.clone();
                    self.transport.spawn(Box::pin(async move {
                        let result = transport.recover_session().await;
                        let mut state = hub.state.lock().await;
                        state.session_recovery = match result {
                            Ok(()) => SessionRecoveryState::Succeeded,
                            Err(error) => SessionRecoveryState::Failed(error),
                        };
                        drop(state);
                        hub.recovery_finished.notify_waiters();
                    }));
                    recovery_finished.await;
                }
                SessionRecoveryState::Recovering => {
                    joined_recovery = true;
                    drop(state);
                    let mut recovery_finished = std::pin::pin!(self.recovery_finished.notified());
                    recovery_finished.as_mut().enable();
                    {
                        let state = self.state.lock().await;
                        if !matches!(state.session_recovery, SessionRecoveryState::Recovering) {
                            continue;
                        }
                    }
                    recovery_finished.await;
                }
                SessionRecoveryState::Succeeded if joined_recovery => return Ok(()),
                SessionRecoveryState::Succeeded => return Err(error.clone()),
                SessionRecoveryState::Failed(recovery_error) => {
                    return Err(recovery_error.clone());
                }
            }
        }
    }

    async fn consume_stream(
        &self,
        mut stream: super::http::ProtocolHttpStream,
        generation: u64,
    ) -> ConsumeResult {
        if !self.is_current(generation).await {
            return ConsumeResult::Stop;
        }
        if !super::is_success_status(stream.status) {
            let mut body = Vec::new();
            while let Some(chunk) = stream.body.next().await {
                if !self.is_current(generation).await {
                    return ConsumeResult::Stop;
                }
                match chunk {
                    Ok(bytes) => body.extend_from_slice(&bytes),
                    Err(error) => {
                        if !self.is_current(generation).await {
                            return ConsumeResult::Stop;
                        }
                        self.fail_all_for_generation(error, generation).await;
                        return ConsumeResult::Stop;
                    }
                }
            }
            let error = if let Ok(envelope) = serde_json::from_slice::<ErrorEnvelope>(&body) {
                with_http_status(error_from_envelope(&envelope), stream.status)
            } else {
                remote_error(
                    "LIX_REMOTE_REQUEST_FAILED",
                    format!("Remote Lix request failed with status {}", stream.status),
                )
                .with_details(serde_json::json!({ "httpStatus": stream.status }))
            };
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            if is_recoverable_session_error(&error) {
                return match self.recover_session_or_fail(&error).await {
                    Ok(()) if self.is_current(generation).await => ConsumeResult::ReconnectNow,
                    Ok(()) => ConsumeResult::Stop,
                    Err(error) => {
                        if !self.is_current(generation).await {
                            return ConsumeResult::Stop;
                        }
                        self.fail_all_for_generation(error, generation).await;
                        ConsumeResult::Stop
                    }
                };
            }
            if is_retryable_observe_status(stream.status) {
                return ConsumeResult::Continue;
            }
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            self.fail_all_for_generation(error, generation).await;
            return ConsumeResult::Stop;
        }
        let content_type = stream
            .header("content-type")
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if content_type != "text/event-stream" {
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            self.fail_all_for_generation(
                protocol_error("remote observe response must be text/event-stream"),
                generation,
            )
            .await;
            return ConsumeResult::Stop;
        }

        let mut buffered = String::new();
        let mut event_name = String::new();
        let mut retry = None;
        let mut data_lines = Vec::new();
        let mut bases = HashMap::<String, ObserveEvent>::new();
        loop {
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            let mut interrupted = std::pin::pin!(self.interrupt.notified());
            interrupted.as_mut().enable();
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            let frame = tokio::select! {
                _ = interrupted.as_mut() => return ConsumeResult::Stop,
                frame = next_sse_event(
                    &mut stream.body,
                    &mut buffered,
                    &mut event_name,
                    &mut retry,
                    &mut data_lines,
                ) => frame,
            };
            if !self.is_current(generation).await {
                return ConsumeResult::Stop;
            }
            match frame {
                Ok(Some(frame)) => {
                    if let Some(server_retry) = frame.retry {
                        let mut state = self.state.lock().await;
                        if state.generation != generation || state.closed {
                            return ConsumeResult::Stop;
                        }
                        state.server_retry_ms = Some(server_retry);
                    }
                    match frame.event.as_str() {
                        "next" => {
                            if let Err(error) = self
                                .handle_next_event(
                                    &frame.data,
                                    &mut bases,
                                    generation,
                                )
                                .await
                            {
                                if !self.is_current(generation).await {
                                    return ConsumeResult::Stop;
                                }
                                self.fail_all_for_generation(error, generation).await;
                                return ConsumeResult::Stop;
                            }
                        }
                        "error" => {
                            return self.handle_error_event(&frame.data, generation).await;
                        }
                        "message" if frame.data.is_empty() => {}
                        other => {
                            if !self.is_current(generation).await {
                                return ConsumeResult::Stop;
                            }
                            self.fail_all_for_generation(
                                protocol_error(format!("unknown remote observe event: {other}")),
                                generation,
                            )
                            .await;
                            return ConsumeResult::Stop;
                        }
                    }
                }
                Ok(None) => return ConsumeResult::Continue,
                Err(error) => {
                    if !self.is_current(generation).await {
                        return ConsumeResult::Stop;
                    }
                    if is_retryable_observe_error(&error) {
                        return ConsumeResult::Continue;
                    }
                    self.fail_all_for_generation(error, generation).await;
                    return ConsumeResult::Stop;
                }
            }
        }
    }

    async fn handle_next_event(
        &self,
        data: &str,
        bases: &mut HashMap<String, ObserveEvent>,
        generation: u64,
    ) -> Result<(), LixError> {
        let payload: MultiplexObserveNext = serde_json::from_str(data).map_err(|_| {
            protocol_error("remote multiplex observe next event contains invalid data")
        })?;
        let subscription = {
            let state = self.state.lock().await;
            if state.generation != generation || state.closed {
                return Ok(());
            }
            state
                .subscriptions
                .get(&payload.subscription_id)
                .cloned()
                .ok_or_else(|| {
                    protocol_error(format!(
                        "unknown remote observe subscription: {}",
                        payload.subscription_id
                    ))
                })?
        };
        let transport_delta = payload.delta.is_some();
        let event = decode_observe_event(payload, bases.get(&subscription.id))?;
        bases.insert(subscription.id.clone(), event.clone());
        // The first SSE frame is already evaluated in the authoritative server
        // session and carries its mutation sequence. Re-executing the query on
        // a separate request adds head-of-line blocking and can pair newer rows
        // with the older frame's sequence.
        let publish = event;
        let mut state = self.state.lock().await;
        if state.generation != generation || state.closed {
            return Ok(());
        }
        accept_event(&subscription, publish, transport_delta).await;
        let completed_initial =
            state.pending_initial.remove(&subscription.id) && state.pending_initial.is_empty();
        if completed_initial {
            state.retry_attempt = 0;
            state.session_recovery = SessionRecoveryState::Idle;
        }
        Ok(())
    }

    async fn handle_error_event(&self, data: &str, generation: u64) -> ConsumeResult {
        if !self.is_current(generation).await {
            return ConsumeResult::Stop;
        }
        let payload: ErrorEnvelope = serde_json::from_str(data).unwrap_or(ErrorEnvelope {
            error: super::wire::ErrorBody {
                code: Some("LIX_SERVER_PROTOCOL_ERROR".to_owned()),
                message: Some(format!(
                    "remote observe error event contains invalid data: {data}"
                )),
                hint: None,
                details: None,
            },
            subscription_id: None,
            retryable: None,
        });
        let error = error_from_envelope(&payload);
        if is_recoverable_session_error(&error) {
            match self.recover_session_or_fail(&error).await {
                Ok(()) if self.is_current(generation).await => {
                    return ConsumeResult::ReconnectNow;
                }
                Ok(()) => return ConsumeResult::Stop,
                Err(error) => {
                    if !self.is_current(generation).await {
                        return ConsumeResult::Stop;
                    }
                    self.fail_all_for_generation(error, generation).await;
                    return ConsumeResult::Stop;
                }
            }
        }
        if let Some(subscription_id) = payload.subscription_id {
            let subscription = {
                let state = self.state.lock().await;
                if state.generation != generation || state.closed {
                    return ConsumeResult::Stop;
                }
                state.subscriptions.get(&subscription_id).cloned()
            };
            if let Some(subscription) = subscription {
                if payload.retryable == Some(true) {
                    recover_subscription(&subscription, error).await;
                    return ConsumeResult::Continue;
                }
                {
                    let mut state = self.state.lock().await;
                    if state.generation != generation || state.closed {
                        return ConsumeResult::Stop;
                    }
                    state.subscription_order.retain(|id| id != &subscription_id);
                    state.subscriptions.remove(&subscription_id);
                }
                close_subscription(&subscription, Some(error)).await;
            }
            return ConsumeResult::Continue;
        }
        if payload.retryable == Some(true) {
            let subscriptions = {
                let state = self.state.lock().await;
                if state.generation != generation || state.closed {
                    return ConsumeResult::Stop;
                }
                state.subscriptions.values().cloned().collect::<Vec<_>>()
            };
            for subscription in subscriptions {
                recover_subscription(&subscription, error.clone()).await;
            }
            ConsumeResult::Continue
        } else {
            self.fail_all_for_generation(error, generation).await;
            ConsumeResult::Stop
        }
    }

    async fn fail_all_for_generation(&self, error: LixError, generation: u64) {
        let subscriptions = {
            let mut state = self.state.lock().await;
            if state.closed || state.generation != generation {
                return;
            }
            state.failed_generation = Some(generation);
            state.generation += 1;
            state.driver_running = false;
            abort_current_stream(&mut state);
            state.subscription_order.clear();
            std::mem::take(&mut state.subscriptions)
        };
        for subscription in subscriptions.into_values() {
            close_subscription(&subscription, Some(error.clone())).await;
        }
    }

    async fn is_current(&self, generation: u64) -> bool {
        let state = self.state.lock().await;
        !state.closed && state.generation == generation
    }
}

impl<T: ObserveTransport> ProtocolObserveEvents<T> {
    pub async fn next(&self) -> Result<Option<ObserveEvent>, LixError> {
        loop {
            {
                let mut events = self.subscription.events.lock().await;
                if let Some(event) = events.pop_front() {
                    return event;
                }
                if self.subscription.closed.load(Ordering::SeqCst) {
                    if let Some(error) = self.subscription.last_error.lock().await.clone() {
                        return Err(error);
                    }
                    return Ok(None);
                }
            }
            // Register before checking the queue again. An event or close can
            // otherwise call `notify_waiters()` after the check but before the
            // waiter is first polled, leaving this read asleep with durable
            // state already queued.
            let mut notified = std::pin::pin!(self.subscription.notify.notified());
            notified.as_mut().enable();
            {
                let mut events = self.subscription.events.lock().await;
                if let Some(event) = events.pop_front() {
                    return event;
                }
                if self.subscription.closed.load(Ordering::SeqCst) {
                    if let Some(error) = self.subscription.last_error.lock().await.clone() {
                        return Err(error);
                    }
                    return Ok(None);
                }
            }
            notified.await;
        }
    }

    pub fn close(&self) {
        let hub = self.hub.clone();
        let subscription = self.subscription.clone();
        self.hub.transport.spawn(Box::pin(async move {
            let mut state = hub.state.lock().await;
            state.subscriptions.remove(&subscription.id);
            state.subscription_order.retain(|id| id != &subscription.id);
            state.generation += 1;
            state.retry_attempt = 0;
            state.server_retry_ms = None;
            abort_current_stream(&mut state);
            hub.interrupt.notify_waiters();
            let should_start = !state.driver_running && !state.subscriptions.is_empty();
            if should_start {
                state.driver_running = true;
            }
            drop(state);
            close_subscription(&subscription, None).await;
            if should_start {
                hub.drive().await;
            }
        }));
    }
}

enum ConsumeResult {
    Continue,
    ReconnectNow,
    Stop,
}

struct CancelOnDrop(Option<super::http::StreamCancel>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel();
        }
    }
}

fn abort_current_stream(state: &mut HubState) {
    for cancel in state.current_cancels.drain(..) {
        cancel();
    }
    state.pending_initial.clear();
}

async fn accept_event(
    subscription: &SubscriptionState,
    event: ObserveEvent,
    transport_delta: bool,
) {
    if subscription.closed.load(Ordering::SeqCst) {
        return;
    }
    let mut last_rows = subscription.last_rows.lock().await;
    if !transport_delta && last_rows.as_ref().is_some_and(|rows| rows == &event.rows) {
        return;
    }
    let mut last_sequence = subscription.last_sequence.lock().await;
    let normalized = ObserveEvent {
        sequence: (*last_sequence + 1) as u64,
        mutation_sequence: event.mutation_sequence,
        rows: event.rows.clone(),
    };
    *last_rows = Some(event.rows);
    *last_sequence += 1;
    let mut events = subscription.events.lock().await;
    events.retain(|event| event.is_err());
    events.push_back(Ok(Some(normalized)));
    subscription.notify.notify_waiters();
}

async fn recover_subscription(subscription: &SubscriptionState, error: LixError) {
    if subscription.closed.load(Ordering::SeqCst) {
        return;
    }
    let mut events = subscription.events.lock().await;
    if !events.iter().any(Result::is_err) {
        events.push_back(Err(error));
    }
    subscription.notify.notify_waiters();
}

async fn close_subscription(subscription: &SubscriptionState, error: Option<LixError>) {
    subscription.closed.store(true, Ordering::SeqCst);
    if let Some(error) = &error {
        *subscription.last_error.lock().await = Some(error.clone());
    }
    let mut events = subscription.events.lock().await;
    if let Some(error) = error {
        events.push_back(Err(error));
    } else {
        events.push_back(Ok(None));
    }
    subscription.notify.notify_waiters();
}

fn decode_observe_event(
    payload: MultiplexObserveNext,
    base: Option<&ObserveEvent>,
) -> Result<ObserveEvent, LixError> {
    match (payload.result, payload.delta) {
        (Some(result), None) => Ok(ObserveEvent {
            sequence: payload.sequence,
            mutation_sequence: payload.mutation_sequence,
            rows: result.into_execute_result()?,
        }),
        (None, Some(delta)) => Ok(ObserveEvent {
            sequence: payload.sequence,
            mutation_sequence: payload.mutation_sequence,
            rows: apply_observe_delta(delta, payload.sequence, base)?,
        }),
        _ => Err(protocol_error(
            "observe event requires exactly one of result or delta",
        )),
    }
}

fn apply_observe_delta(
    delta: ObserveDelta,
    sequence: u64,
    base: Option<&ObserveEvent>,
) -> Result<ExecuteResult, LixError> {
    match delta {
        ObserveDelta::SingleBlobSplice {
            base_sequence,
            prefix_bytes,
            suffix_bytes,
            insert_base64,
        } => {
            let base = base.ok_or_else(|| {
                protocol_error("observe blob delta does not match its transport base")
            })?;
            if base.sequence != base_sequence || sequence != base_sequence + 1 {
                return Err(protocol_error(
                    "observe blob delta does not match its transport base",
                ));
            }
            let rows = base.rows.rows();
            let value = rows.first().and_then(|row| row.values().first());
            let Value::Blob(blob) = value.cloned().unwrap_or(Value::Null) else {
                return Err(protocol_error(
                    "observe blob delta base is not a point blob result",
                ));
            };
            if base.rows.columns() != ["content"]
                || rows.len() != 1
                || rows[0].values().len() != 1
                || base.rows.rows_affected() != 0
                || !base.rows.notices().is_empty()
            {
                return Err(protocol_error(
                    "observe blob delta base is not a point blob result",
                ));
            }
            if prefix_bytes + suffix_bytes > blob.len() {
                return Err(protocol_error(
                    "observe blob delta prefix and suffix overlap",
                ));
            }
            let insert = base64::engine::general_purpose::STANDARD
                .decode(insert_base64.as_bytes())
                .map_err(|_| protocol_error("observe delta insertBase64 must be a string"))?;
            let mut next = Vec::with_capacity(prefix_bytes + insert.len() + suffix_bytes);
            next.extend_from_slice(&blob.as_bytes()[..prefix_bytes]);
            next.extend_from_slice(&insert);
            next.extend_from_slice(&blob.as_bytes()[blob.len() - suffix_bytes..]);
            Ok(ExecuteResult::from_protocol_response(
                None,
                None,
                vec!["content".to_owned()],
                vec![crate::ResultColumnType::Blob],
                vec![vec![Value::Blob(next.into())]],
                0,
                Vec::new(),
            ))
        }
        ObserveDelta::RowSplice {
            base_sequence,
            prefix_rows,
            delete_rows,
            insert_rows,
        } => {
            let base = base.ok_or_else(|| {
                protocol_error("observe row delta does not match its transport base")
            })?;
            if base.sequence != base_sequence || sequence != base_sequence + 1 {
                return Err(protocol_error(
                    "observe row delta does not match its transport base",
                ));
            }
            let current = base.rows.rows();
            if prefix_rows > current.len() || delete_rows > current.len() - prefix_rows {
                return Err(protocol_error(
                    "observe row delta splice range is outside its transport base",
                ));
            }
            let suffix_start = prefix_rows + delete_rows;
            let mut rows =
                Vec::with_capacity(prefix_rows + insert_rows.len() + current.len() - suffix_start);
            for row in current.iter().take(prefix_rows) {
                rows.push(row.values().to_vec());
            }
            for row in insert_rows {
                if row.len() != base.rows.columns().len() {
                    return Err(protocol_error(
                        "observe row delta insert row has the wrong number of values",
                    ));
                }
                rows.push(
                    row.into_iter()
                        .map(WireValue::try_into_engine)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            for row in current.iter().skip(suffix_start) {
                rows.push(row.values().to_vec());
            }
            Ok(ExecuteResult::from_protocol_response(
                None,
                None,
                base.rows.columns().to_vec(),
                base.rows.column_types().to_vec(),
                rows,
                base.rows.rows_affected(),
                base.rows.notices().to_vec(),
            ))
        }
    }
}

fn with_http_status(error: LixError, status: u16) -> LixError {
    let mut details = error
        .details
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    if let Some(object) = details.as_object_mut() {
        object.insert("httpStatus".to_owned(), serde_json::json!(status));
    } else {
        details = serde_json::json!({
            "httpStatus": status,
            "body": details,
        });
    }
    error.with_details(details)
}

fn error_from_envelope(payload: &ErrorEnvelope) -> LixError {
    let mut error = remote_error(
        payload
            .error
            .code
            .clone()
            .unwrap_or_else(|| "LIX_REMOTE_REQUEST_FAILED".to_owned()),
        payload
            .error
            .message
            .clone()
            .unwrap_or_else(|| "Remote Lix operation failed".to_owned()),
    );
    if let Some(hint) = &payload.error.hint {
        error = error.with_hint(hint.clone());
    }
    if let Some(details) = &payload.error.details {
        error = error.with_details(details.clone());
    }
    error
}

fn reconnect_delay(state: &HubState) -> Duration {
    let ms = match state.server_retry_ms {
        Some(retry) => retry.clamp(OBSERVE_RETRY_BASE_MS, OBSERVE_RETRY_MAX_MS),
        None => OBSERVE_RETRY_BASE_MS
            .saturating_mul(1u64 << state.retry_attempt.min(6))
            .min(OBSERVE_RETRY_MAX_MS),
    };
    Duration::from_millis(ms)
}

fn is_retryable_observe_status(status: u16) -> bool {
    status == 408 || status == 429 || status >= 500
}

fn is_retryable_observe_error(error: &LixError) -> bool {
    error.code == "LIX_REMOTE_UNAVAILABLE"
}

impl<H> ObserveTransport for super::ClientCore<H>
where
    H: ProtocolHttp + Clone + 'static,
{
    async fn open_observe_stream(
        &self,
        subscriptions: Vec<MultiplexObserveSubscription>,
    ) -> Result<super::http::ProtocolHttpStream, LixError> {
        let session_id = self
            .session_id()
            .ok_or_else(|| protocol_error("remote observation started without a session"))?;
        let body = serde_json::to_vec(&MultiplexObserveRequest {
            subscriptions: &subscriptions,
        })
        .map_err(|error| protocol_error(format!("could not encode observe request: {error}")))?;
        let (body, compressed) = super::maybe_compress_json(body)?;
        let mut headers = vec![
            ("accept".to_owned(), "text/event-stream".to_owned()),
            ("content-type".to_owned(), "application/json".to_owned()),
            (super::wire::SESSION_HEADER.to_owned(), session_id),
        ];
        if compressed {
            headers.push(("content-encoding".to_owned(), "gzip".to_owned()));
        }
        self.http
            .request_stream(ProtocolHttpRequest {
                method: "POST".to_owned(),
                url: self.join_path("observe/multiplex")?,
                headers,
                body: Some(body),
            })
            .await
    }

    async fn recover_session(&self) -> Result<(), LixError> {
        let _guard = self.operation_lock.lock().await;
        if !self.accepting.load(Ordering::SeqCst) {
            return Err(super::wire::closed_error());
        }
        self.recover_session_once().await
    }

    async fn sleep(&self, duration: Duration) {
        self.http.sleep(duration).await;
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self.http.spawn(fut);
    }

    #[cfg(target_arch = "wasm32")]
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()>>>) {
        self.http.spawn(fut);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::AtomicUsize;

    use bytes::Bytes;
    use futures_util::stream;

    use super::*;

    #[derive(Clone, Default)]
    struct HeldObserveTransport {
        opens: Arc<StdMutex<Vec<Vec<String>>>>,
        cancels: Arc<AtomicUsize>,
    }

    impl ObserveTransport for HeldObserveTransport {
        async fn open_observe_stream(
            &self,
            subscriptions: Vec<MultiplexObserveSubscription>,
        ) -> Result<super::super::http::ProtocolHttpStream, LixError> {
            self.opens.lock().expect("opens").push(
                subscriptions
                    .iter()
                    .map(|subscription| subscription.id.clone())
                    .collect(),
            );
            let frames = subscriptions
                .iter()
                .map(|subscription| {
                    let id = &subscription.id;
                    format!(
                        "event: next\ndata: {{\"subscriptionId\":\"{id}\",\"sequence\":0,\"mutationSequence\":41,\"result\":{{\"columns\":[{{\"name\":\"value\",\"type\":\"text\"}}],\"rows\":[[{{\"kind\":\"text\",\"value\":\"transport\"}}]],\"rowsAffected\":0,\"notices\":[]}}}}\n\n"
                    )
                })
                .collect::<String>();
            let cancels = self.cancels.clone();
            Ok(super::super::http::ProtocolHttpStream {
                status: 200,
                headers: vec![("content-type".to_owned(), "text/event-stream".to_owned())],
                body: Box::pin(
                    stream::once(async move { Ok(Bytes::from(frames)) })
                        .chain(stream::pending()),
                ),
                cancel: Arc::new(move || {
                    cancels.fetch_add(1, Ordering::SeqCst);
                }),
            })
        }

        async fn recover_session(&self) -> Result<(), LixError> {
            Ok(())
        }

        async fn sleep(&self, _duration: Duration) {}

        fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) {
            tokio::spawn(fut);
        }
    }

    #[tokio::test]
    async fn membership_restart_cancels_an_active_stream() {
        let transport = HeldObserveTransport::default();
        let hub = ObservationHub::new(transport.clone());
        let first = hub.observe("SELECT 'first'", Vec::new()).await.expect("first");
        let first_event = tokio::time::timeout(Duration::from_secs(1), first.next())
            .await
            .expect("first subscription receives its frame")
            .expect("first subscription result")
            .expect("first subscription remains open");
        assert_eq!(
            first_event.rows.rows()[0].values(),
            &[Value::Text("transport".to_owned())]
        );
        assert_eq!(first_event.mutation_sequence, 41);

        let second = hub
            .observe("SELECT 'second'", Vec::new())
            .await
            .expect("second");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let latest = transport.opens.lock().expect("opens").last().cloned();
                if latest == Some(vec!["observe-1".to_owned(), "observe-2".to_owned()])
                    && transport.cancels.load(Ordering::SeqCst) >= 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement generation opens with complete membership");

        let second_event = tokio::time::timeout(Duration::from_secs(1), second.next())
            .await
            .expect("new subscription receives its first frame")
            .expect("new subscription result")
            .expect("new subscription remains open");
        assert_eq!(
            second_event.rows.rows()[0].values(),
            &[Value::Text("transport".to_owned())]
        );

        first.close();
        second.close();
        hub.close().await;
    }

    #[tokio::test]
    async fn history_shaped_add_remove_add_converges_to_latest_membership() {
        let transport = HeldObserveTransport::default();
        let hub = ObservationHub::new(transport.clone());
        let checkpoints = hub
            .observe("SELECT * FROM lix_checkpoint", Vec::new())
            .await
            .expect("checkpoints");
        let empty_parent = hub
            .observe("SELECT * FROM lix_commit WHERE id = ''", Vec::new())
            .await
            .expect("empty parent");

        tokio::time::timeout(Duration::from_secs(1), checkpoints.next())
            .await
            .expect("checkpoint frame")
            .expect("checkpoint result")
            .expect("checkpoint remains open");

        empty_parent.close();
        let real_parent = hub
            .observe("SELECT * FROM lix_commit WHERE id = 'oldest'", Vec::new())
            .await
            .expect("real parent");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let latest = transport.opens.lock().expect("opens").last().cloned();
                if latest == Some(vec!["observe-1".to_owned(), "observe-3".to_owned()])
                    && transport.cancels.load(Ordering::SeqCst) >= 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver converges to checkpoint and real-parent membership");

        tokio::time::timeout(Duration::from_secs(1), real_parent.next())
            .await
            .expect("real-parent frame")
            .expect("real-parent result")
            .expect("real parent remains open");
        tokio::time::timeout(Duration::from_secs(1), async {
            while empty_parent
                .next()
                .await
                .expect("closed empty-parent read succeeds")
                .is_some()
            {}
        })
        .await
        .expect("closed empty-parent read eventually settles");

        checkpoints.close();
        real_parent.close();
        hub.close().await;
    }
}
