//! Pull-based streaming reads.
//!
//! A [`RowStream`] runs one read statement once against one pinned storage
//! snapshot and hands its rows out in bounded pages. The snapshot, the
//! DataFusion plan and its batch stream live inside a *producer* future: the
//! producer owns everything it borrows, so no self-referential handle escapes
//! the scoped read. The producer is only polled while the consumer pulls, and
//! it parks on a one-page channel, so an idle stream does no work and holds at
//! most one finished page plus the batch it is cutting.
//!
//! Opening a stream pulls until the producer has planned the statement and
//! delivered its first page (at most [`PageLimits::first_rows`] rows), so
//! planning errors surface at open. The stream ends normally only after the
//! producer confirms the end of the result; a producer that stops without that
//! confirmation surfaces an error instead of a silently truncated result.
//!
//! Lifecycle:
//! - an idle stream holds no session operation guard and never expires, so
//!   a slow consumer neither blocks writes nor loses its snapshot;
//! - [`RowStream::close`], dropping the stream, and closing its session or
//!   snapshot all drop the producer, which stops the engine and releases the
//!   read snapshot synchronously. The engine runs no detached tasks for a
//!   stream (single-partition plans), so a dropped producer has stopped.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

use tokio::sync::mpsc;

use crate::common::public_row_bytes;
use crate::{ExecuteResult, LixError, LixNotice, ResultColumnType, Row};

pub(crate) use crate::sql2::PageLimits;

type Producer = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Held for the duration of one pull; dropping it releases the session
/// operation it represents.
pub(crate) type QueryStreamPullGuard = Box<dyn Send>;

/// Admits one pull. Live streams use it to take a session operation guard
/// only while their producer is polled.
pub(crate) type QueryStreamPullAdmission = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<QueryStreamPullGuard, LixError>> + Send>>
        + Send
        + Sync,
>;

pub(crate) struct QueryStreamHeader {
    pub(crate) columns: Vec<String>,
    pub(crate) column_types: Vec<ResultColumnType>,
    pub(crate) notices: Vec<LixNotice>,
}

pub(crate) enum QueryStreamMessage {
    Header(QueryStreamHeader),
    Page(Vec<Row>),
    /// The producer reached the end of the result. Only this ends a stream
    /// normally.
    End,
    Error(LixError),
}

pub(crate) type QueryStreamSender = mpsc::Sender<QueryStreamMessage>;

/// Returned by a producer when its consumer went away; never surfaced.
pub(crate) fn query_stream_consumer_gone() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "query stream consumer was dropped")
}

pub(crate) async fn send_query_stream_message(
    sender: &QueryStreamSender,
    message: QueryStreamMessage,
) -> Result<(), LixError> {
    sender
        .send(message)
        .await
        .map_err(|_| query_stream_consumer_gone())
}

enum ProducerState {
    Running(Producer),
    Finished,
    Cancelled(LixError),
}

struct ProducerSlot {
    state: Mutex<ProducerState>,
}

impl ProducerSlot {
    fn lock(&self) -> MutexGuard<'_, ProducerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Drops a running producer, releasing its read snapshot, and records why.
    fn cancel(&self, reason: LixError) {
        let previous = {
            let mut state = self.lock();
            match &*state {
                ProducerState::Running(_) => {
                    std::mem::replace(&mut *state, ProducerState::Cancelled(reason))
                }
                ProducerState::Finished | ProducerState::Cancelled(_) => return,
            }
        };
        drop(previous);
    }

    #[cfg(test)]
    fn is_running(&self) -> bool {
        matches!(&*self.lock(), ProducerState::Running(_))
    }

    /// Polls the producer. Returns the cancellation reason when the stream was
    /// cancelled before the consumer saw its end.
    fn poll(&self, cx: &mut Context<'_>) -> Result<(), LixError> {
        let mut state = self.lock();
        match &mut *state {
            ProducerState::Running(producer) => {
                if producer.as_mut().poll(cx).is_ready() {
                    *state = ProducerState::Finished;
                }
                Ok(())
            }
            ProducerState::Finished => Ok(()),
            ProducerState::Cancelled(reason) => Err(reason.clone()),
        }
    }
}

/// Every open stream of one session, so closing the session can cancel them.
#[derive(Default)]
pub(crate) struct QueryStreamRegistry {
    state: Mutex<QueryStreamRegistryState>,
}

#[derive(Default)]
struct QueryStreamRegistryState {
    next_id: u64,
    streams: HashMap<u64, Weak<ProducerSlot>>,
}

impl QueryStreamRegistry {
    fn lock(&self) -> MutexGuard<'_, QueryStreamRegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn register(self: &Arc<Self>, slot: &Arc<ProducerSlot>) -> QueryStreamRegistration {
        let mut state = self.lock();
        let id = state.next_id;
        state.next_id += 1;
        state.streams.insert(id, Arc::downgrade(slot));
        QueryStreamRegistration {
            registry: Arc::downgrade(self),
            id,
        }
    }

    /// Cancels every open stream; their next pull returns `reason`.
    pub(crate) fn cancel_all(&self, reason: impl Fn() -> LixError) {
        let slots = self
            .lock()
            .streams
            .drain()
            .filter_map(|(_, slot)| slot.upgrade())
            .collect::<Vec<_>>();
        for slot in slots {
            slot.cancel(reason());
        }
    }

    /// Streams whose producer still pins a read snapshot.
    #[cfg(test)]
    pub(crate) fn running_count(&self) -> usize {
        self.lock()
            .streams
            .values()
            .filter_map(Weak::upgrade)
            .filter(|slot| slot.is_running())
            .count()
    }
}

struct QueryStreamRegistration {
    registry: Weak<QueryStreamRegistry>,
    id: u64,
}

impl Drop for QueryStreamRegistration {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.lock().streams.remove(&self.id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamState {
    Open,
    /// The producer confirmed the end of the result.
    Ended,
    /// An error, close or cancellation ended the stream.
    Stopped,
}

/// A read statement streamed in bounded pages from one pinned snapshot.
///
/// Created by [`crate::Query::stream`]. The stream is a
/// [`futures_core::Stream`] of rows; [`RowStream::next_page`] pulls the same
/// cursor a page at a time. It never expires: a slow consumer keeps its
/// snapshot for as long as it holds the stream. Errors after the first page
/// (for example a runtime error on a later row) surface from a later pull and
/// end the stream.
pub struct RowStream {
    header: QueryStreamHeader,
    receiver: mpsc::Receiver<QueryStreamMessage>,
    producer: Arc<ProducerSlot>,
    admission: Option<QueryStreamPullAdmission>,
    pending_admission: Option<PullAdmissionFuture>,
    pull_guard: Option<QueryStreamPullGuard>,
    _registrations: Vec<QueryStreamRegistration>,
    buffered: VecDeque<Row>,
    state: StreamState,
}

type PullAdmissionFuture =
    Pin<Box<dyn Future<Output = Result<QueryStreamPullGuard, LixError>> + Send>>;

impl fmt::Debug for RowStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RowStream")
            .field("columns", &self.header.columns)
            .field("buffered_rows", &self.buffered.len())
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl RowStream {
    /// Registers `producer` with every registry that may cancel it, then
    /// pulls until the producer has reported its columns and first page.
    /// Planning, setup and first-page errors are returned here.
    pub(crate) async fn open(
        registries: &[&Arc<QueryStreamRegistry>],
        ensure_open: impl FnOnce() -> Result<(), LixError>,
        receiver: mpsc::Receiver<QueryStreamMessage>,
        producer: impl Future<Output = ()> + Send + 'static,
        admission: Option<QueryStreamPullAdmission>,
    ) -> Result<Self, LixError> {
        let producer = Arc::new(ProducerSlot {
            state: Mutex::new(ProducerState::Running(Box::pin(producer))),
        });
        let registrations = registries
            .iter()
            .map(|registry| registry.register(&producer))
            .collect();
        // Close transitions the session (or snapshot) before it cancels
        // registered streams, so a stream registered while it was still open
        // is either rejected here or cancelled by that close.
        ensure_open()?;
        let mut stream = Self {
            header: QueryStreamHeader {
                columns: Vec::new(),
                column_types: Vec::new(),
                notices: Vec::new(),
            },
            receiver,
            producer,
            admission,
            pending_admission: None,
            pull_guard: None,
            _registrations: registrations,
            buffered: VecDeque::new(),
            state: StreamState::Open,
        };
        match poll_fn(|cx| stream.poll_message(cx)).await? {
            QueryStreamMessage::Header(header) => stream.header = header,
            QueryStreamMessage::Error(error) => {
                stream.stop();
                return Err(error);
            }
            QueryStreamMessage::Page(_) | QueryStreamMessage::End => {
                stream.stop();
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "query stream producer ended before reporting its columns",
                ));
            }
        }
        if let Some(page) = poll_fn(|cx| stream.poll_page(cx)).await? {
            stream.buffered.extend(page);
        }
        Ok(stream)
    }

    /// Result-set column names, in row value order.
    pub fn columns(&self) -> &[String] {
        &self.header.columns
    }

    /// Stable SQL type of each result-set column.
    pub fn column_types(&self) -> &[ResultColumnType] {
        &self.header.column_types
    }

    /// Non-fatal diagnostics produced while planning the statement.
    pub fn notices(&self) -> &[LixNotice] {
        &self.header.notices
    }

    /// Returns the next row, or `None` once the producer confirmed the end
    /// of the result. Cancel-safe: dropping the future loses no row.
    pub async fn try_next(&mut self) -> Result<Option<Row>, LixError> {
        poll_fn(|cx| self.poll_row(cx)).await.transpose()
    }

    /// Returns the rest of the current page, or the next page; `None` once
    /// the result is exhausted. Pages are never empty. The first page holds
    /// at most 64 rows, later pages at most 4096 rows and 1 MiB of values
    /// (a single larger row is a page of its own).
    pub async fn next_page(&mut self) -> Result<Option<Vec<Row>>, LixError> {
        if !self.buffered.is_empty() {
            return Ok(Some(self.buffered.drain(..).collect()));
        }
        poll_fn(|cx| self.poll_page(cx)).await
    }

    /// Stops the stream, waiting until the engine has stopped producing and
    /// the read snapshot is released. Dropping the stream also stops it.
    pub async fn close(mut self) -> Result<(), LixError> {
        self.stop();
        Ok(())
    }

    fn stop(&mut self) {
        if self.state == StreamState::Open {
            self.state = StreamState::Stopped;
        }
        self.buffered.clear();
        self.pending_admission = None;
        self.pull_guard = None;
        self.producer.cancel(LixError::new(
            LixError::CODE_CLOSED,
            "query stream was closed",
        ));
        self.receiver.close();
    }

    fn poll_row(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Row, LixError>>> {
        loop {
            if let Some(row) = self.buffered.pop_front() {
                return Poll::Ready(Some(Ok(row)));
            }
            match std::task::ready!(self.poll_page(cx)) {
                Ok(Some(page)) => self.buffered.extend(page),
                Ok(None) => return Poll::Ready(None),
                Err(error) => return Poll::Ready(Some(Err(error))),
            }
        }
    }

    /// Pulls one page from the producer, ignoring buffered rows.
    fn poll_page(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Vec<Row>>, LixError>> {
        if self.state != StreamState::Open {
            return Poll::Ready(Ok(None));
        }
        loop {
            let message = match std::task::ready!(self.poll_message(cx)) {
                Ok(message) => message,
                Err(error) => {
                    self.stop();
                    return Poll::Ready(Err(error));
                }
            };
            return match message {
                // Producers never send empty pages; skip one if they do.
                QueryStreamMessage::Page(rows) if rows.is_empty() => continue,
                QueryStreamMessage::Page(rows) => Poll::Ready(Ok(Some(rows))),
                QueryStreamMessage::End => {
                    self.state = StreamState::Ended;
                    self.producer.cancel(LixError::new(
                        LixError::CODE_CLOSED,
                        "query stream finished",
                    ));
                    Poll::Ready(Ok(None))
                }
                QueryStreamMessage::Error(error) => {
                    self.stop();
                    Poll::Ready(Err(error))
                }
                QueryStreamMessage::Header(_) => {
                    self.stop();
                    Poll::Ready(Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "query stream producer reported its columns twice",
                    )))
                }
            };
        }
    }

    /// Admits the pull (taking the session operation guard), drives the
    /// producer and receives its next message. The guard is held only while a
    /// message is outstanding, so an idle stream blocks nothing.
    fn poll_message(&mut self, cx: &mut Context<'_>) -> Poll<Result<QueryStreamMessage, LixError>> {
        if self.pull_guard.is_none()
            && let Some(admission) = &self.admission
        {
            let pending = self.pending_admission.get_or_insert_with(|| admission());
            let guard = std::task::ready!(pending.as_mut().poll(cx));
            self.pending_admission = None;
            self.pull_guard = Some(guard?);
        }
        // Drive the producer first: it is the only thing that fills the
        // channel, and it parks once the one-page window is full.
        if let Err(reason) = self.producer.poll(cx) {
            self.pull_guard = None;
            return Poll::Ready(Err(reason));
        }
        let message = std::task::ready!(self.receiver.poll_recv(cx));
        self.pull_guard = None;
        Poll::Ready(message.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "query stream producer stopped without confirming the end of the result",
            )
        }))
    }
}

impl futures_core::Stream for RowStream {
    type Item = Result<Row, LixError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_row(cx)
    }
}

impl futures_core::FusedStream for RowStream {
    fn is_terminated(&self) -> bool {
        self.state != StreamState::Open && self.buffered.is_empty()
    }
}

impl Drop for RowStream {
    fn drop(&mut self) {
        self.producer.cancel(LixError::new(
            LixError::CODE_CLOSED,
            "query stream was dropped",
        ));
    }
}

/// Waits before a retry without binding the producer to one async runtime:
/// bindings may open a stream on one executor and pull it on another.
pub(crate) async fn retry_delay(duration: std::time::Duration) {
    #[cfg(not(target_family = "wasm"))]
    futures_timer::Delay::new(duration).await;
    #[cfg(target_family = "wasm")]
    crate::sync::sleep(duration).await;
}

/// The one-page window between a producer and its consumer.
pub(crate) fn query_stream_channel() -> (QueryStreamSender, mpsc::Receiver<QueryStreamMessage>) {
    mpsc::channel(1)
}


/// Pages an already materialized result. Used for statements whose execution
/// must finish before rows exist (filesystem content hydration, partial
/// replica demand hydration, transaction overlays): the read runs buffered
/// under the default byte cap, and the stream only bounds how much the
/// caller receives at once.
pub(crate) async fn produce_materialized_pages(
    result: impl Future<Output = Result<ExecuteResult, LixError>>,
    limits: PageLimits,
    sender: QueryStreamSender,
) {
    let result = match result.await {
        Ok(result) => result,
        Err(error) => {
            let _ = sender.send(QueryStreamMessage::Error(error)).await;
            return;
        }
    };
    let header = QueryStreamHeader {
        columns: result.columns().to_vec(),
        column_types: result.column_types().to_vec(),
        notices: result.notices().to_vec(),
    };
    if send_query_stream_message(&sender, QueryStreamMessage::Header(header))
        .await
        .is_err()
    {
        return;
    }
    let mut page = Vec::new();
    let mut page_bytes = 0usize;
    let mut page_index = 0usize;
    for row in result.into_rows() {
        let bytes = public_row_bytes(row.values());
        if !page.is_empty()
            && (page_bytes.saturating_add(bytes) > limits.bytes
                || page.len() >= limits.rows_for_page(page_index))
        {
            page_bytes = 0;
            page_index += 1;
            let full = std::mem::take(&mut page);
            if send_query_stream_message(&sender, QueryStreamMessage::Page(full))
                .await
                .is_err()
            {
                return;
            }
        }
        page_bytes = page_bytes.saturating_add(bytes);
        page.push(row);
    }
    if !page.is_empty()
        && send_query_stream_message(&sender, QueryStreamMessage::Page(page))
            .await
            .is_err()
    {
        return;
    }
    let _ = send_query_stream_message(&sender, QueryStreamMessage::End).await;
}
