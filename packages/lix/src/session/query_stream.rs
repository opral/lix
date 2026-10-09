//! Pull-based streaming reads.
//!
//! A [`QueryStream`] runs one read statement once against one pinned storage
//! snapshot and hands its rows out in byte-bounded pages. The snapshot, the
//! DataFusion plan and its batch stream live inside a *producer* future: the
//! producer owns everything it borrows, so no self-referential handle escapes
//! the scoped read. The producer is only polled while the caller awaits
//! [`QueryStream::next_page`], and it parks on a one-page channel, so an idle
//! stream does no work and holds at most one finished page plus the batch
//! it is cutting.
//!
//! Lifecycle:
//! - an idle stream holds no session operation guard, so writes on the same
//!   session are never blocked by it; each pull takes the guard only while it
//!   produces a page, exactly like a buffered read;
//! - [`QueryStream::cancel`], dropping the stream and closing its session all
//!   drop the producer, which releases the read snapshot synchronously.

use std::collections::HashMap;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

use tokio::sync::mpsc;

use crate::common::public_row_bytes;
use crate::{ExecuteResult, LixError, LixNotice, ResultColumnType, Value};

/// Page size used when the caller does not choose one.
pub const DEFAULT_QUERY_STREAM_PAGE_BYTES: usize = 1024 * 1024;
/// Largest accepted page size: one page never exceeds the buffered budget.
pub const MAX_QUERY_STREAM_PAGE_BYTES: usize = crate::common::MAX_READ_RESULT_BYTES;

pub(crate) fn validate_page_bytes(page_bytes: usize) -> Result<usize, LixError> {
    if page_bytes == 0 || page_bytes > MAX_QUERY_STREAM_PAGE_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "query stream page size must be between 1 and {MAX_QUERY_STREAM_PAGE_BYTES} bytes"
            ),
        ));
    }
    Ok(page_bytes)
}

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
    Page(ExecuteResult),
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

/// A read statement streamed in byte-bounded pages from one pinned snapshot.
///
/// Created by [`crate::Lix::query_stream`]. Pages use the same row and value
/// representation as [`ExecuteResult`]. Streams are read-only and are not
/// subject to the buffered read budget or the buffered read deadline.
pub struct QueryStream {
    header: QueryStreamHeader,
    receiver: mpsc::Receiver<QueryStreamMessage>,
    producer: Arc<ProducerSlot>,
    admission: Option<QueryStreamPullAdmission>,
    _registration: QueryStreamRegistration,
    done: bool,
}

impl fmt::Debug for QueryStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryStream")
            .field("columns", &self.header.columns)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl QueryStream {
    /// Registers `producer` and pulls until it reports the result header.
    /// Planning and setup errors are returned here rather than from the first
    /// page.
    pub(crate) async fn open(
        registry: &Arc<QueryStreamRegistry>,
        ensure_open: impl FnOnce() -> Result<(), LixError>,
        receiver: mpsc::Receiver<QueryStreamMessage>,
        producer: impl Future<Output = ()> + Send + 'static,
        admission: Option<QueryStreamPullAdmission>,
    ) -> Result<Self, LixError> {
        let producer = Arc::new(ProducerSlot {
            state: Mutex::new(ProducerState::Running(Box::pin(producer))),
        });
        let registration = registry.register(&producer);
        // Close transitions the session before it cancels registered streams,
        // so a stream registered while the session was still open is either
        // rejected here or cancelled by that close.
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
            _registration: registration,
            done: false,
        };
        match stream.next_message().await? {
            Some(QueryStreamMessage::Header(header)) => {
                stream.header = header;
                Ok(stream)
            }
            Some(QueryStreamMessage::Error(error)) => Err(error),
            Some(QueryStreamMessage::Page(_)) | None => Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "query stream producer ended before reporting its columns",
            )),
        }
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

    /// Returns the next page, or `None` once the result is exhausted.
    ///
    /// Pages are never empty. Each page's rows occupy at most the stream's
    /// page size in public value bytes, unless a single row is larger. An
    /// error ends the stream; errors after the first page are never retried.
    pub async fn next_page(&mut self) -> Result<Option<ExecuteResult>, LixError> {
        match self.next_message().await? {
            Some(QueryStreamMessage::Page(page)) => Ok(Some(page)),
            Some(QueryStreamMessage::Error(error)) => {
                self.finish();
                Err(error)
            }
            Some(QueryStreamMessage::Header(_)) => {
                self.finish();
                Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "query stream producer reported its columns twice",
                ))
            }
            None => Ok(None),
        }
    }

    /// Stops the stream and releases its read snapshot. Later pulls return
    /// `None`. Dropping the stream has the same effect.
    pub fn cancel(&mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        self.done = true;
        self.producer.cancel(LixError::new(
            LixError::CODE_CLOSED,
            "query stream was cancelled",
        ));
        self.receiver.close();
    }

    async fn next_message(&mut self) -> Result<Option<QueryStreamMessage>, LixError> {
        if self.done {
            return Ok(None);
        }
        let _guard = match &self.admission {
            Some(admission) => match admission().await {
                Ok(guard) => Some(guard),
                Err(error) => {
                    self.finish();
                    return Err(error);
                }
            },
            None => None,
        };
        let message = poll_fn(|cx| self.poll_message(cx)).await;
        match message {
            Ok(Some(message)) => Ok(Some(message)),
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(error) => {
                self.finish();
                Err(error)
            }
        }
    }

    fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<QueryStreamMessage>, LixError>> {
        // Drive the producer first: it is the only thing that fills the
        // channel, and it parks once the one-page window is full.
        if let Err(reason) = self.producer.poll(cx) {
            return Poll::Ready(Err(reason));
        }
        self.receiver.poll_recv(cx).map(Ok)
    }
}

impl Drop for QueryStream {
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
/// replica demand hydration): they keep the buffered budget and deadline, and
/// the stream only bounds how much the caller receives at once.
pub(crate) async fn produce_materialized_pages(
    result: impl Future<Output = Result<ExecuteResult, LixError>>,
    page_bytes: usize,
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
    let columns = result.columns().to_vec();
    let column_types = result.column_types().to_vec();
    let mut page = Vec::new();
    let mut page_row_bytes = 0usize;
    for row in result.rows() {
        let values = row.values();
        let bytes = public_row_bytes(values);
        if !page.is_empty() && page_row_bytes.saturating_add(bytes) > page_bytes {
            let rows = std::mem::take(&mut page);
            page_row_bytes = 0;
            let page = materialized_page(&columns, &column_types, rows);
            if send_query_stream_message(&sender, QueryStreamMessage::Page(page))
                .await
                .is_err()
            {
                return;
            }
        }
        page_row_bytes = page_row_bytes.saturating_add(bytes);
        page.push(values.to_vec());
    }
    if !page.is_empty() {
        let page = materialized_page(&columns, &column_types, page);
        let _ = send_query_stream_message(&sender, QueryStreamMessage::Page(page)).await;
    }
}

fn materialized_page(
    columns: &[String],
    column_types: &[ResultColumnType],
    rows: Vec<Vec<Value>>,
) -> ExecuteResult {
    ExecuteResult::from_typed_rows(columns.to_vec(), column_types.to_vec(), rows)
}
