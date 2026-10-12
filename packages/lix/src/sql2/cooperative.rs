//! Cooperative yielding for long SQL execution.
//!
//! A single poll of a query stream can run for seconds: a sort or an
//! aggregate pulls its whole input inside one poll whenever every input batch
//! is ready at once, which is the norm for in-memory and cached storage. Two
//! things depend on that poll returning regularly:
//!
//! - cancellation, which drops the query future between polls, and
//! - on the browser, the worker's event loop, which only delivers messages
//!   (a close, a cancel, the next request) once the engine returns to it.
//!
//! [`YieldingExec`] wraps every leaf of an executed plan. Before a leaf hands
//! out its next batch it checks whether the current time slice is spent; if
//! so it returns `Pending` once and schedules its own wake: immediately on
//! native executors, and through a macrotask on the browser so queued
//! messages run first. Operators above pass the `Pending` up unchanged.

use std::cell::Cell;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream, Statistics,
    StatisticsArgs, StatisticsContext,
};
use futures_util::Stream;
use web_time::Instant;

/// How long engine work runs before it yields once. Short enough that a
/// cancel or a message waits a few milliseconds; long enough that the yield
/// itself (a reschedule, or one macrotask on the browser) is noise.
const TIME_SLICE: Duration = Duration::from_millis(10);

thread_local! {
    /// Start of the current slice on this thread; `None` right after a yield.
    static SLICE_START: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Returns `Pending` (with a wake already scheduled) when this thread's time
/// slice is spent, otherwise `Ready`.
pub(crate) fn poll_yield(cx: &mut Context<'_>) -> Poll<()> {
    let now = Instant::now();
    let due = SLICE_START.with(|start| match start.get() {
        Some(started) => now.duration_since(started) >= TIME_SLICE,
        None => {
            start.set(Some(now));
            false
        }
    });
    if !due {
        return Poll::Ready(());
    }
    SLICE_START.with(|start| start.set(None));
    platform::schedule_wake(cx.waker().clone());
    Poll::Pending
}

#[cfg(not(target_family = "wasm"))]
mod platform {
    pub(super) fn schedule_wake(waker: std::task::Waker) {
        // Executors requeue the task; a `select!` around the query (a cancel,
        // a timeout) gets its turn before the query runs again.
        waker.wake();
    }
}

#[cfg(target_family = "wasm")]
mod platform {
    use std::cell::RefCell;
    use std::task::Waker;

    use js_sys::{Function, Reflect};
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::prelude::JsValue;

    /// Queues one macrotask that wakes every yielded task, after the
    /// messages and timers already queued.
    ///
    /// Node's `setImmediate` runs after the event loop's I/O and message
    /// phase. A `MessageChannel` is the browser's equivalent, but Node
    /// drains up to a thousand port messages per loop turn, so a message
    /// posted from its own handler would starve timers and worker messages
    /// there: `setImmediate` is preferred whenever it exists.
    enum Macrotask {
        Immediate {
            set_immediate: Function,
        },
        Channel {
            port: JsValue,
            post: Function,
        },
    }

    struct Scheduler {
        macrotask: Macrotask,
        wake_all: Closure<dyn FnMut()>,
    }

    thread_local! {
        static WAITING: RefCell<Vec<Waker>> = const { RefCell::new(Vec::new()) };
        static SCHEDULER: Option<Scheduler> = scheduler();
    }

    fn wake_waiting() {
        let waiting = WAITING.with(|waiting| std::mem::take(&mut *waiting.borrow_mut()));
        for waker in waiting {
            waker.wake();
        }
    }

    fn global_function(name: &str) -> Option<Function> {
        Reflect::get(&js_sys::global(), &name.into())
            .ok()?
            .dyn_into::<Function>()
            .ok()
    }

    fn scheduler() -> Option<Scheduler> {
        let wake_all = Closure::<dyn FnMut()>::new(wake_waiting);
        if let Some(set_immediate) = global_function("setImmediate") {
            return Some(Scheduler {
                macrotask: Macrotask::Immediate { set_immediate },
                wake_all,
            });
        }
        let constructor = global_function("MessageChannel")?;
        let channel = Reflect::construct(&constructor, &js_sys::Array::new()).ok()?;
        let receiver = Reflect::get(&channel, &"port1".into()).ok()?;
        let sender = Reflect::get(&channel, &"port2".into()).ok()?;
        Reflect::set(&receiver, &"onmessage".into(), wake_all.as_ref()).ok()?;
        let post = Reflect::get(&sender, &"postMessage".into())
            .ok()?
            .dyn_into::<Function>()
            .ok()?;
        Some(Scheduler {
            macrotask: Macrotask::Channel { port: sender, post },
            wake_all,
        })
    }

    pub(super) fn schedule_wake(waker: Waker) {
        let first = WAITING.with(|waiting| {
            let mut waiting = waiting.borrow_mut();
            waiting.push(waker);
            waiting.len() == 1
        });
        if !first {
            return;
        }
        let queued = SCHEDULER.with(|scheduler| {
            scheduler.as_ref().is_some_and(|scheduler| match &scheduler.macrotask {
                Macrotask::Immediate { set_immediate } => set_immediate
                    .call1(&JsValue::UNDEFINED, scheduler.wake_all.as_ref())
                    .is_ok(),
                Macrotask::Channel { port, post } => post.call1(port, &JsValue::NULL).is_ok(),
            })
        });
        if !queued {
            // Without a macrotask source, fall back to an immediate wake: the
            // query still yields to other futures, just not to messages.
            wake_waiting();
        }
    }
}

/// Wraps a leaf so its stream yields once per time slice. See the module docs.
#[derive(Debug)]
pub(crate) struct YieldingExec {
    input: Arc<dyn ExecutionPlan>,
}

impl YieldingExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for YieldingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "YieldingExec")
    }
}

impl ExecutionPlan for YieldingExec {
    fn name(&self) -> &'static str {
        "YieldingExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return datafusion::common::internal_err!("YieldingExec has exactly one child");
        }
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(YieldingStream {
            inner: self.input.execute(partition, context)?,
        }))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        StatisticsContext::new().compute(
            self.input.as_ref(),
            &StatisticsArgs::new().with_partition(partition),
        )
    }
}

struct YieldingStream {
    inner: SendableRecordBatchStream,
}

impl Stream for YieldingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if poll_yield(cx).is_pending() {
            return Poll::Pending;
        }
        self.inner.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for YieldingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// Wraps every leaf of `plan` in a [`YieldingExec`]. DataFusion's own
/// `CooperativeExec` over a leaf is replaced, since it only yields to a Tokio
/// budget. Idempotent: an already wrapped leaf is left alone.
///
/// A statement scan cache is wrapped whole, never rebuilt: replacing its
/// child would give each consumer its own cache. Its input's leaves are
/// wrapped when the cache is created.
pub(crate) fn wrap_leaves(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<YieldingExec>().is_some() {
        return Ok(plan);
    }
    if plan.name() == "StatementScanCacheExec" {
        return Ok(Arc::new(YieldingExec::new(plan)));
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(Arc::new(YieldingExec::new(plan)));
    }
    if plan.name() == "CooperativeExec"
        && let [child] = children.as_slice()
        && child.children().is_empty()
    {
        return Ok(Arc::new(YieldingExec::new(Arc::clone(child))));
    }
    let mut changed = false;
    let mut wrapped = Vec::with_capacity(children.len());
    for child in children {
        let next = wrap_leaves(Arc::clone(child))?;
        changed |= !Arc::ptr_eq(child, &next);
        wrapped.push(next);
    }
    if !changed {
        return Ok(plan);
    }
    plan.replace_children(
        wrapped,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};
    use std::time::{Duration, Instant};

    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures_util::StreamExt;

    /// Polls a long aggregate with no Tokio context, as the browser does:
    /// DataFusion's own `CooperativeExec` never yields there, so every
    /// `Pending` comes from `YieldingExec`.
    #[test]
    fn a_wrapped_plan_yields_once_per_slice_without_tokio() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let plan = futures_lite::future::block_on(async {
            ctx.sql("SELECT count(*) FROM generate_series(1, 100000000000)")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap()
        });
        let plan = super::wrap_leaves(plan).unwrap();
        let mut stream =
            datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).unwrap();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let started = Instant::now();
        let mut longest = Duration::ZERO;
        let mut yields = 0;
        while started.elapsed() < Duration::from_millis(200) {
            let poll_started = Instant::now();
            let poll = stream.poll_next_unpin(&mut cx);
            longest = longest.max(poll_started.elapsed());
            assert!(matches!(poll, Poll::Pending), "the aggregate cannot finish this soon");
            yields += 1;
        }
        assert!(yields > 2, "only {yields} yields");
        assert!(longest < Duration::from_millis(100), "one poll ran for {longest:?}");
    }
}
