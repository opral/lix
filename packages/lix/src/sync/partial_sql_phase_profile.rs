//! Optional diagnostic pass over existing lix_perf spans. Inclusive span time
//! and time while entered are not OS CPU time and nested values must not be
//! added. Run this separately from the uninstrumented latency comparison.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::Subscriber;
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Default, Debug, serde::Serialize)]
pub(super) struct Metric {
    calls: u64,
    inclusive_ns: u64,
    entered_ns: u64,
}
#[derive(Clone, Default)]
struct Collector {
    metrics: Arc<Mutex<BTreeMap<&'static str, Metric>>>,
}
struct Timing {
    name: &'static str,
    created: Instant,
    entered: Vec<Instant>,
    entered_ns: u64,
}
impl<S> Layer<S> for Collector
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> Interest {
        if metadata.target() == "lix_perf" {
            Interest::always()
        } else {
            Interest::never()
        }
    }
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        if attrs.metadata().target() != "lix_perf" {
            return;
        }
        if let Some(span) = context.span(id) {
            span.extensions_mut().insert(Timing {
                name: attrs.metadata().name(),
                created: Instant::now(),
                entered: Vec::new(),
                entered_ns: 0,
            });
        }
    }
    fn on_enter(&self, id: &Id, context: Context<'_, S>) {
        if let Some(span) = context.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<Timing>() {
                timing.entered.push(Instant::now());
            }
        }
    }
    fn on_exit(&self, id: &Id, context: Context<'_, S>) {
        if let Some(span) = context.span(id) {
            if let Some(timing) = span.extensions_mut().get_mut::<Timing>() {
                if let Some(started) = timing.entered.pop() {
                    timing.entered_ns += started.elapsed().as_nanos() as u64;
                }
            }
        }
    }
    fn on_close(&self, id: Id, context: Context<'_, S>) {
        let Some(span) = context.span(&id) else {
            return;
        };
        let Some(timing) = span.extensions_mut().remove::<Timing>() else {
            return;
        };
        let mut metrics = self.metrics.lock().unwrap();
        let metric = metrics.entry(timing.name).or_default();
        metric.calls += 1;
        metric.inclusive_ns += timing.created.elapsed().as_nanos() as u64;
        metric.entered_ns += timing.entered_ns;
    }
}
pub(super) async fn capture<T>(
    future: impl Future<Output = T>,
) -> (T, BTreeMap<&'static str, Metric>) {
    let collector = Collector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    let result = future.with_subscriber(dispatch).await;
    let metrics = std::mem::take(&mut *collector.metrics.lock().unwrap());
    (result, metrics)
}
