//! Browser mechanics for the shared synchronization state machine.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use js_sys::{Function, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::spawn_local;

use crate::LixError;

#[derive(Debug)]
pub(in crate::sync) struct SyncTask {
    finished: Arc<AtomicBool>,
}

pub(in crate::sync) fn spawn_sync_task<Worker>(worker: Worker) -> Result<SyncTask, LixError>
where
    Worker: Future<Output = ()> + 'static,
{
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    spawn_local(async move {
        worker.await;
        worker_finished.store(true, Ordering::Release);
    });
    Ok(SyncTask { finished })
}

impl SyncTask {
    pub(in crate::sync) async fn join(&self) -> Result<(), LixError> {
        while !self.finished.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }
}

pub(crate) fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
    let state = Arc::new(BrowserSleepState {
        done: AtomicBool::new(false),
        waker: Mutex::new(None),
    });
    let callback_state = Arc::clone(&state);
    let callback = Closure::once_into_js(move || {
        callback_state.done.store(true, Ordering::Release);
        if let Ok(mut waker) = callback_state.waker.lock()
            && let Some(waker) = waker.take()
        {
            waker.wake();
        }
    });
    let global = js_sys::global();
    let scheduled = Reflect::get(&global, &"setTimeout".into())
        .ok()
        .and_then(|timer| timer.dyn_into::<Function>().ok())
        .is_some_and(|timer| {
            timer
                .call2(
                    &global,
                    &callback,
                    &JsValue::from_f64(duration.as_millis() as f64),
                )
                .is_ok()
        });
    if !scheduled {
        state.done.store(true, Ordering::Release);
    }
    BrowserSleep { state }
}

struct BrowserSleepState {
    done: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

struct BrowserSleep {
    state: Arc<BrowserSleepState>,
}

impl Future for BrowserSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.state.done.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let Ok(mut waker) = self.state.waker.lock() else {
            return Poll::Ready(());
        };
        if self.state.done.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *waker = Some(context.waker().clone());
        Poll::Pending
    }
}
