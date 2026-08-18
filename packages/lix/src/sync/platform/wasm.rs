//! Browser mechanics for the shared synchronization state machine.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};

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
            sleep(Duration::from_millis(10)).await?;
        }
        Ok(())
    }
}

pub(in crate::sync) async fn sleep(duration: Duration) -> Result<(), LixError> {
    let promise = Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        if let Ok(timer) = Reflect::get(&global, &"setTimeout".into())
            && let Ok(timer) = timer.dyn_into::<Function>()
        {
            let _ = timer.call2(
                &global,
                &resolve,
                &JsValue::from_f64(duration.as_millis() as f64),
            );
        }
    });
    JsFuture::from(promise).await.map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("browser sync timer failed: {error:?}"),
        )
    })?;
    Ok(())
}

pub(in crate::sync) fn deadline(
    duration: Duration,
) -> impl Future<Output = Result<(), LixError>> + Send {
    // Browser timers carry JavaScript Promise state and therefore are not
    // marked `Send`. Browser WASM is single-threaded, while the shared Lix
    // session surface intentionally retains its native `Send` future shape.
    // Keep that target-specific proof at the adapter boundary.
    unsafe { crate::session::AssumeSendFuture::new(sleep(duration)) }
}
