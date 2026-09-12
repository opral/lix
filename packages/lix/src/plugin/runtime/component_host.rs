//! JavaScript Component execution boundary. The engine owns all host resources;
//! embedders only instantiate components and invoke their exported functions.

use super::PluginCapabilities;
use crate::{LixError, wasm::WasmLimits};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Synchronous imports supplied to a Component invocation. Resources are scoped
/// to the invocation and must not be retained after its returned future settles.
pub trait ComponentHost: Send + Sync {
    fn dispatch(&self, method: &str, arguments: Value) -> Result<Value, LixError>;
}

#[async_trait]
pub trait ComponentGuest: Send + Sync {
    /// `operation` is parse, parseChanges, serialize, serializeChanges, or merge.
    /// Requests use camelCase WIT fields, decimal strings for u64 values, and
    /// numeric opaque resource IDs. Byte buffers are arrays of octets.
    /// Return `CODE_INVALID_PLUGIN` for a guest's declared WIT error; other
    /// errors are traps and retire the instance. Enforce the supplied memory
    /// and execution limits before and throughout the guest call.
    async fn invoke(
        &self,
        operation: &str,
        input: Value,
        host: Arc<dyn ComponentHost>,
        limits: WasmLimits,
    ) -> Result<(), LixError>;
}

#[async_trait]
pub trait ComponentGuestFactory: Send + Sync {
    async fn instantiate(&self) -> Result<Arc<dyn ComponentGuest>, LixError>;
}

#[async_trait]
pub trait ComponentCompiler: Send + Sync {
    /// Validate and compile the component, including its imports and declared
    /// capabilities. Implementations must enforce memory limits during
    /// instantiation and reject any unsupported execution limit.

    async fn compile(
        &self,
        bytes: Vec<u8>,
        limits: WasmLimits,
        capabilities: PluginCapabilities,
    ) -> Result<Arc<dyn ComponentGuestFactory>, LixError>;
}

mod component_backend;
mod component_runtime;

/// Use an embedder's Component compiler with Lix's shared host and actor lifecycle.
pub fn runtime(compiler: Arc<dyn ComponentCompiler>) -> Arc<dyn super::WasmRuntime> {
    Arc::new(component_backend::JsRuntime(compiler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::runtime::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Probe {
        retained_host: Mutex<Option<Arc<dyn ComponentHost>>>,
        trap: bool,
        pending: bool,
        reject: bool,
    }
    #[async_trait]
    impl ComponentGuest for Probe {
        async fn invoke(
            &self,
            operation: &str,
            input: Value,
            host: Arc<dyn ComponentHost>,
            limits: WasmLimits,
        ) -> Result<(), LixError> {
            assert_eq!(operation, "parse");
            assert!(limits.timeout_ms.is_some());
            assert_eq!(input["input"]["creates"]["high"], u64::MAX.to_string());
            let file = input["input"]["file"].clone();
            assert_eq!(
                host.dispatch("snapshot.fileLen", json!({"resource":file}))?,
                json!({"ok":"3"})
            );
            assert_eq!(
                host.dispatch(
                    "snapshot.readFile",
                    json!({"resource":file,"offset":"0","length":3})
                )?,
                json!({"ok":[97,98,99]})
            );
            assert_eq!(
                host.dispatch(
                    "snapshot.readFile",
                    json!({"resource":file,"offset":"3","length":1})
                )?,
                json!({"error":{"tag":"invalid-range"}})
            );
            host.dispatch("snapshot.drop", json!({"resource":file}))?;
            assert!(
                host.dispatch("snapshot.fileLen", json!({"resource":file}))
                    .is_err()
            );
            *self.retained_host.lock().unwrap() = Some(host);
            if self.pending {
                std::future::pending::<()>().await;
            }
            if self.reject {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PLUGIN,
                    "invalid input",
                ));
            }
            if self.trap {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "execution deadline exceeded",
                ));
            }
            Ok(())
        }
    }
    struct Compiler(Arc<Probe>);
    #[async_trait]
    impl ComponentCompiler for Compiler {
        async fn compile(
            &self,
            _bytes: Vec<u8>,
            _limits: WasmLimits,
            _capabilities: PluginCapabilities,
        ) -> Result<Arc<dyn ComponentGuestFactory>, LixError> {
            Ok(Arc::new(Self(self.0.clone())))
        }
    }
    #[async_trait]
    impl ComponentGuestFactory for Compiler {
        async fn instantiate(&self) -> Result<Arc<dyn ComponentGuest>, LixError> {
            Ok(self.0.clone())
        }
    }
    struct File;
    impl WasmByteSource for File {
        fn len(&self) -> u64 {
            3
        }
        fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>, LixError> {
            Ok(b"abc"[offset as usize..offset as usize + length as usize].to_vec())
        }
    }
    fn input() -> WasmOpenFileInput {
        WasmOpenFileInput {
            descriptor: WasmFileDescriptor {
                file_id: "file".into(),
                path: Some("file.txt".into()),
                plugin: WasmPluginSelection {
                    plugin_key: "probe".into(),
                    generation: "1".into(),
                },
            },
            file: Arc::new(File),
            creates: WasmCreateContext {
                high: u64::MAX,
                low: 0,
            },
        }
    }
    #[tokio::test]
    async fn javascript_host_preserves_u64_resources_and_transition_lifecycle() {
        let guest = Arc::new(Probe::default());
        let runtime = runtime(Arc::new(Compiler(guest.clone())));
        let factory = runtime
            .compile_component(
                vec![],
                WasmLimits::default(),
                PluginCapabilities {
                    file_projection: true,
                    column_merger: false,
                },
            )
            .await
            .unwrap();
        let mut actor = factory.instantiate_actor().await.unwrap();
        let transition = actor
            .open_file(WasmTransitionLimits::default(), input())
            .await
            .unwrap();
        assert!(
            actor
                .next_change_page(transition.transition, transition.changes, 1024)
                .await
                .unwrap()
                .is_none()
        );
        actor
            .finish_transition(transition.transition)
            .await
            .unwrap();
        let checkpoint = actor
            .checkpoint_document(transition.document)
            .await
            .unwrap()
            .unwrap();
        let restored = actor.restore_document(&checkpoint).await.unwrap();
        actor.drop_document(restored).await.unwrap();
        let host = guest.retained_host.lock().unwrap().clone().unwrap();
        assert!(
            host.dispatch("snapshot.fileLen", json!({"resource":1}))
                .is_err()
        );
    }
    #[tokio::test]
    async fn javascript_guest_failure_retires_actor_and_revokes_callbacks() {
        let guest = Arc::new(Probe {
            trap: true,
            ..Probe::default()
        });
        let runtime = runtime(Arc::new(Compiler(guest.clone())));
        let factory = runtime
            .compile_component(
                vec![],
                WasmLimits::default(),
                PluginCapabilities {
                    file_projection: true,
                    column_merger: false,
                },
            )
            .await
            .unwrap();
        let mut actor = factory.instantiate_actor().await.unwrap();
        assert!(
            actor
                .open_file(WasmTransitionLimits::default(), input())
                .await
                .is_err()
        );
        assert!(actor.is_retired());
        let host = guest.retained_host.lock().unwrap().clone().unwrap();
        assert!(
            host.dispatch("transition.maxBatchBytes", json!({"resource":2}))
                .is_err()
        );
    }
    #[tokio::test]
    async fn cancelling_javascript_invocation_retires_actor_and_revokes_callbacks() {
        let guest = Arc::new(Probe {
            pending: true,
            ..Probe::default()
        });
        let runtime = runtime(Arc::new(Compiler(guest.clone())));
        let factory = runtime
            .compile_component(
                vec![],
                WasmLimits::default(),
                PluginCapabilities {
                    file_projection: true,
                    column_merger: false,
                },
            )
            .await
            .unwrap();
        let mut actor = factory.instantiate_actor().await.unwrap();
        let mut pending = Box::pin(actor.open_file(WasmTransitionLimits::default(), input()));
        assert!(
            std::future::Future::poll(
                pending.as_mut(),
                &mut std::task::Context::from_waker(std::task::Waker::noop())
            )
            .is_pending()
        );
        drop(pending);
        assert!(actor.is_retired());
        assert!(
            actor
                .open_file(WasmTransitionLimits::default(), input())
                .await
                .is_err()
        );
        let host = guest.retained_host.lock().unwrap().clone().unwrap();
        assert!(
            host.dispatch("transition.maxBatchBytes", json!({"resource":2}))
                .is_err()
        );
    }
    #[tokio::test]
    async fn guest_rejection_preserves_the_actor() {
        let guest = Arc::new(Probe {
            reject: true,
            ..Probe::default()
        });
        let runtime = runtime(Arc::new(Compiler(guest)));
        let factory = runtime
            .compile_component(
                vec![],
                WasmLimits::default(),
                PluginCapabilities {
                    file_projection: true,
                    column_merger: false,
                },
            )
            .await
            .unwrap();
        let mut actor = factory.instantiate_actor().await.unwrap();
        for _ in 0..2 {
            let error = actor
                .open_file(WasmTransitionLimits::default(), input())
                .await
                .unwrap_err();
            assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
            assert!(!actor.is_retired());
        }
    }
}
