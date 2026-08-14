/// Function source available to CEL expressions.
///
/// CEL is shared infrastructure for schema expressions. It should not depend
/// on engine1 or engine runtime traits directly; callers adapt their own
/// execution-scoped function provider to this small boundary.
pub(crate) trait CelFunctionProvider: Clone + Send + Sync + 'static {
    fn call_uuid_v7(&self) -> uuid::Uuid;
    fn call_timestamp(&self) -> String;
}

impl CelFunctionProvider for crate::functions::FunctionProviderHandle {
    fn call_uuid_v7(&self) -> uuid::Uuid {
        self.call_uuid_v7()
    }

    fn call_timestamp(&self) -> String {
        self.call_timestamp().to_string()
    }
}
