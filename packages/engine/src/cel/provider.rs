/// Function source available to CEL expressions.
///
/// CEL is shared infrastructure for schema expressions. It should not depend
/// on engine1 or engine runtime traits directly; callers adapt their own
/// execution-scoped function provider to this small boundary.
pub(crate) trait CelFunctionProvider: Clone + Send + Sync + 'static {
    fn call_uuid_v7(&self) -> uuid::Uuid;
    fn call_timestamp(&self) -> String;
}
