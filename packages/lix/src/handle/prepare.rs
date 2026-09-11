use super::*;
impl<S: Storage + Clone + Send + Sync + 'static> Lix<S> {
    /// Hydrates dependencies for supported deterministic SQL without publishing
    /// user writes. Subsequent execution can become cold if its inputs change.
    /// Currently accepts function-free SELECT and deterministic existing-target
    /// UPDATE of lix_key_value.value or ordinary lix_file.content.
    pub async fn prepare(&self, sql: &str, params: &[Value]) -> Result<(), LixError> {
        self.retry_sync_demands(|| self.session.prepare_sql_once(sql, params))
            .await
    }
}

#[cfg(all(test, feature = "server-protocol", not(target_family = "wasm")))]
mod tests;
