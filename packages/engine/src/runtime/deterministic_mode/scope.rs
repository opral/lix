use crate::schema::access::tracked_relation_name;
use crate::runtime::deterministic_mode::PersistedKeyValueStorageScope;
use crate::schema::builtin::storage::key_value_schema_key;

const GLOBAL_VERSION_ID: &str = "global";

pub(crate) fn global_deterministic_settings_storage_scope() -> PersistedKeyValueStorageScope {
    PersistedKeyValueStorageScope::new(
        tracked_relation_name(key_value_schema_key()),
        GLOBAL_VERSION_ID,
    )
}
