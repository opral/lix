# Error codes

Lix errors carry a machine-readable `code`, a human-readable `message`, and optional `hint` and `details`. Branch on `code`; messages are not stable identifiers. HTTP status and `details.retryable` describe the specific response, not every possible use of a code.

## Naming and compatibility

New error codes use `LIX_ERROR_<CONDITION>` or `LIX_ERROR_<SUBSYSTEM>_<CONDITION>`. Do not repeat `LIX` after `LIX_ERROR_`. Reuse an existing code for the same condition; for example, the reference server and engine both use `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT`.

Existing SDK codes such as `LIX_INVALID_PARAM`, `LIX_STORAGE_FENCED`, and `LIX_INTERNAL_ERROR` remain unchanged. Their shorter prefix is historical, not a separate severity or error class. Do not rename them independently of a consumer migration.

The reference server and LixRay's production canary use only the current names below. There are no legacy aliases or compatibility checks. Update consumers together with the server. Generic SDK error transport preserves the server's code unchanged.

| Previous code | Current code |
| --- | --- |
| `LIX_ERROR_LIX_TIMEOUT` | `LIX_ERROR_TIMEOUT` |
| `LIX_ERROR_LIX_CAPACITY` | `LIX_ERROR_CAPACITY` |
| `LIX_ERROR_LIX_MIGRATING` | `LIX_ERROR_MIGRATING` |
| `LIX_ERROR_LIX_MIGRATION_FAILED` | `LIX_ERROR_MIGRATION_FAILED` |
| `LIX_ERROR_LIX_RECOVERING` | `LIX_ERROR_RECOVERING` |
| `LIX_ERROR_LIX_SHUTTING_DOWN` | `LIX_ERROR_SHUTTING_DOWN` |
| `LIX_ERROR_LIX_CACHE_CLEANUP` | `LIX_ERROR_CACHE_CLEANUP` |
| `LIX_ERROR_STORAGE_FORMAT_UNSUPPORTED` | `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT` |

## Reference server responses

Defined in [routes.rs](../packages/server/src/routes.rs). These codes describe the host's authentication, repository lifecycle, and request envelope failures. Engine and protocol errors can also pass through the host.

| Code | HTTP status | Meaning and recovery |
| --- | --- | --- |
| `LIX_INVALID_ARGUMENT` | 400 | Invalid runtime argument or trusted-principal header. Correct the request. |
| `LIX_NOT_FOUND` | 404 | URL repository ID is not a canonical UUID. Correct the URL. |
| `LIX_ERROR_UNAUTHENTICATED` | 401 | Missing or invalid internal service authentication. Supply valid credentials. |
| `LIX_ERROR_CAPACITY` | 503 | Maximum open repositories reached. Retry after capacity becomes available. |
| `LIX_ERROR_MIGRATING` | 503 | Repository migration is underway. Retry after migration completes; details contain the source and target versions. |
| `LIX_ERROR_MIGRATION_FAILED` | 500 | Repository migration or upgrade failed. Requires operator recovery; not automatically retryable. |
| `LIX_ERROR_RECOVERING` | 503 | Repository runtime is recovering. Retry after the advertised delay. |
| `LIX_ERROR_SHUTTING_DOWN` | 503 | Server is shutting down. Retry on a healthy server. |
| `LIX_ERROR_CACHE_CLEANUP` | 503 | Cache cleanup failed. Requires operator repair despite the 503 status. |
| `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT` | 409 | Repository storage format is unsupported. Requires a compatible reader or supported upgrade/recovery path. |
| `LIX_INTERNAL_ERROR` | 500 | Unexpected runtime-open failure. Inspect server logs for the underlying cause. |
| `LIX_ERROR_TIMEOUT` | 504 | Request timed out with an unknown outcome. A mutation may still complete; do not automatically replay it. |

The four transient lifecycle responses (`CAPACITY`, `MIGRATING`, `RECOVERING`, `SHUTTING_DOWN`) include `Retry-After`. Do not infer retry safety from status 503 alone. Migration-in-progress and migration-failed are distinct conditions.

## Shared SDK categories

These are the named categories defined on [LixError](../packages/lix/src/common/error.rs). The code values below are the wire values, not Rust constant names. Additional operation-specific codes appear in the inventory below.

| Code | Meaning |
| --- | --- |
| `LIX_ERROR_UNKNOWN` | True fallback — use when no more specific category fits. Producing sites should prefer the categorized codes below whenever possible; the SDK contract is that `LIX_ERROR_UNKNOWN` is the *last* resort, never the default. |
| `LIX_PARSE_ERROR` | SQL text could not be parsed. |
| `LIX_UDF_NOT_FOUND` | A SQL function name could not be resolved. |
| `LIX_TYPE_MISMATCH` | A SQL expression or function argument had an incompatible type. |
| `LIX_INVALID_JSON_PATH` | A Lix JSON path argument used another dialect's path language instead of Lix's canonical variadic key/index segments. |
| `LIX_DIALECT_UNSUPPORTED` | SQL syntax belongs to another dialect and is outside the Lix SQL surface. |
| `LIX_BINDING_ERROR` | SQL parameters could not be bound to placeholders. |
| `LIX_INVALID_PARAM` | A caller supplied an invalid SQL parameter value or parameter list. |
| `LIX_TABLE_NOT_FOUND` | A SQL table or view name could not be resolved. |
| `LIX_COLUMN_NOT_FOUND` | A SQL column name could not be resolved in the available projection. |
| `LIX_CONSTRAINT_VIOLATION` | A SQL write violated a primary-key, unique, NOT NULL, or other relational constraint. |
| `LIX_ERROR_READ_ONLY` | A SQL write targeted a read-only internal/component surface. |
| `LIX_UNSUPPORTED_SQL` | SQL syntax is valid, but the feature is intentionally outside the Lix SQL surface. |
| `LIX_UNSUPPORTED_SQL_RUNTIME_PLAN` | SQL planning succeeded far enough to produce a physical runtime shape that the current engine target cannot execute safely. |
| `LIX_STORAGE_ERROR` | Storage I/O failed. |
| `LIX_STORAGE_READ_EXPIRED` | A coherent storage read was invalidated by a concurrent commit. Auto-commit read surfaces consume this internally by reopening the complete read/query against a fresh snapshot. |
| `LIX_STORAGE_DURABILITY_UNAVAILABLE` | The selected storage cannot prove the requested persistence boundary. |
| `LIX_INVALID_SNAPSHOT` | A snapshot is malformed, unsupported, truncated, or fails integrity verification. |
| `LIX_SNAPSHOT_IO` | Reading or writing a snapshot stream failed. |
| `LIX_STORAGE_FENCED` | A newer storage client fenced this writer, so this Lix instance can no longer serve requests. |
| `LIX_STORAGE_CLOSED` | The backing storage instance stopped and this Lix instance can no longer serve requests. |
| `LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN` | A storage commit may have been applied, but its caller did not receive a definitive result. |
| `LIX_IDEMPOTENCY_KEY_REQUIRED` | A server SQL mutation did not provide the required replay identity. |
| `LIX_IDEMPOTENCY_KEY_REUSED` | A replay identity was reused for a different logical mutation. |
| `LIX_IDEMPOTENCY_RESPONSE_TOO_LARGE` | A mutation response cannot be retained safely for idempotent replay. |
| `LIX_TRANSACTION_CONFLICT` | Optimistic transaction publication lost a race with a newer commit. |
| `LIX_INTERNAL_ERROR` | An internal engine invariant failed. |
| `LIX_ERROR_INVALID_PLUGIN` | A plugin ZIP package or manifest is malformed, unsafe, or exceeds the static resource bounds accepted by the engine. Invalid embedded Lix schema definitions retain `CODE_SCHEMA_DEFINITION`. |
| `LIX_ERROR_PLUGIN_UNAVAILABLE` | A file is materialized as durable plugin state, but the plugin needed to render that state is not installed on the file's branch. |
| `LIX_ERROR_PLUGIN_OBSERVATION_STALE` | An incremental plugin write did not carry an exact, still-current private document observation. The client must re-read the file; the engine never guesses identity authority from equal byte hashes. |
| `LIX_ERROR_PLUGIN_RESOURCE_LIMIT` | Creating another live plugin Store would exceed the repository-wide runtime admission limit configured for this Engine. |
| `LIX_ERROR_SCHEMA_VALIDATION` | Write-time failure where user data did not conform to a registered schema (type mismatch, missing required field, pattern violation, additionalProperties, etc.). Raised from the JSON-Schema validator run over a candidate row's snapshot. |
| `LIX_ERROR_FOREIGN_KEY` | A foreign-key constraint could not be satisfied. Covers both the insert-side "no matching target row" failure and the delete-side "still referenced" (restrict) failure. |
| `LIX_ERROR_FILE_NOT_FOUND` | A row references a non-null `file_id` that has no matching `lix_file` descriptor in the same effective branch scope. |
| `LIX_ERROR_UNIQUE` | A primary-key or `x-lix-unique` constraint was violated — another row already owns the value(s) for the declared pointer group. |
| `LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION` | An `INSERT ... VALUES (...)` expression is not supported by the public write surface (e.g. `json(...)`, subqueries, arbitrary SQL expressions). Users should cast inline JSON with `::jsonb`. |
| `LIX_ERROR_SCHEMA_DEFINITION` | The schema JSON itself (the *definition*, not a row against it) is malformed — a missing `key`, an invalid primary-key column, or another leading slash, a reserved-namespace collision, or any other meta-schema validation failure. |
| `LIX_RESERVED_SCHEMA_NAMESPACE` | A public runtime schema registration attempted to use the `lix_*` namespace reserved for schemas owned and bootstrapped by Lix. |
| `LIX_ERROR_CLOSED` | The logical Lix handle/session has been closed and cannot run further operations. Close is a resource-release lifecycle boundary, not a durability boundary. |
| `LIX_INVALID_SESSION_STATE` | An operation is incompatible with the current session mode or state. |
| `LIX_MERGE_CONFLICT` | A merge found incompatible changes to the same tracked-state identity. |
| `LIX_BRANCH_NOT_FOUND` | A caller referenced a branch id that has no matching branch ref. |
| `LIX_COMMIT_NOT_FOUND` | A caller referenced a commit id that has no matching commit record. |
| `LIX_ERROR_INVALID_STORAGE_SCOPE` | A staged row's storage scope flags disagree, such as a global row not using the reserved global branch id. |
| `LIX_AMBIGUOUS_MERGE_BASE` | Merge graph analysis found multiple equally valid merge bases. |
| `LIX_INVALID_MERGE` | A merge request is well-formed but nonsensical for the commit graph, such as merging a branch into itself. |
| `LIX_NOTHING_TO_UNDO` | The selected branch has no ordinary commit above its undo boundary. |
| `LIX_NOTHING_TO_REDO` | The selected branch has no abandoned action available to replay. |

## Literal code inventory

Inventory of literal codes in the engine, reference server, and JavaScript SDK source at this revision. This includes operation-specific codes in addition to the shared categories above. The source link identifies a definition or use and is the authority for its detailed semantics. Environment variables, schema identifiers, test-only fixture codes, and dynamic/custom extension codes are excluded. This is not a closed enum: SDKs must preserve unknown codes.

| Code | Source |
| --- | --- |
| `LIX_ACCESS_DENIED` | [packages/lix/src/authority_client/tests.rs](../packages/lix/src/authority_client/tests.rs) |
| `LIX_ACCOUNT_DISABLED` | [packages/lix/src/account.rs](../packages/lix/src/account.rs) |
| `LIX_ACCOUNT_INSERTION_SCOPE` | [packages/lix/src/account.rs](../packages/lix/src/account.rs) |
| `LIX_ACCOUNT_NOT_FOUND` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_AMBIGUOUS_MERGE_BASE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_AUTHORITY_UPGRADE_REQUIRED` | [packages/lix/src/migration/authority_baseline_fence.rs](../packages/lix/src/migration/authority_baseline_fence.rs) |
| `LIX_BINDING_ERROR` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_BRANCH_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_COLUMN_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_COMMIT_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_CONSTRAINT_VIOLATION` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_CREATE_IN_PROGRESS` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_CREATE_RECOVERY_LIMIT` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_CREATE_UNAVAILABLE` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_DIALECT_UNSUPPORTED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_DIFF_HOT_UNAVAILABLE` | [packages/lix/src/sql2/providers/diff.rs](../packages/lix/src/sql2/providers/diff.rs) |
| `LIX_ERROR_ALREADY_INITIALIZED` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_ERROR_CACHE_CLEANUP` | [packages/server/src/routes.rs](../packages/server/src/routes.rs) |
| `LIX_ERROR_CAPACITY` | [packages/server/src/routes.rs](../packages/server/src/routes.rs) |
| `LIX_ERROR_CLOSED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_FILE_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_FOREIGN_KEY` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_INVALID_JSON` | [packages/lix/src/common/metadata.rs](../packages/lix/src/common/metadata.rs) |
| `LIX_ERROR_INVALID_PLUGIN` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_INVALID_STORAGE_SCOPE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_METHOD_NOT_ALLOWED` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_MIGRATING` | [packages/lix/src/sync/http.rs](../packages/lix/src/sync/http.rs) |
| `LIX_ERROR_MIGRATION_COMMIT_OUTCOME_UNKNOWN` | [packages/lix/src/migration/publish.rs](../packages/lix/src/migration/publish.rs) |
| `LIX_ERROR_MIGRATION_CONCURRENT_MUTATION` | [packages/lix/src/migration/publish.rs](../packages/lix/src/migration/publish.rs) |
| `LIX_ERROR_MIGRATION_FAILED` | [packages/lix/src/migration/api.rs](../packages/lix/src/migration/api.rs) |
| `LIX_ERROR_MIGRATION_LIMIT_EXCEEDED` | [packages/lix/src/hot_state/tracked_head/hot.rs](../packages/lix/src/hot_state/tracked_head/hot.rs) |
| `LIX_ERROR_MIGRATION_SCHEMA_INCOMPATIBLE` | [packages/lix/src/migration/api.rs](../packages/lix/src/migration/api.rs) |
| `LIX_ERROR_NOT_INITIALIZED` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_ERROR_PATH_DOT_SEGMENT` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_EMPTY_SEGMENT` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_INVALID_DIRECTORY_PARENT` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_INVALID_ROOT_USAGE` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_MISSING_LEADING_SLASH` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_NUL` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_SLASH_IN_SEGMENT` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PATH_UNEXPECTED_TRAILING_SLASH` | [packages/lix/src/common/lix_path.rs](../packages/lix/src/common/lix_path.rs) |
| `LIX_ERROR_PLUGIN_OBSERVATION_STALE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_PLUGIN_RESOURCE_LIMIT` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_PLUGIN_UNAVAILABLE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_PROTOCOL_ACCOUNT_MISMATCH` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_PROTOCOL_PATH_NOT_FOUND` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_PROTOCOL_SERVER_CLOSED` | [packages/lix/src/authority_client/wire.rs](../packages/lix/src/authority_client/wire.rs) |
| `LIX_ERROR_PROTOCOL_SESSION_CAPACITY` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_PROTOCOL_SESSION_GONE` | [packages/lix/src/authority_client/wire.rs](../packages/lix/src/authority_client/wire.rs) |
| `LIX_ERROR_PROTOCOL_SESSION_INVALID` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_PROTOCOL_SESSION_REQUIRED` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_PROTOCOL_SNAPSHOT_CAPACITY` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_READ_ONLY` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_RECOVERING` | [packages/server/src/routes.rs](../packages/server/src/routes.rs) |
| `LIX_ERROR_REPLICA_REPLACEMENT_UNAVAILABLE` | [packages/lix/src/sync/repository.rs](../packages/lix/src/sync/repository.rs) |
| `LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED` | [packages/lix/src/init.rs](../packages/lix/src/init.rs) |
| `LIX_ERROR_REQUEST_BODY_TOO_LARGE` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_SCHEMA_DEFINITION` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_SCHEMA_VALIDATION` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_SHUTTING_DOWN` | [packages/server/src/routes.rs](../packages/server/src/routes.rs) |
| `LIX_ERROR_SYNC_CAS_NOT_FOUND` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_SYNC_DEMAND_STALLED` | [packages/lix/src/sync/runtime.rs](../packages/lix/src/sync/runtime.rs) |
| `LIX_ERROR_SYNC_ITEM_TOO_LARGE` | [packages/lix/src/observe_invalidation.rs](../packages/lix/src/observe_invalidation.rs) |
| `LIX_ERROR_SYNC_REPLICA_STATE_AMBIGUOUS` | [packages/lix/src/sync/bootstrap.rs](../packages/lix/src/sync/bootstrap.rs) |
| `LIX_ERROR_SYNC_RESPONSE_TOO_LARGE` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_SYNC_SNAPSHOT_TOO_LARGE` | [packages/lix/src/sync/runtime.rs](../packages/lix/src/sync/runtime.rs) |
| `LIX_ERROR_SYNC_TRANSPORT` | [packages/lix/src/sync/http.rs](../packages/lix/src/sync/http.rs) |
| `LIX_ERROR_SYNC_WRITE_REJECTED` | [packages/lix/src/sync/runtime.rs](../packages/lix/src/sync/runtime.rs) |
| `LIX_ERROR_TIMEOUT` | [packages/server/src/routes.rs](../packages/server/src/routes.rs) |
| `LIX_ERROR_UNAUTHENTICATED` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_UNIQUE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_UNKNOWN` | [packages/lix/src/binary_cas/codec.rs](../packages/lix/src/binary_cas/codec.rs) |
| `LIX_ERROR_UNSUPPORTED_CONTENT_ENCODING` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_UNSUPPORTED_CONTENT_TYPE` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_ERROR_VALUE_TYPE` | [packages/lix/src/session/execute.rs](../packages/lix/src/session/execute.rs) |
| `LIX_EXISTING_STORAGE` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_FOREIGN_KEY_VIOLATION` | [packages/lix/src/handle.rs](../packages/lix/src/handle.rs) |
| `LIX_IDEMPOTENCY_CONFLICT` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_IDEMPOTENCY_KEY_REQUIRED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_IDEMPOTENCY_KEY_REUSED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_IDEMPOTENCY_RESPONSE_TOO_LARGE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INTERNAL_ERROR` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_ACCOUNT_ID` | [packages/lix/src/account.rs](../packages/lix/src/account.rs) |
| `LIX_INVALID_ARGUMENT` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_INVALID_JSON_PATH` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_MERGE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_PARAM` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_REPOSITORY` | [packages/lix/src/migration/epoch.rs](../packages/lix/src/migration/epoch.rs) |
| `LIX_INVALID_SESSION_STATE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_SNAPSHOT` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_INVALID_TRANSACTION_STATE` | [packages/lix/src/authority_client/mod.rs](../packages/lix/src/authority_client/mod.rs) |
| `LIX_MERGE_CONFLICT` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_MIGRATION_CLEANUP_UNRESOLVED` | [packages/lix/src/sync/native_migration_cleanup.rs](../packages/lix/src/sync/native_migration_cleanup.rs) |
| `LIX_MIGRATION_GLOBAL_ALREADY_COMMITTED` | [packages/lix/src/sync/native_global_migration_receipt.rs](../packages/lix/src/sync/native_global_migration_receipt.rs) |
| `LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED` | [packages/lix/src/handle/partial_merge/native_global_migration_tests.rs](../packages/lix/src/handle/partial_merge/native_global_migration_tests.rs) |
| `LIX_MIGRATION_GLOBAL_BODY_INVALID` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_MIGRATION_GLOBAL_CLEANUP_UNRESOLVED` | [packages/lix/src/handle/partial_merge.rs](../packages/lix/src/handle/partial_merge.rs) |
| `LIX_MIGRATION_GLOBAL_JOURNAL_INVALID` | [packages/lix/src/migration/epoch/native_global_conversion_journal.rs](../packages/lix/src/migration/epoch/native_global_conversion_journal.rs) |
| `LIX_MIGRATION_GLOBAL_RESTART_INVALID` | [packages/lix/src/handle/partial_merge.rs](../packages/lix/src/handle/partial_merge.rs) |
| `LIX_MIGRATION_GLOBAL_RETENTION_INVALID` | [packages/lix/src/gc/native_global_retention.rs](../packages/lix/src/gc/native_global_retention.rs) |
| `LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED` | [packages/lix/src/handle/partial_merge/native_global_migration_tests.rs](../packages/lix/src/handle/partial_merge/native_global_migration_tests.rs) |
| `LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED` | [packages/lix/src/session/merge/branch/native_migration.rs](../packages/lix/src/session/merge/branch/native_migration.rs) |
| `LIX_NATIVE_BASELINE_LEASE_INVALID` | [packages/lix/src/gc/native_baseline_lease.rs](../packages/lix/src/gc/native_baseline_lease.rs) |
| `LIX_NATIVE_METADATA_UNAVAILABLE` | [packages/lix/src/sync/native_metadata.rs](../packages/lix/src/sync/native_metadata.rs) |
| `LIX_NATIVE_OBJECT_BATCH_TOO_LARGE` | [packages/lix/src/sync/native_object.rs](../packages/lix/src/sync/native_object.rs) |
| `LIX_NATIVE_OBJECT_UNAVAILABLE` | [packages/lix/src/sync/native_object.rs](../packages/lix/src/sync/native_object.rs) |
| `LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED` | [packages/lix/src/gc/native_upload_attempt.rs](../packages/lix/src/gc/native_upload_attempt.rs) |
| `LIX_NATIVE_UPLOAD_ATTEMPT_INVALID` | [packages/lix/src/gc/native_upload_attempt.rs](../packages/lix/src/gc/native_upload_attempt.rs) |
| `LIX_NETWORK_ERROR` | [packages/js-sdk/src/worker/shared-engine.ts](../packages/js-sdk/src/worker/shared-engine.ts) |
| `LIX_NOTHING_TO_REDO` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_NOTHING_TO_UNDO` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_NOT_FOUND` | [packages/lix/src/migration/epoch/partial_conversion.rs](../packages/lix/src/migration/epoch/partial_conversion.rs) |
| `LIX_PARSE_ERROR` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_PARTIAL_ATTEMPT_RESTARTED` | [packages/lix/src/gc/native_upload_attempt.rs](../packages/lix/src/gc/native_upload_attempt.rs) |
| `LIX_PARTIAL_ATTEMPT_RESTART_INVALID` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_BASELINE_EXPIRED` | [packages/lix/src/gc/native_baseline_lease.rs](../packages/lix/src/gc/native_baseline_lease.rs) |
| `LIX_PARTIAL_BLOB_MANIFEST_REQUIRED` | [packages/lix/src/binary_cas/demand.rs](../packages/lix/src/binary_cas/demand.rs) |
| `LIX_PARTIAL_BRANCH_SWITCH_PENDING` | [packages/lix/src/sync/partial_merge_state.rs](../packages/lix/src/sync/partial_merge_state.rs) |
| `LIX_PARTIAL_CANDIDATE_EXPIRED` | [packages/lix/src/migration/epoch/partial_conversion.rs](../packages/lix/src/migration/epoch/partial_conversion.rs) |
| `LIX_PARTIAL_CONVERSION_EXPIRED_AFTER_PUBLICATION` | [packages/lix/src/migration/epoch/partial_conversion.rs](../packages/lix/src/migration/epoch/partial_conversion.rs) |
| `LIX_PARTIAL_CONVERSION_INVALID_BRANCH` | [packages/lix/src/sync/partial_open.rs](../packages/lix/src/sync/partial_open.rs) |
| `LIX_PARTIAL_CONVERSION_UNRESOLVED` | [packages/lix/src/migration/epoch/pending_conversion.rs](../packages/lix/src/migration/epoch/pending_conversion.rs) |
| `LIX_PARTIAL_CREATED_REF_SOURCE_PENDING` | [packages/lix/src/sync/partial_created_refs.rs](../packages/lix/src/sync/partial_created_refs.rs) |
| `LIX_PARTIAL_GLOBAL_MERGE_PENDING` | [packages/lix/src/sync/partial_global_merge_runtime.rs](../packages/lix/src/sync/partial_global_merge_runtime.rs) |
| `LIX_PARTIAL_GLOBAL_MERGE_STATE_INVALID` | [packages/lix/src/sync/partial_global_merge_runtime.rs](../packages/lix/src/sync/partial_global_merge_runtime.rs) |
| `LIX_PARTIAL_GLOBAL_NEWER_LOCAL_RECONCILIATION_REQUIRED` | [packages/lix/src/sync/partial_global_merge_runtime.rs](../packages/lix/src/sync/partial_global_merge_runtime.rs) |
| `LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED` | [packages/lix/src/sync/partial_global_merge_runtime.rs](../packages/lix/src/sync/partial_global_merge_runtime.rs) |
| `LIX_PARTIAL_GLOBAL_SELECTED_RECONCILIATION_REQUIRED` | [packages/lix/src/sync/partial_global_merge_runtime.rs](../packages/lix/src/sync/partial_global_merge_runtime.rs) |
| `LIX_PARTIAL_INTERESTS_NOT_DURABLE` | [packages/lix/src/hot_state/read_interests.rs](../packages/lix/src/hot_state/read_interests.rs) |
| `LIX_PARTIAL_INTEREST_JOURNAL_INVALID` | [packages/lix/src/sync/partial_interest_journal.rs](../packages/lix/src/sync/partial_interest_journal.rs) |
| `LIX_PARTIAL_MERGE_ATTEMPT_REUSED` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_AUTHORITY_CHANGED` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_BUDGET_EXCEEDED` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_CONFLICT` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_PREPARATION_LIMIT` | [packages/lix/src/transaction/context/native_application.rs](../packages/lix/src/transaction/context/native_application.rs) |
| `LIX_PARTIAL_MERGE_PROTOCOL_INVALID` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_RECEIPT_INVALID` | [packages/lix/src/sync/native_global_migration_receipt.rs](../packages/lix/src/sync/native_global_migration_receipt.rs) |
| `LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED` | [packages/lix/src/handle/partial_merge.rs](../packages/lix/src/handle/partial_merge.rs) |
| `LIX_PARTIAL_MERGE_STATE_INVALID` | [packages/lix/src/sync/partial_merge_runtime.rs](../packages/lix/src/sync/partial_merge_runtime.rs) |
| `LIX_PARTIAL_PUSH_STATE_INVALID` | [packages/lix/src/sync/partial_push_state.rs](../packages/lix/src/sync/partial_push_state.rs) |
| `LIX_PARTIAL_READ_INTEREST_CHANGED` | [packages/lix/src/hot_state/read_interests.rs](../packages/lix/src/hot_state/read_interests.rs) |
| `LIX_PARTIAL_READ_INTEREST_LIMIT` | [packages/lix/src/hot_state/read_interests.rs](../packages/lix/src/hot_state/read_interests.rs) |
| `LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_PARTIAL_REPLICA_BASELINE_RECOVERY_PENDING` | [packages/lix/src/sync/partial_publication.rs](../packages/lix/src/sync/partial_publication.rs) |
| `LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED` | [packages/lix/src/migration/epoch/partial_conversion.rs](../packages/lix/src/migration/epoch/partial_conversion.rs) |
| `LIX_PARTIAL_REPLICA_DEMAND_UNSUPPORTED` | [packages/lix/src/sync/partial_runtime.rs](../packages/lix/src/sync/partial_runtime.rs) |
| `LIX_PARTIAL_REPLICA_GC_UNAVAILABLE` | [packages/lix/src/gc.rs](../packages/lix/src/gc.rs) |
| `LIX_PARTIAL_REPLICA_MERGE_PENDING` | [packages/lix/src/sync/partial_merge_settlement.rs](../packages/lix/src/sync/partial_merge_settlement.rs) |
| `LIX_PARTIAL_REPLICA_MERGE_RECOVERY_PENDING` | [packages/lix/src/sync/partial_merge_runtime.rs](../packages/lix/src/sync/partial_merge_runtime.rs) |
| `LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_PARTIAL_REPLICA_OFFLINE` | [packages/lix/src/handle.rs](../packages/lix/src/handle.rs) |
| `LIX_PARTIAL_REPLICA_REBASE_REQUIRED` | [packages/lix/src/sync/partial_publication.rs](../packages/lix/src/sync/partial_publication.rs) |
| `LIX_PARTIAL_REPLICA_REQUIRES_ON_DEMAND_SYNC` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_PARTIAL_REPLICA_RESTORE_UNAVAILABLE` | [packages/lix/src/transaction/context.rs](../packages/lix/src/transaction/context.rs) |
| `LIX_PARTIAL_REPLICA_SCOPE_NOT_PREPARED` | [packages/lix/src/engine.rs](../packages/lix/src/engine.rs) |
| `LIX_PARTIAL_REPLICA_SCOPE_UNSUPPORTED` | [packages/lix/src/sql2/providers/change.rs](../packages/lix/src/sql2/providers/change.rs) |
| `LIX_PARTIAL_REPLICA_STATE_INVALID` | [packages/lix/src/hot_state/partial_scope_policy.rs](../packages/lix/src/hot_state/partial_scope_policy.rs) |
| `LIX_PARTIAL_SCOPE_PREPARATION_LIMIT` | [packages/lix/src/sync/partial_branch_switch.rs](../packages/lix/src/sync/partial_branch_switch.rs) |
| `LIX_PARTIAL_SCOPE_PREPARATION_REQUIRED` | [packages/lix/src/hot_state/partial_scope_policy.rs](../packages/lix/src/hot_state/partial_scope_policy.rs) |
| `LIX_PARTIAL_SCOPE_PREPARATION_STALLED` | [packages/lix/src/sync/partial_branch_switch.rs](../packages/lix/src/sync/partial_branch_switch.rs) |
| `LIX_PARTIAL_SQL_DEMAND_LIMIT` | [packages/lix/src/sync/partial_sql_tests.rs](../packages/lix/src/sync/partial_sql_tests.rs) |
| `LIX_PARTIAL_SQL_NO_PROGRESS` | [packages/lix/src/sync/partial_sql_tests.rs](../packages/lix/src/sync/partial_sql_tests.rs) |
| `LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_PARTIAL_UPLOAD_PAGE_REQUIRED` | [packages/lix/src/sync/partial_checkpoint_upload.rs](../packages/lix/src/sync/partial_checkpoint_upload.rs) |
| `LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED` | [packages/lix/src/sync/native_migration_pin_upload.rs](../packages/lix/src/sync/native_migration_pin_upload.rs) |
| `LIX_PARTIAL_WRITE_FRONTIER_INVALID` | [packages/lix/src/sync/partial_write_frontier.rs](../packages/lix/src/sync/partial_write_frontier.rs) |
| `LIX_PARTIAL_WRITE_FRONTIER_REQUIRED` | [packages/lix/src/sync/partial_candidate_prepare.rs](../packages/lix/src/sync/partial_candidate_prepare.rs) |
| `LIX_PROTOCOL_VERSION_MISMATCH` | [packages/lix/src/server_protocol/handler.rs](../packages/lix/src/server_protocol/handler.rs) |
| `LIX_PROVISION_CONFLICT` | [packages/server/src/store.rs](../packages/server/src/store.rs) |
| `LIX_RECOVERY_EPOCH_CHANGED` | [packages/lix/src/sync/recovery.rs](../packages/lix/src/sync/recovery.rs) |
| `LIX_RECOVERY_EXPORT_TOO_LARGE` | [packages/lix/src/session/media_upload.rs](../packages/lix/src/session/media_upload.rs) |
| `LIX_REMOTE_BLOB_BASE_MISSING` | [packages/lix/src/authority_client/wire.rs](../packages/lix/src/authority_client/wire.rs) |
| `LIX_REMOTE_CONFIGURATION_ERROR` | [packages/lix/src/authority_client/mod.rs](../packages/lix/src/authority_client/mod.rs) |
| `LIX_REMOTE_REQUEST_FAILED` | [packages/lix/src/authority_client/mod.rs](../packages/lix/src/authority_client/mod.rs) |
| `LIX_REMOTE_UNAVAILABLE` | [packages/lix/src/authority_client/observe.rs](../packages/lix/src/authority_client/observe.rs) |
| `LIX_REPLICA_CACHE_READ_ONLY` | [packages/lix/src/sync/repository.rs](../packages/lix/src/sync/repository.rs) |
| `LIX_RESERVED_SCHEMA_NAMESPACE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_SERVER_ERROR` | [packages/lix/src/lifecycle.rs](../packages/lix/src/lifecycle.rs) |
| `LIX_SERVER_PROTOCOL_ERROR` | [packages/lix/src/authority_client/observe.rs](../packages/lix/src/authority_client/observe.rs) |
| `LIX_SHARED_ENGINE_IDENTITY_MISMATCH` | [packages/js-sdk/src/worker/shared-admission.ts](../packages/js-sdk/src/worker/shared-admission.ts) |
| `LIX_SNAPSHOT_IO` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STALE_SELECTED_CHANGE_LOCATOR` | [packages/lix/src/tracked_state/storage.rs](../packages/lix/src/tracked_state/storage.rs) |
| `LIX_STORAGE_CLOSED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_CORRUPTION` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_DURABILITY` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_DURABILITY_UNAVAILABLE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_ERROR` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_FENCED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_INVALID_CURSOR` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_INVALID_KEY` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_IN_USE` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_IO` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_PRECONDITION_FAILED` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_READ_EXPIRED` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_STORAGE_UNSUPPORTED` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_STORAGE_WRITE_CONFLICT` | [packages/js-sdk/src/storage-adapter.ts](../packages/js-sdk/src/storage-adapter.ts) |
| `LIX_SYNC_ACCOUNT_MISMATCH` | [packages/lix/src/server_protocol/handler/partial_merge.rs](../packages/lix/src/server_protocol/handler/partial_merge.rs) |
| `LIX_SYNC_BRANCH_CONTROLS_REQUIRED` | [packages/lix/src/branch/control.rs](../packages/lix/src/branch/control.rs) |
| `LIX_SYNC_BRANCH_INVENTORY_REQUIRED` | [packages/lix/src/branch/control.rs](../packages/lix/src/branch/control.rs) |
| `LIX_SYNC_CHUNKS_REQUIRED` | [packages/lix/src/binary_cas/kv.rs](../packages/lix/src/binary_cas/kv.rs) |
| `LIX_SYNC_HISTORY_REQUIRED` | [packages/lix/src/handle.rs](../packages/lix/src/handle.rs) |
| `LIX_SYNC_IMMUTABLE_OBJECT_MISMATCH` | [packages/lix/src/sync/mod.rs](../packages/lix/src/sync/mod.rs) |
| `LIX_SYNC_PREPARED_REF_LIMIT` | [packages/lix/src/sync/upload_proof.rs](../packages/lix/src/sync/upload_proof.rs) |
| `LIX_SYNC_PROTOCOL_MISMATCH` | [packages/lix/src/sync/mod.rs](../packages/lix/src/sync/mod.rs) |
| `LIX_SYNC_REPOSITORY_ID_MISMATCH` | [packages/lix/src/sync/mod.rs](../packages/lix/src/sync/mod.rs) |
| `LIX_TABLE_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_TRANSACTION_CONFLICT` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_TYPE_MISMATCH` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_UDF_NOT_FOUND` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_UNSUPPORTED_OPERATION` | [packages/lix/src/sync/platform/wasm_http.rs](../packages/lix/src/sync/platform/wasm_http.rs) |
| `LIX_UNSUPPORTED_REMOTE_OPERATION` | [packages/lix/src/authority_client/wire.rs](../packages/lix/src/authority_client/wire.rs) |
| `LIX_UNSUPPORTED_SQL` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_UNSUPPORTED_SQL_RUNTIME_PLAN` | [packages/lix/src/common/error.rs](../packages/lix/src/common/error.rs) |
| `LIX_WORKER_TERMINATED` | [packages/js-sdk/src/worker/client.ts](../packages/js-sdk/src/worker/client.ts) |
