import type { Generated, Insertable, Selectable, Updateable } from "kysely";
import type { SqliteWasmDatabase } from "sqlite-wasm-kysely";
export declare function applyKeyValueDatabaseSchema(sqlite: SqliteWasmDatabase): SqliteWasmDatabase;
export type KeyValue = Selectable<KeyValueTable>;
export type NewKeyValue = Insertable<KeyValueTable>;
export type KeyValueUpdate = Updateable<KeyValueTable>;
export type KeyValueTable = {
    /**
     * The key of the key-value pair.
     *
     * Lix prefixes its keys with "lix-" to avoid conflicts with user-defined keys.
     *
     * @example
     *   "namespace-cool-key"
     */
    key: KeyValueKeys;
    /**
     * The value of the key-value pair.
     *
     * Must be a string. A JSON is a string too ;)
     *
     * @example
     *   "some value"
     *   "{ "foo": "bar" }"
     *
     */
    value: string;
    /**
     * If `true`, the key-value pair is not tracked with own change control.
     *
     * Carefull (!) when querying the database. The return value will be `0` or `1`.
     * SQLite does not have a boolean select type https://www.sqlite.org/datatype3.html.
     *
     * @default false
     */
    skip_change_control: Generated<boolean>;
};
type PredefinedKeys = "lix_id" | "lix_server_url" | "lix_sync";
type KeyType = string & {};
type KeyValueKeys = PredefinedKeys | KeyType;
export {};
//# sourceMappingURL=database-schema.d.ts.map