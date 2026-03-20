import type { Generated, Insertable, Selectable, Updateable } from "kysely";
import type { SqliteWasmDatabase } from "sqlite-wasm-kysely";
export declare function applyAccountDatabaseSchema(sqlite: SqliteWasmDatabase): SqliteWasmDatabase;
export type Account = Selectable<AccountTable>;
export type NewAccount = Insertable<AccountTable>;
export type AccountUpdate = Updateable<AccountTable>;
export type AccountTable = {
    id: Generated<string>;
    name: string;
};
export type ActiveAccount = Selectable<ActiveAccountTable>;
export type NewActiveAccount = Insertable<ActiveAccountTable>;
export type ActiveAccountUpdate = Updateable<ActiveAccountTable>;
export type ActiveAccountTable = {
    id: string;
    name: string;
};
//# sourceMappingURL=database-schema.d.ts.map