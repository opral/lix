import { Kysely } from "kysely";
import { type SqliteWasmDatabase } from "sqlite-wasm-kysely";
import type { LixDatabaseSchema } from "./schema.js";
export declare function initDb(args: {
    sqlite: SqliteWasmDatabase;
}): Kysely<LixDatabaseSchema>;
//# sourceMappingURL=init-db.d.ts.map