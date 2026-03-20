import type { Insertable, Selectable } from "kysely";
import type { SqliteWasmDatabase } from "sqlite-wasm-kysely";
export declare const tablesByDepencies: string[];
export declare const tableIdColumns: Record<string, Array<string>>;
export declare function applyMutationLogDatabaseSchema(sqlite: SqliteWasmDatabase): void;
export type MutationLog = Selectable<MutationLogTable>;
export type NewMutationLog = Insertable<MutationLogTable>;
export type MutationLogTable = {
    row_id: Record<string, string>;
    table_name: string;
    operation: "INSERT" | "UPDATE";
    session: string;
    session_time: number;
    wall_clock: number;
};
//# sourceMappingURL=database-schema.d.ts.map