import type { LixPlugin } from "../plugin/lix-plugin.js";
import { type SqliteWasmDatabase } from "sqlite-wasm-kysely";
import { type Kysely } from "kysely";
import type { LixDatabaseSchema } from "../database/schema.js";
import type { NewKeyValue } from "../key-value/database-schema.js";
import type { Account } from "../account/database-schema.js";
export type Lix = {
    /**
     * The raw SQLite instance.
     *
     * Required for advanced use cases that can't be
     * expressed with the db API.
     *
     * Use with caution, automatic transformation of
     * results like parsing json (similar to the db API)
     * is not guaranteed.
     */
    sqlite: SqliteWasmDatabase;
    db: Kysely<LixDatabaseSchema>;
    plugin: {
        getAll: () => Promise<LixPlugin[]>;
    };
};
/**
 * Common setup between different lix environments.
 */
export declare function openLix(args: {
    /**
     * The account that is opening this lix.
     *
     * Lix will automatically set the active account to the provided account.
     *
     * @example
     *   const account = localStorage.getItem("account")
     *   const lix = await openLix({ account })
     */
    account?: Account;
    database: SqliteWasmDatabase;
    /**
     * Usecase are lix apps that define their own file format,
     * like inlang (unlike a markdown, csv, or json plugin).
     *
     * (+) avoids separating app code from plugin code and
     *     resulting bundling logic.
     *
     * (-) such a file format must always be opened with the
     *     file format sdk. the file is not portable
     *
     * @example
     *   const lix = await openLixInMemory({ providePlugins: [myPlugin] })
     */
    providePlugins?: LixPlugin[];
    /**
     * Set the key values when opening the lix.
     *
     * @example
     *   const lix = await openLix({ keyValues: [{ key: "lix_sync", value: "false" }] })
     */
    keyValues?: NewKeyValue[];
}): Promise<Lix>;
/**
 * Get all used file extensions.
 */
export declare function usedFileExtensions(db: Kysely<LixDatabaseSchema>): Promise<any>;
//# sourceMappingURL=open-lix.d.ts.map