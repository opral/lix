import { createInMemoryDatabase, importDatabase } from "sqlite-wasm-kysely";
import { openLix } from "./open-lix.js";
/**
 * Opens a lix in memory.
 *
 */
export async function openLixInMemory(args) {
    const database = await createInMemoryDatabase({
        readOnly: false,
    });
    if (args.blob) {
        importDatabase({
            db: database,
            content: new Uint8Array(await args.blob.arrayBuffer()),
        });
    }
    return openLix({ ...args, database });
}
//# sourceMappingURL=open-lix-in-memory.js.map