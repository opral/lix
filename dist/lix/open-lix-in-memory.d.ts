import { openLix } from "./open-lix.js";
/**
 * Opens a lix in memory.
 *
 */
export declare function openLixInMemory(args: {
    /**
     * The lix file to open. If not provided, an empty (new) lix will be opened.
     */
    blob?: Blob;
} & Omit<Parameters<typeof openLix>[0], "database">): Promise<ReturnType<typeof openLix>>;
//# sourceMappingURL=open-lix-in-memory.d.ts.map