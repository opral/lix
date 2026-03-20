import type { Lix } from "../lix/open-lix.js";
/**
 * Waits until the file queue is settled.
 *
 * @example
 *   ```ts
 *   await fileQueueSettled({ lix });
 *   ```
 */
export declare function fileQueueSettled(args: {
    lix: Pick<Lix, "db">;
}): Promise<void>;
//# sourceMappingURL=file-queue-settled.d.ts.map