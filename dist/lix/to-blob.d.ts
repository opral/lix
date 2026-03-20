import type { Lix } from "./open-lix.js";
/**
 * Convert the lix to a blob.
 *
 * @example
 *   const blob = await toBlob({ lix })
 */
export declare function toBlob(args: {
    lix: Pick<Lix, "db" | "sqlite">;
}): Promise<Blob>;
//# sourceMappingURL=to-blob.d.ts.map