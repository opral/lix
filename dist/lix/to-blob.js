import { contentFromDatabase } from "sqlite-wasm-kysely";
/**
 * Convert the lix to a blob.
 *
 * @example
 *   const blob = await toBlob({ lix })
 */
export async function toBlob(args) {
    return new Blob([contentFromDatabase(args.lix.sqlite)]);
}
//# sourceMappingURL=to-blob.js.map