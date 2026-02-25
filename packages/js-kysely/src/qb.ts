import { createLixKysely } from "./create-lix-kysely.js";

type QbInput = Parameters<typeof createLixKysely>[0];

/**
 * Kysely entrypoint for Lix.
 *
 * Usage:
 * await qb(lix).selectFrom("file").selectAll().execute()
 */
export const qb = (lix: QbInput) => createLixKysely(lix);
