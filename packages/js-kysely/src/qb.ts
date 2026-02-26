import { createLixKysely } from "./create-lix-kysely.js";
import type { CreateLixKyselyOptions } from "./create-lix-kysely.js";

type QbInput = Parameters<typeof createLixKysely>[0];
type QbOptions = CreateLixKyselyOptions;

/**
 * Kysely entrypoint for Lix.
 *
 * Usage:
 * await qb(lix).selectFrom("lix_file").selectAll().execute()
 */
export const qb = (lix: QbInput, options?: QbOptions) =>
	createLixKysely(lix, options);
