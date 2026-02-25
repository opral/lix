import {
	DummyDriver,
	Kysely,
	SqliteAdapter,
	SqliteIntrospector,
	SqliteQueryCompiler,
} from "kysely";
import type { LixDatabaseSchema } from "./schema.js";

const compileOnlySqliteDialect = {
	createAdapter: () => new SqliteAdapter(),
	createDriver: () => new DummyDriver(),
	createIntrospector: (db: Kysely<any>) => new SqliteIntrospector(db),
	createQueryCompiler: () => new SqliteQueryCompiler(),
};

/**
 * Compile-only Kysely query builder for Lix.
 *
 * Usage:
 * const compiled = qb.selectFrom("file").selectAll().compile()
 * await lix.execute(compiled.sql, compiled.parameters)
 */
export const qb = new Kysely<LixDatabaseSchema>({
	dialect: compileOnlySqliteDialect,
});

