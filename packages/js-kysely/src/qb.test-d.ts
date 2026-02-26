import type { Insertable, Selectable } from "kysely";
import { ebEntity, qb } from "./index.js";
import type { LixDatabaseSchema } from "./schema.js";

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;

type Expect<T extends true> = T;

type FileRow = Selectable<LixDatabaseSchema["lix_file"]>;
type _FilePathIsString = Expect<Equal<FileRow["path"], string>>;
const fileHiddenBoolean: FileRow["hidden"] = true;
const fileHiddenUndefined: FileRow["hidden"] = undefined;
// @ts-expect-error wrong hidden type
const fileHiddenString: FileRow["hidden"] = "true";
void fileHiddenBoolean;
void fileHiddenUndefined;
void fileHiddenString;

type KeyValueByVersionInsert = Insertable<
	LixDatabaseSchema["lix_key_value_by_version"]
>;

type _InsertHasKey = Expect<Equal<KeyValueByVersionInsert["key"], string>>;

const db = qb({
	execute: async () => ({ rows: [] }),
});

const dbWithWriter = qb(
	{
		execute: async () => ({ rows: [] }),
	},
	{ writerKey: "writer-a" },
);
dbWithWriter.selectFrom("lix_file").select("id").compile();

db.selectFrom("lix_file").select(["id", "path", "hidden"]).compile();
db.selectFrom("lix_directory").select(["id", "path"]).compile();
db.selectFrom("lix_key_value_by_version")
	.select(["key", "value", "lixcol_version_id"])
	.compile();

db.selectFrom("lix_commit")
	.where(ebEntity("lix_commit").hasLabel({ name: "checkpoint" }))
	.select("id")
	.compile();

db.insertInto("lix_key_value_by_version")
	.values({
		key: "flashtype_active_file_id",
		value: "file-1",
		lixcol_version_id: "global",
		lixcol_untracked: true,
	})
	.compile();

db.updateTable("lix_key_value_by_version")
	.set({ value: "file-2" })
	.where("key", "=", "flashtype_active_file_id")
	.compile();

db.deleteFrom("lix_key_value_by_version")
	.where("key", "=", "flashtype_active_file_id")
	.compile();

const withDb = qb({ db });
withDb.selectFrom("lix_file").select("id");

// @ts-expect-error unknown table
db.selectFrom("not_a_table").selectAll().compile();

// @ts-expect-error unknown column
db.selectFrom("lix_file").select(["not_a_column"]).compile();

const badInsert: Insertable<LixDatabaseSchema["lix_key_value_by_version"]> = {
	key: "x",
	value: "y",
	// @ts-expect-error wrong column type
	lixcol_untracked: "yes",
};
void badInsert;
