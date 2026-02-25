import type { Insertable, Selectable } from "kysely";
import { qb } from "./index.js";
import type { LixDatabaseSchema } from "./schema.js";

type Equal<A, B> =
	(<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
		? true
		: false;

type Expect<T extends true> = T;

type FileRow = Selectable<LixDatabaseSchema["file"]>;
type _FilePathIsString = Expect<Equal<FileRow["path"], string>>;
const fileHiddenBoolean: FileRow["hidden"] = true;
const fileHiddenUndefined: FileRow["hidden"] = undefined;
// @ts-expect-error wrong hidden type
const fileHiddenString: FileRow["hidden"] = "true";
void fileHiddenBoolean;
void fileHiddenUndefined;
void fileHiddenString;

type KeyValueByVersionInsert = Insertable<
	LixDatabaseSchema["key_value_by_version"]
>;

type _InsertHasKey = Expect<Equal<KeyValueByVersionInsert["key"], string>>;

qb.selectFrom("file").select(["id", "path", "hidden"]).compile();
qb.selectFrom("directory").select(["id", "path"]).compile();
qb.selectFrom("key_value_by_version")
	.select(["key", "value", "lixcol_version_id"])
	.compile();

qb.insertInto("key_value_by_version")
	.values({
		key: "flashtype_active_file_id",
		value: "file-1",
		lixcol_version_id: "global",
		lixcol_untracked: true,
	})
	.compile();

qb.updateTable("key_value_by_version")
	.set({ value: "file-2" })
	.where("key", "=", "flashtype_active_file_id")
	.compile();

qb.deleteFrom("key_value_by_version")
	.where("key", "=", "flashtype_active_file_id")
	.compile();

// @ts-expect-error unknown table
qb.selectFrom("not_a_table").selectAll().compile();

// @ts-expect-error unknown column
qb.selectFrom("file").select(["not_a_column"]).compile();

const badInsert: Insertable<LixDatabaseSchema["key_value_by_version"]> = {
	key: "x",
	value: "y",
	// @ts-expect-error wrong column type
	lixcol_untracked: "yes",
};
void badInsert;
