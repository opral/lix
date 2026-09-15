import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
assert.ok(
  process.argv[2],
  "Pass the historical packages/js-sdk/dist/index.js path",
);
const { openLix, Value } = await import(
  pathToFileURL(resolve(process.argv[2])).href
);
const db = await openLix();
try {
  const schema = {
    $schema: "https://lix.dev/schema-v1.json",
    key: "legacy_expression",
    columns: [{ name: "id", type: "text", nullable: false }],
    primary_key: ["id"],
  };
  await db.execute("INSERT INTO lix_registered_schema(value) VALUES($1)", [
    Value.jsonb(schema),
  ]);
  await db.execute("INSERT INTO legacy_expression(id) VALUES('a')");
  schema.columns.push(
    {
      name: "stable_id",
      type: "uuid",
      nullable: false,
      default_expression: "uuidv7()",
    },
    {
      name: "created",
      type: "timestamptz",
      nullable: false,
      default_expression: "CURRENT_TIMESTAMP",
    },
  );
  await db.execute(
    "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='legacy_expression'",
    [Value.jsonb(schema)],
  );
  await assert.rejects(
    db.execute("SELECT stable_id,created FROM legacy_expression"),
    "The historical producer must leave old rows without generated values",
  );
  await writeFile(
    new URL("./sparse-expression-v81.lix", import.meta.url),
    new Uint8Array(await new Response(db.exportSnapshot()).arrayBuffer()),
  );
} finally {
  await db.close();
}
