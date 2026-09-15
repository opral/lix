// Run with the historical SDK, not the current checkout's SDK.
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
    key: "legacy_default",
    columns: [
      { name: "id", type: "text", nullable: false },
      { name: "value", type: "int8", nullable: false },
    ],
    primary_key: ["id"],
  };
  await db.execute("INSERT INTO lix_registered_schema(value) VALUES($1)", [
    Value.jsonb(schema),
  ]);
  await db.execute(
    "INSERT INTO legacy_default(id,value) VALUES('a',1),('b',1),('c',1)",
  );
  schema.columns.push({
    name: "priority",
    type: "int8",
    nullable: false,
    default_value: 7,
  });
  await db.execute(
    "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='legacy_default'",
    [Value.jsonb(schema)],
  );
  // A current SDK backfills the default and must not replace this fixture.
  const result = await db.execute(
    "UPDATE legacy_default SET value=2 WHERE priority=7",
  );
  assert.equal(
    result.rowsAffected,
    0,
    "Fixture producer must retain the historical sparse-row behavior",
  );
  const bytes = new Uint8Array(
    await new Response(db.exportSnapshot()).arrayBuffer(),
  );
  await writeFile(new URL("./sparse-default-v81.lix", import.meta.url), bytes);
  console.log(`Wrote ${bytes.length} bytes`);
} finally {
  await db.close();
}
