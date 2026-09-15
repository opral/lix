// Usage: SDK_INDEX=/absolute/sdk/dist/index.js node audit-legacy-index.mjs create|verify /absolute/storage-dir /absolute/snapshot.bin
// Run create with an affected v81 native SDK, then verify with the fixed v81 SDK.
import assert from "node:assert/strict";
import { writeFile, readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
const [phase, path, snapshotPath] = process.argv.slice(2);
const sdkUrl = process.env.SDK_INDEX
  ? pathToFileURL(process.env.SDK_INDEX)
  : new URL("../dist/index.js", import.meta.url);
const { openLix, Value } = await import(sdkUrl);
const storage = {
  lixStorage: {
    version: 1,
    config: { kind: "filesystem", path, syncAllFiles: false },
    connect() {},
  },
};
const schema = {
  $schema: "https://lix.dev/schema-v1.json",
  key: "legacy_note",
  columns: [
    { name: "id", type: "text", nullable: false },
    { name: "label", type: "text", nullable: false },
  ],
  primary_key: ["id"],
  unique: [["label"]],
};
async function rows(db) {
  return (await db.execute("SELECT id FROM legacy_note WHERE label='s0'")).rows;
}
if (phase === "create") {
  const db = await openLix({ storage });
  await db.execute(
    "INSERT INTO lix_registered_schema(value,lixcol_global,lixcol_untracked) VALUES ($1,false,false)",
    [Value.jsonb(schema)],
  );
  await db.execute("INSERT INTO legacy_note(id,label) VALUES ('seed','seed')");
  await db.execute(
    `INSERT INTO legacy_note(id,label) VALUES ${Array.from({ length: 512 }, (_, i) => `('p${String(i).padStart(3, "0")}','s${i}')`).join(",")}`,
  );
  assert.equal(
    (await db.execute("SELECT id FROM legacy_note")).rows.length,
    513,
  );
  assert.deepEqual(
    await rows(db),
    [],
    "fixture must demonstrate missing indexed rows",
  );
  await writeFile(
    snapshotPath,
    new Uint8Array(await new Response(db.exportSnapshot()).arrayBuffer()),
  );
  await db.close();
  console.log(
    JSON.stringify({ phase, path, snapshotPath, status: "reproduced" }),
  );
} else if (phase === "verify") {
  for (const mode of ["persistent", "snapshot"]) {
    const db =
      mode === "persistent"
        ? await openLix({ storage })
        : await openLix.fromSnapshot(
            new Uint8Array(await readFile(snapshotPath)),
          );
    assert.deepEqual(await rows(db), [{ id: "p000" }], `${mode} reopen`);
    await db.execute(
      "INSERT INTO legacy_note(id,label) VALUES ('fresh','fresh')",
    );
    assert.deepEqual(
      await rows(db),
      [{ id: "p000" }],
      `${mode} incremental write`,
    );
    await db.execute(
      "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='legacy_note'",
      [Value.jsonb({ ...schema, description: "amended" })],
    );
    assert.deepEqual(await rows(db), [{ id: "p000" }], `${mode} amendment`);
    await assert.rejects(
      () =>
        db.execute(
          "INSERT INTO legacy_note(id,label) VALUES ('duplicate','s0')",
        ),
      { code: "LIX_ERROR_UNIQUE" },
    );
    await db.close();
    console.log(JSON.stringify({ phase, mode, status: "pass" }));
  }
} else throw Error("create or verify phase required");
