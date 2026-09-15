import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
const db = await openLix();
let checks = 0;
const schema = (key, columns, extra) => ({
  $schema: "https://lix.dev/schema-v1.json",
  key,
  columns,
  primary_key: ["id"],
  ...extra,
});
const col = (name, type, nullable = false) => ({ name, type, nullable });
for (const s of [
  schema("parent", [col("id", "int8"), col("text_key", "text")], {
    unique: [["text_key"]],
  }),
  schema(
    "child",
    [col("id", "text"), col("fk", "int8", true), col("tfk", "text", true)],
    {
      foreign_keys: [
        {
          columns: ["fk"],
          references: { schema_key: "parent", columns: ["id"] },
        },
        {
          columns: ["tfk"],
          references: { schema_key: "parent", columns: ["text_key"] },
        },
      ],
    },
  ),
])
  await db.execute("INSERT INTO lix_registered_schema(value) VALUES($1)", [
    Value.jsonb(s),
  ]);
await db.execute(
  "INSERT INTO parent(id,text_key) VALUES " +
    Array.from({ length: 130 }, (_, i) => `(${i},'p${i}')`).join(","),
);
for (const untracked of [false, true])
  await db.execute(
    "INSERT INTO child(id,fk,tfk,lixcol_untracked) VALUES " +
      Array.from(
        { length: 1024 },
        (_, i) =>
          `('${untracked ? "u" : "t"}${i}',${i % 130},'p${i % 130}',${untracked})`,
      ).join(","),
  );
await db.execute("INSERT INTO child(id,fk,tfk) VALUES ('null',NULL,NULL)");
async function compare(label, sql, oracle) {
  const a = (await db.execute(sql)).rows,
    b = (await db.execute(oracle)).rows;
  assert.deepEqual(a, b, label);
  checks++;
  console.log("PASS", label, a.length);
}
async function audit(stage) {
  for (const n of [1, 63, 64, 65, 129, 130]) {
    const ints = Array.from({ length: n }, (_, i) => i).join(","),
      strs = Array.from({ length: n }, (_, i) => `'p${i}'`).join(",");
    for (const [c, expression, vals] of [
      ["fk", "fk + 0", ints],
      ["tfk", "concat(tfk,'')", strs],
    ])
      await compare(
        `${stage} ${c} IN ${n}`,
        `SELECT id FROM child WHERE ${c} IN (${vals}) ORDER BY id`,
        `SELECT id FROM child WHERE ${expression} IN (${vals}) ORDER BY id`,
      );
  }
  for (const [c, expression, v] of [
    ["fk", "fk + 0", "0"],
    ["tfk", "concat(tfk,'')", "'p0'"],
  ]) {
    await compare(
      `${stage} ${c} equality`,
      `SELECT id FROM child WHERE ${c}=${v} ORDER BY id`,
      `SELECT id FROM child WHERE ${expression}=${v} ORDER BY id`,
    );
    await compare(
      `${stage} ${c} range`,
      `SELECT id FROM child WHERE ${c}>=${v} ORDER BY id`,
      `SELECT id FROM child WHERE ${expression}>=${v} ORDER BY id`,
    );
  }
  await compare(
    stage + " nullable",
    "SELECT id FROM child WHERE fk IS NULL ORDER BY id",
    "SELECT id FROM child WHERE fk + 0 IS NULL ORDER BY id",
  );
  await compare(
    stage + " join",
    "SELECT p.id,c.id AS cid FROM parent p LEFT JOIN child c ON c.fk=p.id ORDER BY p.id,c.id",
    "SELECT p.id,c.id AS cid FROM parent p LEFT JOIN child c ON c.fk+0=p.id ORDER BY p.id,c.id",
  );
}
await audit("mixed lanes");
await db.execute("UPDATE child SET fk=0,tfk='p0' WHERE fk IS NOT NULL");
await audit("oversized bucket");
await db.execute("UPDATE child SET fk=129,tfk='p129' WHERE fk IS NOT NULL");
await audit("stale entries");
await db.execute("DELETE FROM child WHERE id LIKE 't%'");
await audit("tracked deleted");
await db.close();
console.log({ checks });
