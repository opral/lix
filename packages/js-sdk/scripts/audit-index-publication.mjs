// Boundary, constraint, update-certificate, and snapshot audit.
import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
const schema = (key, columns, extra = {}) => ({
  $schema: "https://lix.dev/schema-v1.json",
  key,
  columns: columns.map((name) => ({ name, type: "text", nullable: false })),
  primary_key: ["id"],
  ...extra,
});
let checks = 0;
async function reject(db, sql, code = "LIX_ERROR_UNIQUE") {
  await assert.rejects(() => db.execute(sql), { code });
  checks++;
}
async function check(db, label, n) {
  const r = await db.execute(`SELECT id FROM p WHERE slug='${label}'`);
  assert.equal(r.rows.length, n, `slug ${label}`);
  checks++;
}
for (const count of [511, 512, 513, 1024]) {
  const db = await openLix();
  for (const s of [
    schema("p", ["id", "slug", "payload"], { unique: [["slug"]] }),
    schema("c", ["id", "pid", "payload"], {
      foreign_keys: [
        { columns: ["pid"], references: { schema_key: "p", columns: ["id"] } },
      ],
    }),
  ])
    await db.execute("INSERT INTO lix_registered_schema(value) VALUES ($1)", [
      Value.jsonb(s),
    ]);
  await db.execute(
    `INSERT INTO p(id,slug,payload) VALUES ${Array.from({ length: count }, (_, i) => `('p${i}','s${i}','a')`).join(",")}`,
  );
  await db.execute(
    `INSERT INTO c(id,pid,payload) VALUES ${Array.from({ length: count }, (_, i) => `('c${i}','p${i}','a')`).join(",")}`,
  );
  await reject(db, `INSERT INTO p(id,slug,payload) VALUES ('dup','s0','a')`);
  await reject(db, `UPDATE p SET slug='s0' WHERE id='p1'`);
  await reject(
    db,
    `UPDATE c SET pid='missing' WHERE id='c0'`,
    "LIX_ERROR_FOREIGN_KEY",
  );
  await reject(db, `DELETE FROM p WHERE id='p0'`, "LIX_ERROR_FOREIGN_KEY");
  await db.execute(`UPDATE p SET payload='b'`);
  await db.execute(`UPDATE c SET payload='b'`);
  await check(db, "s0", 1);
  await check(db, `s${count - 1}`, 1);
  await db.execute(`UPDATE p SET slug=concat('new_',slug)`);
  await check(db, "s0", 0);
  await check(db, "new_s0", 1);
  await reject(
    db,
    `INSERT INTO p(id,slug,payload) VALUES ('dup','new_s0','a')`,
  );
  await db.execute(`UPDATE c SET pid='p0'`);
  assert.equal(
    (await db.execute(`SELECT id FROM c WHERE pid='p0'`)).rows.length,
    count,
  );
  checks++;
  assert.equal(
    (await db.execute(`SELECT id FROM c WHERE pid='p1'`)).rows.length,
    0,
  );
  checks++;
  await reject(db, `DELETE FROM p WHERE id='p0'`, "LIX_ERROR_FOREIGN_KEY");
  const snapshot = new Uint8Array(
    await new Response(db.exportSnapshot()).arrayBuffer(),
  );
  await db.close();
  const reopened = await openLix.fromSnapshot(snapshot);
  await check(reopened, "new_s0", 1);
  await reject(
    reopened,
    `INSERT INTO p(id,slug,payload) VALUES ('dup','new_s0','a')`,
  );
  await reject(
    reopened,
    `DELETE FROM p WHERE id='p0'`,
    "LIX_ERROR_FOREIGN_KEY",
  );
  await reopened.close();
  console.log({ count, checks, status: "pass" });
}
