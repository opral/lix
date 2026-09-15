// Run after building the native SDK: node packages/js-sdk/scripts/audit-indexed-queries.mjs
// Override sizes with SIZES=63,64,65. Each query is compared to an independent
// JavaScript filter over a full SQL scan; mutation results also retain an oracle.
import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
let checks = 0;
const sizes = process.env.SIZES
  ? process.env.SIZES.split(",").map(Number)
  : [63, 64, 65, 255, 256, 257, 511, 512, 513, 1025];
const stable = (rows) => rows.map((r) => JSON.stringify(r)).sort();
for (const n of sizes) {
  const lix = await openLix();
  await lix.execute("INSERT INTO lix_registered_schema (value) VALUES ($1)", [
    Value.jsonb({
      $schema: "https://lix.dev/schema-v1.json",
      key: "audit_scan",
      columns: [
        { name: "id", type: "text", nullable: false },
        { name: "bucket", type: "text", nullable: true },
        { name: "num", type: "int8", nullable: true },
      ],
      primary_key: ["id"],
    }),
  ]);
  const expected = new Map();
  for (let i = 0; i < n; i++)
    expected.set(`id_${String(i).padStart(5, "0")}`, {
      id: `id_${String(i).padStart(5, "0")}`,
      bucket: i % 13 === 0 ? null : `b${i % 71}`,
      num: i % 17 === 0 ? null : i % 97,
    });
  const lit = (x) =>
    x === null ? "NULL" : typeof x === "number" ? String(x) : `'${x}'`;
  await lix.execute(
    "INSERT INTO audit_scan (id,bucket,num) VALUES " +
      [...expected.values()]
        .map((r) => `(${lit(r.id)},${lit(r.bucket)},${lit(r.num)})`)
        .join(","),
  );
  async function verify(stage) {
    const all = (await lix.execute("SELECT id,bucket,num FROM audit_scan"))
      .rows;
    assert.deepEqual(
      stable(all),
      stable([...expected.values()]),
      `${n} ${stage} full`,
    );
    checks++;
    const predicates = [
      ["bucket = 'b1'", (r) => r.bucket === "b1"],
      ["bucket IS NULL", (r) => r.bucket === null],
      ["num IS NULL", (r) => r.num === null],
      ["num = 42", (r) => r.num === 42],
      [
        "num >= 45 AND num <= 60",
        (r) => r.num !== null && r.num >= 45 && r.num <= 60,
      ],
      [
        "bucket IN ('b1','b2',NULL)",
        (r) => r.bucket === "b1" || r.bucket === "b2",
      ],
      ["num IN (1,2,3,4,5)", (r) => [1, 2, 3, 4, 5].includes(r.num)],
      ["id = 'id_00000'", (r) => r.id === "id_00000"],
      [
        "id >= 'id_00060' AND id < 'id_00070'",
        (r) => r.id >= "id_00060" && r.id < "id_00070",
      ],
    ];
    for (const count of [63, 64, 65, 129]) {
      const ids = Array.from(
        { length: count },
        (_, i) => `id_${String(i * 2).padStart(5, "0")}`,
      );
      predicates.push([
        `id IN (${ids.map(lit).join(",")})`,
        (r) => ids.includes(r.id),
      ]);
    }
    for (let repeat = 0; repeat < 2; repeat++)
      for (const [where, pred] of predicates) {
        let rows;
        try {
          rows = (
            await lix.execute(
              `SELECT id,bucket,num FROM audit_scan WHERE ${where}`,
            )
          ).rows;
        } catch (e) {
          console.error({ n, stage, where });
          throw e;
        }
        assert.deepEqual(
          stable(rows),
          stable(all.filter(pred)),
          `${n} ${stage} ${where}`,
        );
        checks++;
      }
    for (const op of ["JOIN", "LEFT JOIN"]) {
      const got = (
        await lix.execute(
          `SELECT a.id AS aid,b.id AS bid FROM audit_scan a ${op} audit_scan b ON a.bucket=b.bucket WHERE a.id < 'id_00070'`,
        )
      ).rows;
      const want = [];
      for (const a of all.filter((r) => r.id < "id_00070")) {
        const matches = all.filter(
          (b) => a.bucket !== null && a.bucket === b.bucket,
        );
        for (const b of matches) want.push({ aid: a.id, bid: b.id });
        if (!matches.length && op === "LEFT JOIN")
          want.push({ aid: a.id, bid: null });
      }
      assert.deepEqual(stable(got), stable(want), `${n} ${stage} ${op}`);
      checks++;
    }
  }
  await verify("insert");
  await lix.execute(
    "UPDATE audit_scan SET bucket='b1',num=42 WHERE num IN (45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60)",
  );
  for (const r of expected.values())
    if (r.num !== null && r.num >= 45 && r.num <= 60) {
      r.bucket = "b1";
      r.num = 42;
    }
  await verify("update");
  await lix.execute("DELETE FROM audit_scan WHERE bucket IN ('b2','b3')");
  for (const [k, r] of expected)
    if (["b2", "b3"].includes(r.bucket)) expected.delete(k);
  await verify("delete");
  await lix.close();
  console.log(JSON.stringify({ n, checks, status: "pass" }));
}
