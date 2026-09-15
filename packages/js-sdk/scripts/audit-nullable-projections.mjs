// Run after building the native SDK: node packages/js-sdk/scripts/audit-nullable-projections.mjs
// Override sizes with SIZES=63,64,65. Each query is compared to an independent
// JavaScript filter over a full SQL scan, before and after updates/deletes.
import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
const stable = (rows) => rows.map((r) => JSON.stringify(r)).sort();
let checks = 0;
const sizes = process.env.SIZES
  ? process.env.SIZES.split(",").map(Number)
  : [65, 255, 513, 1025];
for (const n of sizes) {
  const lix = await openLix();
  const columns = [
    { name: "id", type: "text", nullable: false },
    ...["int8", "float8", "boolean", "timestamptz", "jsonb", "uuid"].map(
      (type) => ({ name: type, type, nullable: true }),
    ),
  ];
  await lix.execute("INSERT INTO lix_registered_schema (value) VALUES ($1)", [
    Value.jsonb({
      $schema: "https://lix.dev/schema-v1.json",
      key: "audit_types",
      columns,
      primary_key: ["id"],
    }),
  ]);
  const values = [];
  // Stay within JavaScript's exact integer range; preserve timestamp microseconds.
  for (let i = 0; i < n; i++) {
    const row = [
      `'id_${String(i).padStart(5, "0")}'`,
      i % 3 === 0 ? "NULL" : i % 2 ? "-9007199254740991" : "9007199254740991",
      i % 5 === 0 ? "NULL" : i % 2 ? "-1.25e100" : "1.25e-100",
      i % 7 === 0 ? "NULL" : i % 2 ? "TRUE" : "FALSE",
      i % 11 === 0 ? "NULL" : "'2026-09-15T01:02:03.123456Z'",
      i % 13 === 0 ? "NULL" : `'${JSON.stringify({ a: [null, true, 1] })}'`,
      i % 17 === 0 ? "NULL" : "'01920000-0000-7000-8000-000000000001'",
    ];
    values.push(`(${row.join(",")})`);
  }
  await lix.execute(
    "INSERT INTO audit_types (id,int8,float8,boolean,timestamptz,jsonb,uuid) VALUES " +
      values.join(","),
  );
  for (const stage of ["insert", "update", "delete"]) {
    if (stage === "update")
      await lix.execute(
        "UPDATE audit_types SET int8=NULL,float8=NULL,boolean=NULL,timestamptz=NULL,jsonb=NULL,uuid=NULL WHERE id IN ('id_00060','id_00061')",
      );
    if (stage === "delete")
      await lix.execute(
        "DELETE FROM audit_types WHERE id IN ('id_00062','id_00063')",
      );
    const all = (await lix.execute("SELECT * FROM audit_types")).rows;
    for (let repeat = 0; repeat < 2; repeat++) {
      const where = [
        [
          "id >= 'id_00060' AND id < 'id_00070'",
          (r) => r.id >= "id_00060" && r.id < "id_00070",
        ],
        ...["int8", "float8", "boolean", "timestamptz", "jsonb", "uuid"].map(
          (k) => [`${k} IS NULL`, (r) => r[k] === null],
        ),
        ["boolean=TRUE", (r) => r.boolean === true],
      ];
      for (const [sql, pred] of where) {
        let got;
        try {
          got = (await lix.execute(`SELECT * FROM audit_types WHERE ${sql}`))
            .rows;
        } catch (e) {
          console.error({ n, stage, sql });
          throw e;
        }
        assert.deepEqual(
          stable(got),
          stable(all.filter(pred)),
          `${n} ${stage} ${sql}`,
        );
        checks++;
      }
    }
  }
  console.log(JSON.stringify({ n, checks, status: "pass" }));
  await lix.close();
}
