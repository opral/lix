#!/usr/bin/env node
// Build matching JS/native SDK artifacts first. See profile-indexed-import.md.
import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
const { openLix, Value } = await import(
  process.env.LIX_PROFILE_SDK ?? "../dist/index.js"
);

const sizes = (process.env.LIX_PROFILE_BUNDLES ?? "34,35,256,2048")
  .split(",")
  .map(Number);
const rounds = Number(process.env.LIX_PROFILE_ROUNDS ?? "5");
const warmups = Number(process.env.LIX_PROFILE_WARMUPS ?? "1");
assert(sizes.every((n) => Number.isSafeInteger(n) && n > 0));
assert(Number.isSafeInteger(rounds) && rounds > 0);
assert(Number.isSafeInteger(warmups) && warmups >= 0);
const samples = [];
const otlpExportBytes = [];
async function measure(bundleCount, round, operation, fn) {
  const start = performance.now();
  const result = await fn();
  const ms = performance.now() - start;
  if (round >= 0) samples.push({ bundles: bundleCount, round, operation, ms });
  return result;
}
for (let round = -warmups; round < rounds; round++) {
  // Rotate sizes so startup, thermal changes, and background activity are not
  // consistently charged to the same case.
  const rotated = sizes.map(
    (_, i) => sizes[(i + round + warmups) % sizes.length],
  );
  for (const count of rotated) {
    const lix = await openLix(
      process.env.LIX_PROFILE_TRACE === "1"
        ? { telemetry: { onExport: (request) => otlpExportBytes.push(request.byteLength) } }
        : undefined,
    );
    try {
      for (const [key, columns, foreign_keys] of [
        ["profile_bundle", [{ name: "id", type: "text", nullable: false }], []],
        [
          "profile_message",
          [
            { name: "id", type: "text", nullable: false },
            { name: "bundle_id", type: "text", nullable: false },
          ],
          [
            {
              columns: ["bundle_id"],
              references: { schema_key: "profile_bundle", columns: ["id"] },
            },
          ],
        ],
        [
          "profile_variant",
          [
            { name: "id", type: "text", nullable: false },
            { name: "message_id", type: "text", nullable: false },
            { name: "pattern", type: "text", nullable: false },
          ],
          [
            {
              columns: ["message_id"],
              references: { schema_key: "profile_message", columns: ["id"] },
            },
          ],
        ],
      ])
        await lix.execute(
          "INSERT INTO lix_registered_schema (value) VALUES ($1)",
          [
            Value.jsonb({
              $schema: "https://lix.dev/schema-v1.json",
              key,
              columns,
              foreign_keys,
              primary_key: ["id"],
            }),
          ],
        );
      const bundles = [],
        messages = [],
        variants = [];
      for (let i = 0; i < count; i++) {
        const id = `b${String(i).padStart(6, "0")}`;
        bundles.push(`('${id}')`);
        for (let locale = 0; locale < 7; locale++) {
          const mid = `${id}_m${locale}`;
          messages.push(`('${mid}','${id}')`);
          variants.push(`('${mid}_v','${mid}','Translated ${mid}')`);
        }
      }
      const statements = [
        `INSERT INTO profile_bundle (id) VALUES ${bundles.join(",")}`,
        `INSERT INTO profile_message (id,bundle_id) VALUES ${messages.join(",")}`,
        `INSERT INTO profile_variant (id,message_id,pattern) VALUES ${variants.join(",")}`,
      ];
      const tx = await lix.beginTransaction();
      await measure(count, round, "stage", async () => {
        for (const sql of statements) await tx.execute(sql);
      });
      await measure(count, round, "commit", () => tx.commit());
      const query =
        "SELECT id FROM profile_message WHERE bundle_id = 'b000000' ORDER BY id";
      const scan =
        "SELECT id FROM profile_message WHERE concat(bundle_id, '') = 'b000000' ORDER BY id";
      const cold = await measure(count, round, "equality_cold", () =>
        lix.execute(query),
      );
      assert.equal(cold.rows.length, 7);
      const scanCold = await measure(count, round, "scan_cold", () =>
        lix.execute(scan),
      );
      assert.deepEqual(cold.rows, scanCold.rows);
      for (let repeat = 0; repeat < 5; repeat++) {
        const indexed = await measure(count, round, "equality_warm", () =>
          lix.execute(query),
        );
        const full = await measure(count, round, "scan_warm", () =>
          lix.execute(scan),
        );
        assert.deepEqual(indexed.rows, full.rows);
      }
      const join =
        "SELECT b.id AS bid,m.id AS mid,v.pattern FROM profile_bundle b LEFT JOIN profile_message m ON m.bundle_id=b.id LEFT JOIN profile_variant v ON v.message_id=m.id ORDER BY b.id,m.id";
      for (let repeat = 0; repeat < 3; repeat++) {
        const result = await measure(
          count,
          round,
          repeat === 0 ? "join_cold" : "join_warm",
          () => lix.execute(join),
        );
        assert.equal(result.rows.length, count * 7);
        for (const [i, row] of result.rows.entries()) {
          const bid = `b${String(Math.floor(i / 7)).padStart(6, "0")}`;
          assert.deepEqual(row, {
            bid,
            mid: `${bid}_m${i % 7}`,
            pattern: `Translated ${bid}_m${i % 7}`,
          });
        }
      }
    } finally {
      await lix.close();
    }
  }
}
const summary = [];
for (const count of sizes)
  for (const operation of [...new Set(samples.map((s) => s.operation))]) {
    const values = samples
      .filter((s) => s.bundles === count && s.operation === operation)
      .map((s) => s.ms)
      .sort((a, b) => a - b);
    summary.push({
      bundles: count,
      rows: count * 15,
      operation,
      samples: values.length,
      medianMs: values[Math.floor(values.length / 2)],
      p95Ms:
        values[
          Math.min(values.length - 1, Math.ceil(values.length * 0.95) - 1)
        ],
    });
  }
console.log(
  JSON.stringify(
    {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      sizes,
      rounds,
      warmups,
      summary,
      samples,
      otlpExportBytes,
    },
    null,
    2,
  ),
);
