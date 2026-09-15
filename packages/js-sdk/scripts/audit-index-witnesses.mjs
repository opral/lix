import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
let cases = 0,
  failures = [];
for (const untrackedSchema of [false, true])
  for (const scope of ["main", "fork", "snapshot"])
    for (const rows of ["tracked", "untracked", "mixed"]) {
      if (untrackedSchema && (rows !== "untracked" || scope === "fork"))
        continue;
      const db = await openLix();
      const s = {
        $schema: "https://lix.dev/schema-v1.json",
        key: "note",
        columns: [
          { name: "id", type: "text", nullable: false },
          { name: "label", type: "text", nullable: true },
        ],
        primary_key: ["id"],
        unique: [["label"]],
      };
      const tag = `schema:${untrackedSchema} scope:${scope} rows:${rows}`;
      let handle = db;
      try {
        await db.execute(
          `INSERT INTO lix_registered_schema(value,lixcol_global,lixcol_untracked) VALUES($1,false,${untrackedSchema})`,
          [Value.jsonb(s)],
        );
        const vals = Array.from(
          { length: 65 },
          (_, i) =>
            `('n${i}','v${i}',${rows === "untracked" || (rows === "mixed" && i % 2 === 0)})`,
        ).join(",");
        await db.execute(
          "INSERT INTO note(id,label,lixcol_untracked) VALUES " + vals,
        );
        if (scope === "fork") {
          const b = await db.createBranch({ name: "fork" });
          await db.switchBranch({ branchId: b.id });
        }
        if (scope === "snapshot") {
          handle = await openLix.fromSnapshot(db.exportSnapshot());
        }
        const before = (
          await handle.execute(
            "SELECT id FROM note WHERE label='v1' ORDER BY id",
          )
        ).rows;
        await handle.execute(
          "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='note'",
          [Value.jsonb({ ...s, description: "amended" })],
        );
        for (const label of ["v0", "v1", "v64"]) {
          const a = (
            await handle.execute(
              "SELECT id FROM note WHERE label=$1 ORDER BY id",
              [label],
            )
          ).rows;
          const b = (
            await handle.execute(
              "SELECT id FROM note WHERE concat(label,'')=$1 ORDER BY id",
              [label],
            )
          ).rows;
          const index = Number(label.slice(1));
          const inherited =
            scope === "fork" &&
            (rows === "untracked" || (rows === "mixed" && index % 2 === 0));
          assert.equal(
            b.length,
            inherited ? 0 : 1,
            `${tag}: scan cardinality for ${label}`,
          );
          cases++;
          if (JSON.stringify(a) !== JSON.stringify(b))
            failures.push({ tag, label, index: a, scan: b, before });
        }
        console.log("CHECK", tag);
      } catch (e) {
        throw new Error(`${tag}: ${e.message}`, { cause: e });
      } finally {
        if (handle !== db) await handle.close();
        await db.close();
      }
    }
console.log(JSON.stringify({ cases, failures }, null, 2));

assert.equal(failures.length, 0, "schema amendments preserve indexed reads");

// Reads and mutations must observe the same current-schema defaults.
for (const untracked of [false, true]) {
  const db = await openLix();
  try {
    const schema = {
      $schema: "https://lix.dev/schema-v1.json",
      key: "amended_write",
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
      `INSERT INTO amended_write(id,value,lixcol_untracked) VALUES('a',1,${untracked}),('b',1,${untracked}),('c',1,${untracked})`,
    );
    schema.columns.push({
      name: "priority",
      type: "int8",
      nullable: false,
      default_value: 7,
    });
    await db.execute(
      "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='amended_write'",
      [Value.jsonb(schema)],
    );
    assert.equal(
      (await db.execute("SELECT id FROM amended_write WHERE priority=7")).rows
        .length,
      3,
    );
    await db.execute("UPDATE amended_write SET value=2 WHERE priority=7");
    assert.equal(
      (await db.execute("SELECT id FROM amended_write WHERE value=2")).rows
        .length,
      3,
    );
    await db.execute(
      "UPDATE amended_write SET priority=priority+1 WHERE id='a'",
    );
    assert.equal(
      Number(
        (await db.execute("SELECT priority FROM amended_write WHERE id='a'"))
          .rows[0].priority,
      ),
      8,
    );
    await db.execute("DELETE FROM amended_write WHERE priority=7");
    assert.deepEqual(
      (await db.execute("SELECT id FROM amended_write ORDER BY id")).rows,
      [{ id: "a" }],
    );
    console.log("PASS amended write predicates and expressions", { untracked });
  } finally {
    await db.close();
  }
}

// Pre-fix repositories retain sparse rows even though new amendments backfill.
const { readFile } = await import("node:fs/promises");
const sparseSnapshot = await readFile(
  new URL(
    "../../lix/tests/fixtures/schema-amendments/sparse-default-v81.lix",
    import.meta.url,
  ),
);
for (const route of ["predicate", "expression", "conflict", "delete"]) {
  const db = await openLix.fromSnapshot(sparseSnapshot);
  try {
    assert.equal(
      (await db.execute("SELECT id FROM legacy_default WHERE priority=7")).rows
        .length,
      3,
    );
    if (route === "predicate") {
      await db.execute("UPDATE legacy_default SET value=2 WHERE priority=7");
      assert.equal(
        (await db.execute("SELECT id FROM legacy_default WHERE value=2")).rows
          .length,
        3,
      );
    } else if (route === "expression") {
      await db.execute(
        "UPDATE legacy_default SET priority=priority+1 WHERE id='a'",
      );
      assert.equal(
        Number(
          (await db.execute("SELECT priority FROM legacy_default WHERE id='a'"))
            .rows[0].priority,
        ),
        8,
      );
    } else if (route === "conflict") {
      await db.execute(
        "INSERT INTO legacy_default(id,value) VALUES('b',3) ON CONFLICT(id) DO UPDATE SET value=legacy_default.priority",
      );
      assert.equal(
        Number(
          (await db.execute("SELECT value FROM legacy_default WHERE id='b'"))
            .rows[0].value,
        ),
        7,
      );
    } else {
      await db.execute("DELETE FROM legacy_default WHERE priority=7");
      assert.equal(
        (await db.execute("SELECT id FROM legacy_default")).rows.length,
        0,
      );
    }
    console.log("PASS legacy sparse-default fixture", { route });
  } finally {
    await db.close();
  }
}

const sparseExpressionSnapshot = await readFile(
  new URL(
    "../../lix/tests/fixtures/schema-amendments/sparse-expression-v81.lix",
    import.meta.url,
  ),
);
const recovery = await openLix.fromSnapshot(sparseExpressionSnapshot);
try {
  const query = "SELECT stable_id,created FROM legacy_expression";
  const reapply =
    "UPDATE lix_registered_schema SET value=value WHERE schema_key='legacy_expression'";
  const beforeHead = (
    await recovery.execute("SELECT lix_active_branch_commit_id() AS head")
  ).rows;
  await assert.rejects(recovery.execute(query), /expression default/);
  assert.deepEqual(
    (await recovery.execute("SELECT lix_active_branch_commit_id() AS head"))
      .rows,
    beforeHead,
  );
  const rollback = await recovery.beginTransaction();
  await rollback.execute(reapply);
  await rollback.rollback();
  await assert.rejects(recovery.execute(query), /expression default/);
  await recovery.execute(reapply);
  const stable = (await recovery.execute(query)).rows;
  assert.equal(stable.length, 1);
  assert.ok(stable[0].stable_id);
  assert.ok(stable[0].created);
  assert.deepEqual((await recovery.execute(query)).rows, stable);
  const reopened = await openLix.fromSnapshot(recovery.exportSnapshot());
  try {
    assert.deepEqual((await reopened.execute(query)).rows, stable);
  } finally {
    await reopened.close();
  }
  console.log("PASS legacy expression-default explicit recovery");
} finally {
  await recovery.close();
}
