import { openLix, Value } from "../dist/index.js";
import assert from "node:assert/strict";
let checks = 0;
async function check(db, stage) {
  for (const val of ["seed", "v000", "v511", "v1023", "new_v000", "new_v511"]) {
    const a = (
      await db.execute("SELECT id FROM note WHERE label=$1 ORDER BY id", [val])
    ).rows;
    const b = (
      await db.execute(
        "SELECT id FROM note WHERE concat(label,'')=$1 ORDER BY id",
        [val],
      )
    ).rows;
    assert.deepEqual(a, b, stage + ":" + val);
    checks++;
  }
  console.log("PASS", stage);
}
for (const untracked of [false, true])
  for (const count of [511, 512, 513, 1024]) {
    const db = await openLix();
    const main = await db.activeBranchId();
    await db.execute("INSERT INTO lix_registered_schema(value) VALUES($1)", [
      Value.jsonb({
        $schema: "https://lix.dev/schema-v1.json",
        key: "note",
        columns: [
          { name: "id", type: "text", nullable: false },
          { name: "label", type: "text", nullable: true },
        ],
        primary_key: ["id"],
        unique: [["label"]],
      }),
    ]);
    await db.execute(
      `INSERT INTO note(id,label,lixcol_untracked) VALUES('seed','seed',${untracked})`,
    );
    const vals = Array.from(
      { length: count },
      (_, i) =>
        `('n${String(i).padStart(4, "0")}','v${String(i).padStart(3, "0")}',${untracked})`,
    ).join(",");
    await db.execute(
      "INSERT INTO note(id,label,lixcol_untracked) VALUES " + vals,
    );
    const stage = `${count}:${untracked}`;
    await check(db, stage + " insert");
    const cp = (
      await db.execute("SELECT commit_id FROM lix_create_checkpoint($1, $2)", ["Index audit", { _type: "zettel_doc", blocks: [] }])
    ).rows[0].commit_id;
    await check(db, stage + " checkpoint");
    const reopened = await openLix.fromSnapshot(db.exportSnapshot());
    await check(reopened, stage + " reopen");
    await reopened.close();
    const branch = await db.createBranch({ name: "fork" });
    await db.switchBranch({ branchId: branch.id });
    await check(db, stage + " fork");
    await db.execute("UPDATE note SET label=concat('new_',label)");
    await check(db, stage + " replacement");
    if (!untracked) {
      await db.execute("SELECT commit_id FROM lix_undo()");
      await check(db, stage + " undo");
      await db.execute("SELECT commit_id FROM lix_redo()");
      await check(db, stage + " redo");
    }
    await db.switchBranch({ branchId: main });
    await check(db, stage + " main");
    await db.switchBranch({ branchId: branch.id });
    await check(db, stage + " fork again");
    await db.execute("DELETE FROM note WHERE concat(label,'') LIKE 'new_v%'");
    await check(db, stage + " delete");
    await db.close();
  }
console.log({ checks });
