import { expect, test } from "vitest";
import { openLix, type LixOpenProgress } from "@lix-js/sdk";
import { OpfsStorage } from "../dist/index.js";

test("normal WASM opening upgrades released OPFS history without a migration artifact", async () => {
  const name = `released-open-${crypto.randomUUID()}`;
  const snapshot = await (await fetch(new URL("../../lix/tests/fixtures/v75_released_repository.lixsnap", import.meta.url))).arrayBuffer();
  const worker = new Worker(new URL("./released-open-fixture.worker.ts", import.meta.url), { type: "module" });
  try {
    await new Promise<void>((resolve, reject) => {
      worker.onerror = event => reject(new Error(event.message));
      worker.onmessage = ({ data }) => data.error ? reject(new Error(data.error)) : resolve();
      worker.postMessage({ name, snapshot }, [snapshot]);
    });
  } finally { worker.terminate(); }
  const progress: LixOpenProgress[] = [];
  const lix = await openLix({ storage: new OpfsStorage({ name }), onProgress: event => { progress.push(event); if (event.phase === "migrating") throw new Error("UI failed"); } });
  try {
    expect(lix.openReport?.migrations).toEqual([{ scope: "local", fromFormat: 75, toFormat: lix.openReport?.format }]);
    expect(progress.some(event => event.phase === "migrating" && event.scope === "local")).toBe(true);
    const rows = await lix.execute("SELECT value FROM lix_key_value WHERE key = 'fixture-shared'");
    expect(rows.rows).toEqual([{ value: { generation: 75, lane: "main" } }]);
    const checkpoints = await lix.execute("SELECT id FROM lix_commit WHERE is_checkpoint");
    expect(checkpoints.rows.length).toBeGreaterThanOrEqual(3);
    const file = await lix.execute("SELECT content FROM lix_file WHERE path = '/docs/released-v75.bin'");
    expect((file.rows[0]?.content as Uint8Array).length).toBe(65537);
  } finally { await lix.close(); }
  const reopened = await openLix({ storage: new OpfsStorage({ name }) });
  try { expect(reopened.openReport?.migrations).toEqual([]); }
  finally { await reopened.close(); }
}, 120_000);
