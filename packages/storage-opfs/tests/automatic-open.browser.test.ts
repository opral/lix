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
    const checkpoints = await lix.execute("SELECT commit_id FROM lix_log() WHERE is_checkpoint");
    expect(checkpoints.rows.length).toBeGreaterThanOrEqual(3);
    const file = await lix.execute("SELECT content FROM lix_file WHERE path = '/docs/released-v75.bin'");
    expect((file.rows[0]?.content as Uint8Array).length).toBe(65537);
  } finally { await lix.close(); }
  const reopened = await openLix({ storage: new OpfsStorage({ name }) });
  try { expect(reopened.openReport?.migrations).toEqual([]); }
  finally { await reopened.close(); }
}, 120_000);

test("host opening profile separates a fresh OPFS store from its persisted reopen", async () => {
	const name = `host-profile-${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		expect(first.openReport?.initialized).toBe(true);
		const profile = first.openReport?.hostProfile;
		expect(profile?.version).toBe(1);
		expect(profile?.wasm.waitMs).toEqual(expect.any(Number));
		expect(profile?.wasm.source).toMatch(/^(bundled|cache|network|realm)$/);
		expect(profile?.componentCompiler.importMs).toEqual(expect.any(Number));
		expect(profile?.componentCompiler.initializeMs).toEqual(expect.any(Number));
		expect(profile?.provider?.moduleImportMs).toEqual(expect.any(Number));
		expect(profile?.provider?.createMs).toEqual(expect.any(Number));
		expect(profile?.provider?.openMs).toEqual(expect.any(Number));
		expect(profile?.provider?.opfs?.lockWaitMs).toEqual(expect.any(Number));
		expect(profile?.provider?.opfs?.sqliteInitMs).toEqual(expect.any(Number));
		expect(profile?.provider?.opfs?.poolOpenMs).toEqual(expect.any(Number));
		expect(profile?.provider?.opfs?.schemaInitMs).toEqual(expect.any(Number));
		expect(profile?.nativeBindingOpenMs).toEqual(expect.any(Number));
	} finally {
		await first.close();
	}

	const reopened = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		expect(reopened.openReport?.initialized).toBe(false);
		expect(reopened.openReport?.hostProfile?.provider?.opfs).toEqual({
			lockWaitMs: expect.any(Number),
			sqliteInitMs: expect.any(Number),
			poolOpenMs: expect.any(Number),
			schemaInitMs: expect.any(Number),
		});
		const wasm = reopened.openReport?.hostProfile?.wasm;
		if (wasm?.realmReused) {
			expect(wasm).toEqual({
				waitMs: expect.any(Number),
				realmReused: true,
				source: "realm",
				cacheStatus: "not_consulted",
			});
		}
	} finally {
		await reopened.close();
	}
}, 120_000);
