import { expect, test } from "vitest";

async function runScenario(scenario: string) {
	const worker = new Worker(new URL("./open-cleanup.worker.ts", import.meta.url), {
		type: "module",
	});
	try {
		const result = await new Promise((resolve, reject) => {
			worker.onmessage = (event) => resolve(event.data);
			worker.onerror = reject;
			worker.postMessage({ scenario });
		});
		return result;
	} finally {
		worker.terminate();
	}
}

test("a failed ownership callback releases the physical OPFS locks", async () => {
	expect(await runScenario("callback")).toEqual({ ok: true });
}, 30_000);

test("full OPFS ownership rejects aliases of the same physical filename", async () => {
	expect(await runScenario("aliases")).toEqual({ code: "LIX_STORAGE_FENCED" });
}, 30_000);
