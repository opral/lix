import { expect, test } from "vitest";

test("a failed ownership callback releases the physical OPFS locks", async () => {
	const worker = new Worker(new URL("./open-cleanup.worker.ts", import.meta.url), {
		type: "module",
	});
	try {
		const result = await new Promise((resolve, reject) => {
			worker.onmessage = (event) => resolve(event.data);
			worker.onerror = reject;
			worker.postMessage({});
		});
		expect(result).toEqual({ ok: true });
	} finally {
		worker.terminate();
	}
}, 30_000);
