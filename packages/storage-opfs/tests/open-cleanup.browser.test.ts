import { expect, test } from "vitest";

async function runScenario(scenario: string, name?: string, files?: string[]) {
	const worker = new Worker(new URL("./open-cleanup.worker.ts", import.meta.url), {
		type: "module",
	});
	try {
		const result = await new Promise((resolve, reject) => {
			worker.onmessage = (event) => resolve(event.data);
			worker.onerror = reject;
			worker.postMessage({ scenario, name, files });
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

test("an interrupted SQLite initialization can be retried in the same worker", async () => {
	expect(await runScenario("sqlite-retry")).toEqual({ ok: true });
}, 30_000);

async function storedFiles(name: string) {
	let directory = await navigator.storage.getDirectory();
	for (const part of ["lix", "sqlite-sahpool", btoa(name).replace(/=+$/u, "")]) {
		try { directory = await directory.getDirectoryHandle(part); }
		catch (error) { if ((error as DOMException).name === "NotFoundError") return []; throw error; }
	}
	const files: { path: string; size: number; hash: string }[] = [];
	async function visit(parent: FileSystemDirectoryHandle, prefix: string) {
		for await (const [key, handle] of parent.entries()) {
			if (handle.kind === "directory") await visit(handle as FileSystemDirectoryHandle, prefix + key + "/");
			else {
				const file = await (handle as FileSystemFileHandle).getFile();
				const hash = await crypto.subtle.digest("SHA-256", await file.arrayBuffer());
				files.push({ path: prefix + key, size: file.size, hash: Array.from(new Uint8Array(hash), b => b.toString(16).padStart(2, "0")).join("") });
			}
		}
	}
	await visit(directory, "");
	return files.sort((a, b) => a.path.localeCompare(b.path));
}

test("failed pool opening preserves every existing repository file", async () => {
	const name = `pool-failure-${crypto.randomUUID()}`;
	expect(await runScenario("seed", name)).toEqual({ ok: true });
	const before = await storedFiles(name);
	expect(before.length).toBeGreaterThan(0);
	expect(await runScenario("pool-failure", name, before.map(file => file.path.split("/").at(-1)!))).toEqual({ ok: true });
	expect(await storedFiles(name)).toEqual(before);
	expect(await runScenario("read", name)).toEqual({ value: 42 });
}, 30_000);
