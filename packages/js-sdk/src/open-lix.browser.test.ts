import { expect, test } from "vitest";
import { registerMemoryStorageContract } from "../tests/memory-storage-contract.js";

registerMemoryStorageContract({
	name: "browser WASM",
	loadSdk: async () => await import("@lix-js/sdk"),
	operationTimeoutMs: 30_000,
	supportsPluginExecution: false,
});

registerMemoryStorageContract({
	name: "browser WASM IndexedDB",
	loadSdk: async () => await import("@lix-js/sdk"),
	openStorage: async () => {
		const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
		return openLix({
			storage: new IndexedDbStorage({
				name: `lix-indexed-db-contract:${crypto.randomUUID()}`,
			}),
		});
	},
	operationTimeoutMs: 30_000,
	supportsPluginExecution: false,
});

test("forwards opt-in SQL telemetry from browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	let resolveSpan!: (span: { attributes: Record<string, unknown> }) => void;
	const received = new Promise<{ attributes: Record<string, unknown> }>(
		(resolve) => {
			resolveSpan = resolve;
		},
	);
	const lix = await openLix({
		telemetry: {
			onSpan(span) {
				if (span.name === "lix.sql.query") resolveSpan(span);
			},
		},
	});
	try {
		await lix.execute("SELECT 'private-value' AS value, 42 AS number");
		const span = await received;
		expect(span.attributes["db.query.text"]).toBe(
			"SELECT ? AS value, ? AS number",
		);
	} finally {
		await lix.close();
	}
});

test("loads and executes the engine outside the browser main thread", async () => {
	const wasm = WebAssembly as unknown as Record<
		string,
		(...args: unknown[]) => unknown
	>;
	const methodNames = [
		"compile",
		"compileStreaming",
		"instantiate",
		"instantiateStreaming",
	] as const;
	const originals = new Map<string, (...args: unknown[]) => unknown>();
	let mainThreadCalls = 0;
	for (const name of methodNames) {
		const original = wasm[name];
		if (!original) continue;
		originals.set(name, original);
		wasm[name] = (...args: unknown[]) => {
			mainThreadCalls += 1;
			return original(...args);
		};
	}

	try {
		const { openLix } = await import("@lix-js/sdk");
		const lix = await openLix();
		const result = await lix.execute("SELECT 1 AS value");
		expect(result.rows[0]?.get("value")).toBe(1);
		await lix.close();
		expect(mainThreadCalls).toBe(0);
	} finally {
		for (const [name, original] of originals) wasm[name] = original;
	}
});

test("createCheckpoint returns the new active head through browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["checkpoint-test", "working"],
		);
		const before = (
			await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
		).rows[0]?.get("commit_id");

		const checkpoint = await lix.createCheckpoint();

		expect(checkpoint.commitId).not.toBe(before);
		expect(
			(
				await lix.execute("SELECT lix_active_branch_commit_id() AS commit_id")
			).rows[0]?.get("commit_id"),
		).toBe(checkpoint.commitId);
	} finally {
		await lix.close();
	}
});

test("executes a globally ordered union plan in browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute("INSERT INTO lix_directory (path) VALUES ($1)", [
			"/docs",
		]);
		await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
			"/README.md",
			new Uint8Array(),
		]);

		const result = await lix.execute(`
			SELECT path, 'directory' AS kind FROM lix_directory
			UNION ALL
			SELECT path, 'file' AS kind FROM lix_file
			ORDER BY path ASC
		`);
		const rows = result.rows
			.map((row) => row.toObject() as { path: string; kind: string })
			.filter((row) => !row.path.startsWith("/.lix/"));

		expect(rows).toEqual([
			{ path: "/README.md", kind: "file" },
			{ path: "/docs", kind: "directory" },
		]);
	} finally {
		await lix.close();
	}
});

test("remote client state survives reopen while the server selects the session branch", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const storage = new IndexedDbStorage({
		name: `lix-client-state-test:${crypto.randomUUID()}`,
	});
	const sessions = new Map<string, string>();
	const initialBranchRequests: Array<string | null> = [];
	const initialAccountRequests: Array<string | null> = [];
	const requestBodies: string[] = [];
	let nextSession = 0;
	const remoteFetch = async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		const url = new URL(request.url);
		const suppliedSession = request.headers.get("lix-session-id");
		if (url.pathname.endsWith("/lix/v1/")) {
			const requestedBranch = url.searchParams.get("activeBranchId");
			if (!suppliedSession) {
				initialBranchRequests.push(requestedBranch);
				initialAccountRequests.push(url.searchParams.get("activeAccountId"));
			}
			const sessionId = suppliedSession ?? `session-${++nextSession}`;
			if (!sessions.has(sessionId)) {
				sessions.set(sessionId, requestedBranch ?? "main");
			}
			return Response.json({
				protocolVersion: 2,
				activeBranchId: sessions.get(sessionId),
				activeAccountId: "00000000-0000-7000-8000-000000000002",
				sessionId,
			});
		}
		if (url.pathname.endsWith("/branch/switch")) {
			const body = await request.text();
			requestBodies.push(body);
			const branchId = (JSON.parse(body) as { branchId: string }).branchId;
			if (!suppliedSession) throw new Error("missing test session");
			sessions.set(suppliedSession, branchId);
			return Response.json({ branchId });
		}
		if (url.pathname.endsWith("/lix/v1/session")) {
			if (suppliedSession) sessions.delete(suppliedSession);
			return new Response(null, { status: 204 });
		}
		throw new Error(`Unexpected request: ${url.pathname}`);
	};
	const options = {
		server: {
			mode: "remote" as const,
			url: "https://lixray.test/@acme/client-state",
			fetch: remoteFetch,
		},
		storage,
	};

	const first = await openLix(options);
	await first.clientState.set("atelier", { focusedPanel: "right" });
	await first.switchBranch({ branchId: "draft" });
	await first.close();

	const second = await openLix(options);
	try {
		expect(await second.activeBranchId()).toBe("main");
		await expect(second.clientState.get("atelier")).resolves.toEqual({
			focusedPanel: "right",
		});
		expect(requestBodies).toEqual([JSON.stringify({ branchId: "draft" })]);
		expect(requestBodies.join("\n")).not.toContain("focusedPanel");
	} finally {
		await second.close();
	}
	expect(initialBranchRequests).toEqual([null, null]);
	expect(initialAccountRequests).toEqual([
		null,
		"00000000-0000-7000-8000-000000000002",
	]);
});

test("IndexedDbStorage persists a complete local Lix", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const storage = new IndexedDbStorage({
		name: `lix-indexed-db-test:${crypto.randomUUID()}`,
	});
	const first = await openLix({ storage });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["durable", { value: 42 }],
	);
	await first.close();

	const second = await openLix({ storage });
	try {
		expect(
			(
				await second.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"durable",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 42 });
	} finally {
		await second.close();
	}
});

test("IndexedDbStorage exclusively owns a database name", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const name = `lix-indexed-db-owner-test:${crypto.randomUUID()}`;
	const firstStorage = new IndexedDbStorage({ name });
	const secondStorage = new IndexedDbStorage({ name });
	const first = await openLix({ storage: firstStorage });

	await expect(openLix({ storage: secondStorage })).rejects.toThrow(
		"already open",
	);
	await first.close();

	const second = await openLix({ storage: secondStorage });
	await second.close();
});

test("IndexedDbStorage releases ownership after corrupt data rejects open", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const name = `lix-indexed-db-corrupt-test:${crypto.randomUUID()}`;
	await seedMalformedIndexedDbEntry(name);

	await expect(
		openLix({ storage: new IndexedDbStorage({ name }) }),
	).rejects.toThrow("not binary data");
	await deleteIndexedDb(name);

	const recovered = await openLix({ storage: new IndexedDbStorage({ name }) });
	await recovered.close();
});

test("remote IndexedDbStorage isolates client state by server URL", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const name = `lix-remote-isolation-test:${crypto.randomUUID()}`;
	const remoteFetch = async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		const url = new URL(request.url);
		if (url.pathname.endsWith("/lix/v1/")) {
			return Response.json({
				protocolVersion: 2,
				activeBranchId: "main",
				activeAccountId: "00000000-0000-7000-8000-000000000002",
				sessionId: crypto.randomUUID(),
			});
		}
		if (url.pathname.endsWith("/lix/v1/session")) {
			return new Response(null, { status: 204 });
		}
		throw new Error(`Unexpected request: ${url.pathname}`);
	};
	const openRemote = (url: string) =>
		openLix({
			server: { mode: "remote", url, fetch: remoteFetch },
			storage: new IndexedDbStorage({ name }),
		});

	const workspaceA = await openRemote("https://lixray.test/@acme/a");
	await workspaceA.clientState.set("selected-panel", "history");
	await workspaceA.close();

	const workspaceB = await openRemote("https://lixray.test/@acme/b");
	await expect(
		workspaceB.clientState.get("selected-panel"),
	).resolves.toBeUndefined();
	await workspaceB.clientState.set("selected-panel", "files");
	await workspaceB.close();

	const reopenedA = await openRemote("https://lixray.test/@acme/a/");
	await expect(reopenedA.clientState.get("selected-panel")).resolves.toBe(
		"history",
	);
	await reopenedA.close();
});

test("IndexedDbStorage close can retry after an active transaction", async () => {
	const { IndexedDbStorage, openLix } = await import("@lix-js/sdk");
	const storage = new IndexedDbStorage({
		name: `lix-indexed-db-close-test:${crypto.randomUUID()}`,
	});
	const lix = await openLix({ storage });
	const tx = await lix.beginTransaction();

	await expect(lix.close()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	await tx.rollback();
	await lix.clientState.set("after-failed-close", true);
	await lix.close();

	const reopened = await openLix({ storage });
	await expect(reopened.clientState.get("after-failed-close")).resolves.toBe(
		true,
	);
	await reopened.close();
});

async function seedMalformedIndexedDbEntry(name: string): Promise<void> {
	const request = indexedDB.open(name, 1);
	request.onupgradeneeded = () => request.result.createObjectStore("entries");
	const database = await new Promise<IDBDatabase>((resolve, reject) => {
		request.onsuccess = () => resolve(request.result);
		request.onerror = () => reject(request.error);
	});
	const transaction = database.transaction("entries", "readwrite");
	transaction.objectStore("entries").put("not-binary", new Uint8Array([0, 0, 0, 1]));
	await new Promise<void>((resolve, reject) => {
		transaction.oncomplete = () => resolve();
		transaction.onerror = () => reject(transaction.error);
		transaction.onabort = () => reject(transaction.error);
	});
	database.close();
}

async function deleteIndexedDb(name: string): Promise<void> {
	const request = indexedDB.deleteDatabase(name);
	await new Promise<void>((resolve, reject) => {
		request.onsuccess = () => resolve();
		request.onerror = () => reject(request.error);
		request.onblocked = () => reject(new Error("IndexedDB delete was blocked"));
	});
}
