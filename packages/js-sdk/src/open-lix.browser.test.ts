import { expect, test } from "vitest";
import { registerMemoryStorageContract } from "../tests/memory-storage-contract.js";
import type { LixSnapshotStorage } from "./types.js";

registerMemoryStorageContract({
	name: "browser WASM",
	loadSdk: async () => await import("@lix-js/sdk"),
	operationTimeoutMs: 30_000,
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

test("executes a globally ordered union plan in browser WASM", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		await lix.execute("INSERT INTO lix_directory (path) VALUES ($1)", [
			"/docs/",
		]);
		await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
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
			{ path: "/docs/", kind: "directory" },
		]);
	} finally {
		await lix.close();
	}
});

test("remote client state and active branch survive reopen without reaching the server", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { LocalStorage } = await import("@lix-js/sdk/local-storage-adapter");
	const prefix = `lix-client-state-test:${crypto.randomUUID()}`;
	const sessions = new Map<string, string>();
	const availableBranches = new Set(["main", "draft"]);
	const initialBranchRequests: Array<string | null> = [];
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
				if (requestedBranch && !availableBranches.has(requestedBranch)) {
					return Response.json(
						{
							error: {
								code: "LIX_BRANCH_NOT_FOUND",
								message: "Branch not found",
								details: { branchId: requestedBranch },
							},
						},
						{ status: 404 },
					);
				}
			}
			const sessionId = suppliedSession ?? `session-${++nextSession}`;
			if (!sessions.has(sessionId)) {
				sessions.set(sessionId, requestedBranch ?? "main");
			}
			return Response.json({
				protocolVersion: 1,
				activeBranchId: sessions.get(sessionId),
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
		storage: new LocalStorage({ prefix }),
	};

	const first = await openLix(options);
	await first.clientState.set("atelier", { focusedPanel: "right" });
	await first.switchBranch({ branchId: "draft" });
	await first.close();

	const second = await openLix(options);
	try {
		expect(await second.activeBranchId()).toBe("draft");
		expect(second.clientState.get("atelier")).toEqual({
			focusedPanel: "right",
		});
		expect(requestBodies).toEqual([JSON.stringify({ branchId: "draft" })]);
		expect(requestBodies.join("\n")).not.toContain("focusedPanel");
	} finally {
		await second.close();
	}

	availableBranches.delete("draft");
	const fallback = await openLix(options);
	expect(await fallback.activeBranchId()).toBe("main");
	await fallback.close();

	const reopenedFallback = await openLix(options);
	try {
		expect(await reopenedFallback.activeBranchId()).toBe("main");
		expect(initialBranchRequests).toEqual([
			null,
			"draft",
			"draft",
			null,
			"main",
		]);
	} finally {
		await reopenedFallback.close();
	}
});

test("a failed client snapshot save does not reject a completed remote branch switch", async () => {
	const { openLix } = await import("@lix-js/sdk");
	let snapshot: Uint8Array | undefined;
	let failSaves = false;
	const storage: LixSnapshotStorage = {
		load: async () => snapshot?.slice(),
		save: async (_namespace, nextSnapshot) => {
			if (failSaves) throw new Error("storage.save failed");
			snapshot = nextSnapshot.slice();
		},
	};
	let activeBranchId = "main";
	const remoteFetch = async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		const pathname = new URL(request.url).pathname;
		if (pathname.endsWith("/lix/v1/")) {
			return Response.json({
				protocolVersion: 1,
				activeBranchId,
				sessionId: "save-failure-session",
			});
		}
		if (pathname.endsWith("/branch/switch")) {
			activeBranchId = (
				(await request.json()) as { branchId: string }
			).branchId;
			return Response.json({ branchId: activeBranchId });
		}
		if (pathname.endsWith("/lix/v1/session")) {
			return new Response(null, { status: 204 });
		}
		throw new Error(`Unexpected request: ${pathname}`);
	};
	const options = {
		server: {
			mode: "remote" as const,
			url: "https://lixray.test/@acme/save-failure",
			fetch: remoteFetch,
		},
		storage,
	};
	const lix = await openLix(options);
	let branchNotifications = 0;
	lix.subscribeActiveBranch(() => {
		branchNotifications += 1;
	});

	failSaves = true;
	await expect(lix.clientState.set("direct", true)).rejects.toThrow(
		"storage.save failed",
	);
	expect(lix.clientState.get("direct")).toBe(true);
	await expect(lix.switchBranch({ branchId: "draft" })).resolves.toEqual({
		branchId: "draft",
	});
	expect(await lix.activeBranchId()).toBe("draft");
	expect(branchNotifications).toBe(1);

	failSaves = false;
	await lix.close();

	const reopened = await openLix(options);
	try {
		expect(reopened.clientState.get("direct")).toBe(true);
	} finally {
		await reopened.close();
	}
});

test("LocalStorage can persist a complete local Lix", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { LocalStorage } = await import("@lix-js/sdk/local-storage-adapter");
	const storage = new LocalStorage({
		prefix: `lix-local-storage-test:${crypto.randomUUID()}`,
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

test("snapshot-backed close can retry after an active transaction", async () => {
	const { openLix } = await import("@lix-js/sdk");
	let snapshot: Uint8Array | undefined;
	const storage: LixSnapshotStorage = {
		load: async () => snapshot?.slice(),
		save: async (_namespace, nextSnapshot) => {
			snapshot = nextSnapshot.slice();
		},
	};
	const lix = await openLix({ storage });
	const tx = await lix.beginTransaction();

	await expect(lix.close()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	await tx.rollback();
	await lix.clientState.set("after-failed-close", true);
	await lix.close();

	const reopened = await openLix({ storage });
	expect(reopened.clientState.get("after-failed-close")).toBe(true);
	await reopened.close();
});

test("a committed transaction releases its lifecycle when snapshot saving fails", async () => {
	const { openLix } = await import("@lix-js/sdk");
	let snapshot: Uint8Array | undefined;
	let failSaves = false;
	const storage: LixSnapshotStorage = {
		load: async () => snapshot?.slice(),
		save: async (_namespace, nextSnapshot) => {
			if (failSaves) throw new Error("transaction snapshot save failed");
			snapshot = nextSnapshot.slice();
		},
	};
	const lix = await openLix({ storage });
	const tx = await lix.beginTransaction();
	await tx.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["committed-before-save", true],
	);

	failSaves = true;
	await expect(tx.commit()).rejects.toMatchObject({
		code: "LIX_SNAPSHOT_PERSISTENCE_FAILED",
	});
	await expect(tx.rollback()).rejects.toThrow(/closed/);

	failSaves = false;
	await lix.close();
	const reopened = await openLix({ storage });
	try {
		const result = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["committed-before-save"],
		);
		expect(result.rows[0]?.get("value")).toBe(true);
	} finally {
		await reopened.close();
	}
});
