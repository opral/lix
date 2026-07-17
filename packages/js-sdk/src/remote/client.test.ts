import { expect, test, vi } from "vitest";
import { openLix } from "../index.js";

test("remote mode uses the workspace protocol without loading a local engine", async () => {
	const requests: Request[] = [];
	let headerCalls = 0;
	const remoteFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		requests.push(request);
		if (new URL(request.url).pathname.endsWith("/.lix/v1/")) {
			return Response.json({ protocolVersion: 1, activeBranchId: "main-id" });
		}
		if (new URL(request.url).pathname.endsWith("/.lix/v1/execute")) {
			return Response.json({
				columns: ["n", "bytes", "json"],
				rows: [
					[
						{ kind: "int", value: 42 },
						{ kind: "blob", base64: "AQID" },
						{ kind: "json", value: { ok: true } },
					],
				],
				rowsAffected: 0,
				notices: [],
			});
		}
		throw new Error(`Unexpected request: ${request.url}`);
	});
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: remoteFetch as typeof fetch,
			headers: () => {
				headerCalls += 1;
				return { Authorization: `Bearer token-${headerCalls}` };
			},
		},
	});

	expect(await lix.activeBranchId()).toBe("main-id");
	const result = await lix.execute(
		"SELECT $1, $2, $3, $4, $5, $6, $7",
		[
			null,
			true,
			7,
			1.5,
			"text",
			{ nested: [1, false] },
			new Uint8Array([1, 2, 3]),
		],
		{ originKey: "remote-test" },
	);

	expect(result.rows[0]?.get("n")).toBe(42);
	expect(result.rows[0]?.value("bytes").asBytes()).toEqual(
		new Uint8Array([1, 2, 3]),
	);
	expect(result.rows[0]?.get("json")).toEqual({ ok: true });
	expect(headerCalls).toBe(2);
	expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
		"/@acme/workspace/.lix/v1/",
		"/@acme/workspace/.lix/v1/execute",
	]);
	expect(requests[1]?.headers.get("authorization")).toBe("Bearer token-2");
	expect(await requests[1]?.json()).toEqual({
		branchId: "main-id",
		sql: "SELECT $1, $2, $3, $4, $5, $6, $7",
		params: [
			{ kind: "null", value: null },
			{ kind: "bool", value: true },
			{ kind: "int", value: 7 },
			{ kind: "float", value: 1.5 },
			{ kind: "text", value: "text" },
			{ kind: "json", value: { nested: [1, false] } },
			{ kind: "blob", base64: "AQID" },
		],
		options: { originKey: "remote-test" },
	});

	await lix.close();
});

test("remote branches preserve local Lix branch semantics", async () => {
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: new URL("https://lixray.test/@acme/workspace/"),
			branchId: "requested-id",
			fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/.lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
					});
				}
				if (pathname.endsWith("/branch/switch")) {
					const body = (await request.json()) as { branchId: string };
					return Response.json({ branchId: body.branchId });
				}
				if (pathname.endsWith("/branch/create")) {
					return Response.json({
						id: "draft-id",
						name: "Draft",
						hidden: false,
						commitId: "commit-id",
					});
				}
				throw new Error(`Unexpected request: ${pathname}`);
			}) as typeof fetch,
		},
	});

	expect(await lix.activeBranchId()).toBe("requested-id");
	const created = await lix.createBranch({ name: "Draft" });
	expect(created.id).toBe("draft-id");
	expect(await lix.activeBranchId()).toBe("requested-id");
	await lix.switchBranch({ branchId: created.id });
	expect(await lix.activeBranchId()).toBe("draft-id");
	expect(
		await Promise.all(requests.slice(1).map((request) => request.clone().json())),
	).toEqual([
		{ branchId: "requested-id" },
		{ branchId: "requested-id", name: "Draft" },
		{ branchId: "draft-id" },
	]);

	await lix.close();
});

test("a failed remote branch switch leaves the active branch unchanged", async () => {
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: (async (input: RequestInfo | URL) => {
				const pathname = new URL(input.toString()).pathname;
				return pathname.endsWith("/.lix/v1/")
					? Response.json({ protocolVersion: 1, activeBranchId: "main-id" })
					: Response.json(
							{
								error: {
									code: "LIX_BRANCH_NOT_FOUND",
									message: "Branch not found",
									hint: "Choose an existing branch",
									details: { branchId: "missing" },
								},
							},
							{ status: 404 },
						);
			}) as typeof fetch,
		},
	});

	await expect(lix.switchBranch({ branchId: "missing" })).rejects.toMatchObject({
		name: "LixError",
		code: "LIX_BRANCH_NOT_FOUND",
		message: "Branch not found",
		hint: "Choose an existing branch",
		details: { branchId: "missing" },
		status: 404,
	});
	expect(await lix.activeBranchId()).toBe("main-id");

	await lix.close();
});

test("remote operations preserve normal Lix call ordering", async () => {
	let finishSwitch: (() => void) | undefined;
	const switchGate = new Promise<void>((resolve) => {
		finishSwitch = resolve;
	});
	const requestOrder: string[] = [];
	let executeBranchId = "";
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/.lix/v1/")) {
					return Response.json({ protocolVersion: 1, activeBranchId: "main-id" });
				}
				if (pathname.endsWith("/branch/switch")) {
					requestOrder.push("switch");
					await switchGate;
					return Response.json({ branchId: "draft-id" });
				}
				if (pathname.endsWith("/execute")) {
					requestOrder.push("execute");
					executeBranchId = ((await request.json()) as { branchId: string })
						.branchId;
					return Response.json({
						columns: [],
						rows: [],
						rowsAffected: 0,
						notices: [],
					});
				}
				throw new Error(`Unexpected request: ${pathname}`);
			},
		},
	});

	const switching = lix.switchBranch({ branchId: "draft-id" });
	const executing = lix.execute("SELECT 1");
	await Promise.resolve();
	await Promise.resolve();
	expect(requestOrder).toEqual(["switch"]);
	finishSwitch?.();
	await Promise.all([switching, executing]);
	expect(requestOrder).toEqual(["switch", "execute"]);
	expect(executeBranchId).toBe("draft-id");

	await lix.close();
});

test("remote responses reject malformed rows and non-JSON HTTP errors", async () => {
	let executeCalls = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input) => {
				const pathname = new URL(input.toString()).pathname;
				if (pathname.endsWith("/.lix/v1/")) {
					return Response.json({ protocolVersion: 1, activeBranchId: "main-id" });
				}
				executeCalls += 1;
				return executeCalls === 1
					? Response.json({
							columns: ["a", "b"],
							rows: [[{ kind: "int", value: 1 }]],
							rowsAffected: 0,
							notices: [],
						})
					: new Response("upstream unavailable", { status: 502 });
			},
		},
	});

	await expect(lix.execute("SELECT malformed")).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
	});
	await expect(lix.execute("SELECT unavailable")).rejects.toMatchObject({
		code: "LIX_REMOTE_REQUEST_FAILED",
		status: 502,
		details: { body: "upstream unavailable" },
	});

	await lix.close();
});

test("remote mode rejects unsupported local-only operations honestly", async () => {
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: (async () =>
				Response.json({
					protocolVersion: 1,
					activeBranchId: "main-id",
				})) as typeof fetch,
		},
	});

	await expect(lix.beginTransaction()).rejects.toMatchObject({
		code: "LIX_UNSUPPORTED_REMOTE_OPERATION",
	});
	await expect(
		lix.mergeBranch({ sourceBranchId: "source" }),
	).rejects.toMatchObject({ code: "LIX_UNSUPPORTED_REMOTE_OPERATION" });
	await lix.close();
	await expect(lix.execute("SELECT 1")).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
});

test("remote mode rejects incompatible protocol versions", async () => {
	await expect(
		openLix({
			storage: undefined,
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				fetch: (async () =>
					Response.json({
						protocolVersion: 2,
						activeBranchId: "main-id",
					})) as typeof fetch,
			},
		}),
	).rejects.toMatchObject({ code: "LIX_REMOTE_PROTOCOL_ERROR" });
});
