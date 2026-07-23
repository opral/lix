import { expect, test, vi } from "vitest";
import { gunzipSync } from "fflate";
import { openLix } from "../index.js";

async function requestJson(request: Request): Promise<unknown> {
	const bytes = new Uint8Array(await request.arrayBuffer());
	const decoded =
		request.headers.get("content-encoding") === "gzip"
			? gunzipSync(bytes)
			: bytes;
	return JSON.parse(new TextDecoder().decode(decoded));
}

test("remote mode uses the workspace protocol without loading a local engine", async () => {
	const requests: Request[] = [];
	let headerCalls = 0;
	const remoteFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		requests.push(request);
		if (new URL(request.url).pathname.endsWith("/lix/v1/")) {
			return Response.json({
				protocolVersion: 1,
				activeBranchId: "main-id",
				sessionId: "session-1",
			});
		}
		if (new URL(request.url).pathname.endsWith("/lix/v1/execute")) {
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
		if (request.method === "DELETE") {
			return new Response(null, { status: 204 });
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
				return {
					Authorization: `Bearer token-${headerCalls}`,
					"Lix-Session-Id": "caller-must-not-control-this",
				};
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
	expect(headerCalls).toBe(3);
	expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
		"/@acme/workspace/lix/v1/",
		"/@acme/workspace/lix/v1/",
		"/@acme/workspace/lix/v1/execute",
	]);
	expect(requests[2]?.headers.get("authorization")).toBe("Bearer token-3");
	expect(requests[0]?.headers.has("lix-session-id")).toBe(false);
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
	expect(requests[2]?.headers.get("lix-session-id")).toBe("session-1");
	expect(await requests[2]?.json()).toEqual({
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
	expect(new URL(requests[3]?.url ?? "").pathname).toBe(
		"/@acme/workspace/lix/v1/session",
	);
	expect(requests[3]?.method).toBe("DELETE");
	expect(requests[3]?.headers.get("lix-session-id")).toBe("session-1");
});

test("remote mode requests and verifies an immutable branch-pinned session", async () => {
	const requests: Request[] = [];
	const branchId = "draft/agent one";
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			branchId,
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				return Response.json({
					protocolVersion: 1,
					activeBranchId: branchId,
					sessionId: "session-1",
					sessionScope: { kind: "branch", branchId },
				});
			},
		},
	});

	expect(await lix.activeBranchId()).toBe(branchId);
	await lix.close();

	expect(requests).toHaveLength(3);
	expect(new URL(requests[0]?.url ?? "").searchParams.get("branchId")).toBe(
		branchId,
	);
	expect(new URL(requests[1]?.url ?? "").search).toBe("");
	expect(new URL(requests[2]?.url ?? "").search).toBe("");
	expect(requests[0]?.headers.has("lix-session-id")).toBe(false);
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
	expect(requests[2]?.headers.get("lix-session-id")).toBe("session-1");
});

test("remote mode fails closed and releases a session when branch pinning is not honored", async () => {
	const requests: Request[] = [];
	await expect(
		openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				branchId: "draft-id",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					requests.push(request.clone());
					if (request.method === "DELETE") {
						return new Response(null, { status: 204 });
					}
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "draft-id",
						sessionId: "session-1",
					});
				},
			},
		}),
	).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote server did not open the requested branch-pinned session",
	});

	expect(requests.map((request) => request.method)).toEqual(["GET", "DELETE"]);
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
});

test("remote mode releases a branch session whose active branch contradicts its scope", async () => {
	const requests: Request[] = [];
	await expect(
		openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				branchId: "draft-id",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					requests.push(request.clone());
					if (request.method === "DELETE") {
						return new Response(null, { status: 204 });
					}
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
						sessionScope: { kind: "branch", branchId: "draft-id" },
					});
				},
			},
		}),
	).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote server did not open the requested branch-pinned session",
	});

	expect(requests.map((request) => request.method)).toEqual(["GET", "DELETE"]);
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
});

test("remote mode releases a session after malformed scope decoding without masking that error", async () => {
	const requests: Request[] = [];
	await expect(
		openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				branchId: "draft-id",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					requests.push(request.clone());
					if (request.method === "DELETE") {
						return Response.json(
							{
								error: {
									code: "LIX_DELETE_FAILED",
									message: "cleanup failed",
								},
							},
							{ status: 500 },
						);
					}
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "draft-id",
						sessionId: "session-1",
						sessionScope: { kind: "branch" },
					});
				},
			},
		}),
	).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote handshake sessionScope is invalid",
	});

	expect(requests.map((request) => request.method)).toEqual(["GET", "DELETE"]);
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
});

test.each(["", "   ", 42])(
	"remote mode rejects an invalid branchId: %j",
	async (branchId) => {
		await expect(
			openLix({
				server: {
					mode: "remote",
					url: "https://lixray.test/workspace",
					branchId: branchId as string,
				},
			}),
		).rejects.toThrow(
			"openLix() remote server branchId must be a non-empty string",
		);
	},
);

test("remote mode compresses only large compressible JSON requests", async () => {
	const executeRequests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (pathname.endsWith("/lix/v1/execute")) {
					executeRequests.push(request.clone());
					return Response.json({
						columns: [],
						rows: [],
						rowsAffected: 1,
						notices: [],
					});
				}
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				throw new Error(`Unexpected request: ${request.url}`);
			},
		},
	});

	const compressible = new Uint8Array(100 * 1024).fill(0x41);
	await lix.execute("UPDATE lix_file SET data = $1 WHERE id = $2", [
		compressible,
		"file-1",
	]);
	expect(executeRequests[0]?.headers.get("content-encoding")).toBe("gzip");
	const compressedBody = new Uint8Array(
		await executeRequests[0]!.arrayBuffer(),
	);
	const decodedBody = JSON.parse(
		new TextDecoder().decode(gunzipSync(compressedBody)),
	);
	expect(decodedBody.params[0].base64).toBe("QUFB".repeat(34_133) + "QQ==");

	let random = 0x1234_5678;
	const incompressible = Uint8Array.from({ length: 100 * 1024 }, () => {
		random ^= random << 13;
		random ^= random >>> 17;
		random ^= random << 5;
		return random & 0xff;
	});
	await lix.execute("UPDATE lix_file SET data = $1 WHERE id = $2", [
		incompressible,
		"file-1",
	]);
	expect(executeRequests[1]?.headers.has("content-encoding")).toBe(false);
	expect(await executeRequests[1]?.json()).toMatchObject({
		params: [{ kind: "blob" }, { kind: "text", value: "file-1" }],
	});

	await lix.close();
});

test("remote executeBatch uses the first-class atomic batch endpoint", async () => {
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				return new URL(request.url).pathname.endsWith("/lix/v1/")
					? Response.json({
							protocolVersion: 1,
							activeBranchId: "main-id",
							sessionId: "session-1",
						})
					: Response.json([
							{
								columns: ["value"],
								rows: [[{ kind: "int", value: 1 }]],
								rowsAffected: 0,
								notices: [],
							},
							{
								columns: ["value"],
								rows: [[{ kind: "text", value: "two" }]],
								rowsAffected: 0,
								notices: [],
							},
						]);
			},
		},
	});

	const results = await lix.executeBatch(
		[
			{ sql: "SELECT $1 AS value", params: [1] },
			{ sql: "SELECT $1 AS value", params: ["two"] },
		],
		{ originKey: "batch-test" },
	);
	expect(results.map((result) => result.rows[0]?.get("value"))).toEqual([
		1,
		"two",
	]);
	expect(new URL(requests[1]?.url ?? "").pathname).toBe(
		"/@acme/workspace/lix/v1/execute-batch",
	);
	expect(await requests[1]?.json()).toEqual({
		statements: [
			{ sql: "SELECT $1 AS value", params: [{ kind: "int", value: 1 }] },
			{
				sql: "SELECT $1 AS value",
				params: [{ kind: "text", value: "two" }],
			},
		],
		options: { originKey: "batch-test" },
	});

	await lix.close();
});

test("remote execute sends successful large blob updates as exact splices", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshakeResponse();
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				bodies.push((await requestJson(request)) as Record<string, unknown>);
				return Response.json(emptyExecuteResponse());
			},
		},
	});
	const firstBacking = new Uint8Array(32 * 1024 + 3).fill(97);
	const first = firstBacking.subarray(3);
	const secondBacking = new Uint8Array(first.byteLength + 5);
	secondBacking.set(first, 5);
	const second = secondBacking.subarray(5);
	second[16 * 1024 + 3] = 98;
	const prototype = Uint8Array.prototype as Uint8Array & {
		toBase64?: () => string;
	};
	const originalToBase64 = Object.getOwnPropertyDescriptor(
		prototype,
		"toBase64",
	);
	const encodedLengths: number[] = [];
	Object.defineProperty(prototype, "toBase64", {
		configurable: true,
		value: function (this: Uint8Array) {
			encodedLengths.push(this.byteLength);
			return btoa(String.fromCharCode(...this));
		},
	});
	let deltaEncodedLengths: number[];
	try {
		await lix.execute("UPDATE lix_file SET data = $1", [first]);
		encodedLengths.length = 0;
		await lix.execute("UPDATE lix_file SET data = $1", [second]);
		deltaEncodedLengths = [...encodedLengths];
	} finally {
		if (originalToBase64 === undefined) delete prototype.toBase64;
		else Object.defineProperty(prototype, "toBase64", originalToBase64);
	}
	await lix.close();

	expect(deltaEncodedLengths).toEqual([1]);
	expect(bodies[0]).toMatchObject({
		cacheBlobs: true,
		params: [{ kind: "blob" }],
	});
	const delta = (bodies[1]?.params as Array<Record<string, unknown>>)[0];
	expect(bodies[1]?.cacheBlobs).toBe(true);
	expect(delta).toMatchObject({
		kind: "blob-splice",
		prefixBytes: 16 * 1024 + 3,
		suffixBytes: 16 * 1024 - 4,
		insertBase64: "Yg==",
	});
	expect(delta?.baseSha256).toMatch(/^[0-9a-f]{64}$/);
	expect(delta?.resultSha256).toMatch(/^[0-9a-f]{64}$/);
	expect(delta?.baseSha256).not.toBe(delta?.resultSha256);
	expect(JSON.stringify(bodies[1]).length).toBeLessThan(
		JSON.stringify(bodies[0]).length * 0.1,
	);
});

test("remote execute retries a missing blob base once with full bytes", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	let rejectedDelta = false;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshakeResponse();
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				const body = (await requestJson(request)) as Record<string, unknown>;
				bodies.push(body);
				const param = (body.params as Array<Record<string, unknown>>)[0];
				if (param?.kind === "blob-splice" && !rejectedDelta) {
					rejectedDelta = true;
					return Response.json(
						{
							error: {
								code: "LIX_REMOTE_BLOB_BASE_MISSING",
								message: "Blob base was evicted",
							},
						},
						{ status: 409 },
					);
				}
				return Response.json(emptyExecuteResponse());
			},
		},
	});
	const first = new Uint8Array(32 * 1024).fill(97);
	const second = new Uint8Array(first);
	second[0] = 98;

	await lix.execute("UPDATE lix_file SET data = $1", [first]);
	await lix.execute("UPDATE lix_file SET data = $1", [second]);
	await lix.close();

	expect(bodies).toHaveLength(3);
	expect((bodies[1]?.params as Array<Record<string, unknown>>)[0]?.kind).toBe(
		"blob-splice",
	);
	expect((bodies[2]?.params as Array<Record<string, unknown>>)[0]?.kind).toBe(
		"blob",
	);
	expect(bodies[2]?.cacheBlobs).toBe(true);
});

test("remote execute keeps small and low-saving blob updates full", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshakeResponse();
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				bodies.push((await requestJson(request)) as Record<string, unknown>);
				return Response.json(emptyExecuteResponse());
			},
		},
	});
	const first = new Uint8Array(32 * 1024).fill(97);
	const unrelated = new Uint8Array(32 * 1024).fill(98);

	await lix.execute("UPDATE lix_file SET data = $1", [first]);
	await lix.execute("UPDATE lix_file SET data = $1", [unrelated]);
	await lix.execute("UPDATE lix_file SET data = $1", [new Uint8Array(1024)]);
	await lix.close();

	expect((bodies[1]?.params as Array<Record<string, unknown>>)[0]?.kind).toBe(
		"blob",
	);
	expect(bodies[1]?.cacheBlobs).toBe(true);
	expect((bodies[2]?.params as Array<Record<string, unknown>>)[0]?.kind).toBe(
		"blob",
	);
	expect(bodies[2]).not.toHaveProperty("cacheBlobs");
});

test("remote execute keeps large writes full without the splice capability", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				bodies.push((await requestJson(request)) as Record<string, unknown>);
				return Response.json(emptyExecuteResponse());
			},
		},
	});
	const first = new Uint8Array(32 * 1024).fill(97);
	const second = new Uint8Array(first);
	second[0] = 98;

	await lix.execute("UPDATE lix_file SET data = $1", [first]);
	await lix.execute("UPDATE lix_file SET data = $1", [second]);
	await lix.close();

	expect(bodies).toHaveLength(2);
	for (const body of bodies) {
		expect((body.params as Array<Record<string, unknown>>)[0]?.kind).toBe(
			"blob",
		);
		expect(body).not.toHaveProperty("cacheBlobs");
	}
});

test("remote executeBatch uses blob splices for stable statement slots", async () => {
	const bodies: Array<Record<string, unknown>> = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshakeResponse();
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				bodies.push((await requestJson(request)) as Record<string, unknown>);
				return Response.json([emptyExecuteResponse()]);
			},
		},
	});
	const first = new Uint8Array(32 * 1024).fill(97);
	const second = new Uint8Array(first);
	second[second.byteLength - 1] = 98;
	const statement = (blob: Uint8Array) => [
		{ sql: "UPDATE lix_file SET data = $1", params: [blob] },
	];

	await lix.executeBatch(statement(first));
	await lix.executeBatch(statement(second));
	await lix.close();

	const statements = bodies[1]?.statements as Array<{
		params: Array<Record<string, unknown>>;
	}>;
	expect(bodies[1]?.cacheBlobs).toBe(true);
	expect(statements[0]?.params[0]).toMatchObject({
		kind: "blob-splice",
		prefixBytes: 32 * 1024 - 1,
		suffixBytes: 0,
		insertBase64: "Yg==",
	});
});

test("remote branches preserve local Lix branch semantics", async () => {
	const requests: Request[] = [];
	let activeBranchId = "main-id";
	const lix = await openLix({
		server: {
			mode: "remote",
			url: new URL("https://lixray.test/@acme/workspace/"),
			fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId,
						sessionId: "session-1",
					});
				}
				if (pathname.endsWith("/branch/switch")) {
					const body = (await request.json()) as { branchId: string };
					activeBranchId = body.branchId;
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
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				throw new Error(`Unexpected request: ${pathname}`);
			}) as typeof fetch,
		},
	});

	expect(await lix.activeBranchId()).toBe("main-id");
	const created = await lix.createBranch({ name: "Draft" });
	expect(created.id).toBe("draft-id");
	expect(await lix.activeBranchId()).toBe("main-id");
	await lix.switchBranch({ branchId: created.id });
	expect(await lix.activeBranchId()).toBe("draft-id");
	const posted = requests.filter((request) => request.method === "POST");
	expect(await Promise.all(posted.map((request) => request.clone().json()))).toEqual([
		{ name: "Draft" },
		{ branchId: "draft-id" },
	]);

	await lix.close();
});

test("a failed remote branch switch leaves the active branch unchanged", async () => {
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				return pathname.endsWith("/lix/v1/")
					? Response.json({
							protocolVersion: 1,
							activeBranchId: "main-id",
							sessionId: "session-1",
						})
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

test("remote clients share the same workspace active branch", async () => {
	let activeBranchId = "main-id";
	let nextSessionId = 0;
	const remoteFetch = async (input: RequestInfo | URL, init?: RequestInit) => {
		const request = new Request(input, init);
		const pathname = new URL(request.url).pathname;
		if (pathname.endsWith("/lix/v1/")) {
			return Response.json({
				protocolVersion: 1,
				activeBranchId,
				sessionId:
					request.headers.get("lix-session-id") ??
					`session-${++nextSessionId}`,
			});
		}
		if (pathname.endsWith("/branch/switch")) {
			activeBranchId = ((await request.json()) as { branchId: string }).branchId;
			return Response.json({ branchId: activeBranchId });
		}
		if (request.method === "DELETE") {
			return new Response(null, { status: 204 });
		}
		throw new Error(`Unexpected request: ${pathname}`);
	};
	const options = {
		server: {
			mode: "remote" as const,
			url: "https://lixray.test/@acme/workspace",
			fetch: remoteFetch,
		},
	};
	const first = await openLix(options);
	const second = await openLix(options);

	await first.switchBranch({ branchId: "draft-id" });
	expect(await first.activeBranchId()).toBe("draft-id");
	expect(await second.activeBranchId()).toBe("draft-id");

	await Promise.all([first.close(), second.close()]);
});

test("remote operations preserve normal Lix call ordering", async () => {
	let finishSwitch: (() => void) | undefined;
	const switchGate = new Promise<void>((resolve) => {
		finishSwitch = resolve;
	});
	const requestOrder: string[] = [];
	let executeBody: unknown;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (pathname.endsWith("/branch/switch")) {
					requestOrder.push("switch");
					await switchGate;
					return Response.json({ branchId: "draft-id" });
				}
				if (pathname.endsWith("/execute")) {
					requestOrder.push("execute");
					executeBody = await request.json();
					return Response.json({
						columns: [],
						rows: [],
						rowsAffected: 0,
						notices: [],
					});
				}
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
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
	expect(executeBody).toEqual({ sql: "SELECT 1", params: [] });

	await lix.close();
});

test("remote responses reject malformed rows and non-JSON HTTP errors", async () => {
	let executeCalls = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
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
					sessionId: "session-1",
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

test.each([
	undefined,
	"",
	" contains-space",
	"contains\nnewline",
	42,
])("remote mode rejects an invalid handshake sessionId: %j", async (sessionId) => {
	await expect(
		openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				fetch: (async () =>
					Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId,
					})) as typeof fetch,
			},
		}),
	).rejects.toMatchObject({ code: "LIX_REMOTE_PROTOCOL_ERROR" });
});

test("a later handshake cannot silently replace the remote session", async () => {
	let handshakeCalls = 0;
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				handshakeCalls += 1;
				return Response.json({
					protocolVersion: 1,
					activeBranchId: "main-id",
					sessionId: handshakeCalls === 1 ? "session-1" : "session-2",
				});
			},
		},
	});

	await expect(lix.activeBranchId()).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote handshake changed sessionId",
	});
	expect(requests.map((request) => request.method)).toEqual([
		"GET",
		"GET",
		"DELETE",
	]);
	expect(requests[2]?.headers.get("lix-session-id")).toBe("session-1");
	await expect(lix.execute("SELECT 1")).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	await lix.close();
});

test("a queued handshake does not run after an earlier handshake fails closed", async () => {
	let handshakeCalls = 0;
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				handshakeCalls += 1;
				return Response.json({
					protocolVersion: 1,
					activeBranchId: "main-id",
					sessionId: handshakeCalls === 1 ? "session-1" : "session-2",
				});
			},
		},
	});

	const failing = lix.activeBranchId();
	const queued = lix.activeBranchId();
	await expect(failing).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote handshake changed sessionId",
	});
	await expect(queued).rejects.toMatchObject({ code: "LIX_ERROR_CLOSED" });
	expect(requests.map((request) => request.method)).toEqual([
		"GET",
		"GET",
		"DELETE",
	]);
	await lix.close();
});

test("concurrent close shares fail-closed handshake cleanup", async () => {
	let handshakeCalls = 0;
	const resumedHandshakeStarted = deferred<void>();
	const resumedHandshake = deferred<Response>();
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				handshakeCalls += 1;
				if (handshakeCalls === 1) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				resumedHandshakeStarted.resolve();
				return await resumedHandshake.promise;
			},
		},
	});

	const refreshing = lix.activeBranchId();
	await resumedHandshakeStarted.promise;
	const closing = lix.close();
	resumedHandshake.resolve(
		Response.json({
			protocolVersion: 1,
			activeBranchId: "main-id",
			sessionId: "session-2",
		}),
	);

	await expect(refreshing).rejects.toMatchObject({
		code: "LIX_REMOTE_PROTOCOL_ERROR",
		message: "remote handshake changed sessionId",
	});
	await expect(closing).resolves.toBeUndefined();
	expect(requests.map((request) => request.method)).toEqual([
		"GET",
		"GET",
		"DELETE",
	]);
	expect(requests[2]?.headers.get("lix-session-id")).toBe("session-1");
});

test.each([
	[
		"scope kind",
		{
			activeBranchId: "draft-id",
			sessionScope: { kind: "workspace" },
		},
	],
	[
		"scope branchId",
		{
			activeBranchId: "draft-id",
			sessionScope: { kind: "branch", branchId: "other-id" },
		},
	],
	[
		"activeBranchId",
		{
			activeBranchId: "other-id",
			sessionScope: { kind: "branch", branchId: "draft-id" },
		},
	],
])(
	"a resumed branch-pinned handshake fails closed when %s changes",
	async (_field, resumedHandshake) => {
		let handshakeCalls = 0;
		const requests: Request[] = [];
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/workspace",
				branchId: "draft-id",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					requests.push(request.clone());
					if (request.method === "DELETE") {
						return new Response(null, { status: 204 });
					}
					handshakeCalls += 1;
					return Response.json({
						protocolVersion: 1,
						sessionId: "session-1",
						...(handshakeCalls === 1
							? {
								activeBranchId: "draft-id",
								sessionScope: {
									kind: "branch",
									branchId: "draft-id",
								},
							}
							: resumedHandshake),
					});
				},
			},
		});

		await expect(lix.activeBranchId()).rejects.toMatchObject({
			code: "LIX_REMOTE_PROTOCOL_ERROR",
			message:
				"remote server did not preserve the requested branch-pinned session",
		});
		expect(requests.map((request) => request.method)).toEqual([
			"GET",
			"GET",
			"DELETE",
		]);
		expect(requests[2]?.headers.get("lix-session-id")).toBe("session-1");
		await expect(lix.execute("SELECT 1")).rejects.toMatchObject({
			code: "LIX_ERROR_CLOSED",
		});
		await lix.close();
	},
);

test("an expired session mutation is propagated without a new handshake or retry", async () => {
	let handshakeCalls = 0;
	let executeCalls = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					handshakeCalls += 1;
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (request.method === "DELETE") {
					return new Response(null, { status: 204 });
				}
				executeCalls += 1;
				return Response.json(
					{
						error: {
							code: "LIX_REMOTE_SESSION_EXPIRED",
							message: "Remote session expired",
						},
					},
					{ status: 410 },
				);
			},
		},
	});

	await expect(lix.execute("UPDATE lix_file SET data = $1")).rejects.toMatchObject({
		code: "LIX_REMOTE_SESSION_EXPIRED",
		status: 410,
	});
	expect(handshakeCalls).toBe(1);
	expect(executeCalls).toBe(1);
	await lix.close();
});

test("close waits for queued operations before deleting the remote session", async () => {
	const executeStarted = deferred<void>();
	const releaseExecute = deferred<void>();
	const order: string[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) {
					return Response.json({
						protocolVersion: 1,
						activeBranchId: "main-id",
						sessionId: "session-1",
					});
				}
				if (request.method === "DELETE") {
					order.push("delete");
					expect(request.headers.get("lix-session-id")).toBe("session-1");
					return new Response(null, { status: 204 });
				}
				order.push("execute-start");
				executeStarted.resolve();
				await releaseExecute.promise;
				order.push("execute-finish");
				return Response.json({
					columns: [],
					rows: [],
					rowsAffected: 1,
					notices: [],
				});
			},
		},
	});

	const executing = lix.execute("UPDATE lix_file SET data = $1");
	await executeStarted.promise;
	const closing = lix.close();
	await Promise.resolve();
	expect(order).toEqual(["execute-start"]);
	releaseExecute.resolve();
	await Promise.all([executing, closing]);
	expect(order).toEqual(["execute-start", "execute-finish", "delete"]);
});

function handshakeResponse() {
	return Response.json({
		protocolVersion: 1,
		activeBranchId: "main-id",
		sessionId: "session-1",
		capabilities: { requestBlobSplice: true },
	});
}

function emptyExecuteResponse() {
	return {
		columns: [],
		rows: [],
		rowsAffected: 1,
		notices: [],
	};
}

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	const promise = new Promise<T>((resolvePromise) => {
		resolve = resolvePromise;
	});
	return { promise, resolve };
}
