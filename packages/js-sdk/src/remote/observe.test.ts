import { expect, test, vi } from "vitest";
import { openLix } from "../index.js";

test("remote observe streams native Lix results", async () => {
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				return new URL(request.url).pathname.endsWith("/.lix/v1/")
					? handshake()
					: sseResponse(
							sseFrame("next", observePayload("hello", 0, 7)),
						);
			},
		},
	});

	const events = lix.observe("SELECT $1 AS value", ["hello"]);
	const initial = await events.next();
	expect(initial?.sequence).toBe(0);
	expect(initial?.mutationSequence).toBe(7);
	expect(initial?.result.rows[0]?.get("value")).toBe("hello");
	expect(requests[1]?.headers.get("accept")).toBe("text/event-stream");
	expect(await requests[1]?.json()).toEqual({
		branchId: "main-id",
		sql: "SELECT $1 AS value",
		params: [{ kind: "text", value: "hello" }],
	});

	events.close();
	expect(await events.next()).toBeUndefined();
	await lix.close();
});

test("remote observe can continue after a semantic SSE error", async () => {
	vi.useFakeTimers();
	try {
		let observeRequests = 0;
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/@acme/workspace",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/.lix/v1/")) {
						return handshake();
					}
					observeRequests += 1;
					return observeRequests === 1
						? sseResponse(
								sseFrame("error", {
									retryable: true,
									error: {
										code: "LIX_OBSERVE_RUNTIME",
										message: "temporary observation failure",
										hint: "Retry the observation",
										details: { transient: true },
									},
								}),
							)
						: heldSseResponse(
								sseFrame("next", observePayload("recovered", 0, 2)),
								request.signal,
							);
				},
			},
		});

		const events = lix.observe("SELECT value");
		await expect(events.next()).rejects.toMatchObject({
			name: "LixError",
			code: "LIX_OBSERVE_RUNTIME",
			message: "temporary observation failure",
			hint: "Retry the observation",
			details: { transient: true },
		});
		const recovered = events.next();
		await vi.advanceTimersByTimeAsync(100);
		expect((await recovered)?.result.rows[0]?.get("value")).toBe("recovered");
		expect(observeRequests).toBe(2);

		events.close();
		await lix.close();
	} finally {
		vi.useRealTimers();
	}
});

test("remote observe treats unmarked semantic errors as terminal", async () => {
	let observeRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input) => {
				if (new URL(input.toString()).pathname.endsWith("/.lix/v1/")) {
					return handshake();
				}
				observeRequests += 1;
				return sseResponse(
					sseFrame("error", {
						error: {
							code: "LIX_INVALID_SQL",
							message: "invalid observed query",
						},
					}),
				);
			},
		},
	});

	const events = lix.observe("INVALID");
	await expect(events.next()).rejects.toMatchObject({
		code: "LIX_INVALID_SQL",
	});
	await expect(events.next()).rejects.toMatchObject({
		code: "LIX_INVALID_SQL",
	});
	expect(observeRequests).toBe(1);

	events.close();
	await lix.close();
});

test("a successful branch switch restarts existing remote observations", async () => {
	const observedBranches: string[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/.lix/v1/")) return handshake();
				if (pathname.endsWith("/branch/switch")) {
					const body = (await request.json()) as { branchId: string };
					return body.branchId === "missing-id"
						? Response.json(
								{
									error: {
										code: "LIX_BRANCH_NOT_FOUND",
										message: "Branch not found",
									},
								},
								{ status: 404 },
							)
						: Response.json({ branchId: body.branchId });
				}
				const body = (await request.clone().json()) as { branchId: string };
				observedBranches.push(body.branchId);
				return heldSseResponse(
					sseFrame("next", observePayload(body.branchId, 0, 0)),
					request.signal,
				);
			},
		},
	});

	const events = lix.observe("SELECT active_branch");
	expect((await events.next())?.result.rows[0]?.get("value")).toBe("main-id");
	const afterSwitch = events.next();
	await lix.switchBranch({ branchId: "draft-id" });
	const switched = await afterSwitch;
	expect(switched?.result.rows[0]?.get("value")).toBe("draft-id");
	expect(switched?.sequence).toBe(1);
	expect(observedBranches).toEqual(["main-id", "draft-id"]);
	await expect(
		lix.switchBranch({ branchId: "missing-id" }),
	).rejects.toMatchObject({ code: "LIX_BRANCH_NOT_FOUND" });
	expect(observedBranches).toEqual(["main-id", "draft-id"]);

	events.close();
	await lix.close();
});

test("a stale pre-switch HTTP error cannot fail the restarted observation", async () => {
	const firstObserveOpened = deferred<void>();
	let staleBody: ReadableStreamDefaultController<Uint8Array> | undefined;
	let observeRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/.lix/v1/")) return handshake();
				if (pathname.endsWith("/branch/switch")) {
					return Response.json({ branchId: "draft-id" });
				}
				observeRequests += 1;
				if (observeRequests === 1) {
					firstObserveOpened.resolve();
					return new Response(
						new ReadableStream<Uint8Array>({
							start(controller) {
								staleBody = controller;
							},
						}),
						{
							status: 400,
							headers: { "content-type": "application/json" },
						},
					);
				}
				return heldSseResponse(
					sseFrame("next", observePayload("draft", 0, 1)),
					request.signal,
				);
			},
		},
	});

	const events = lix.observe("SELECT value");
	const first = events.next();
	await firstObserveOpened.promise;
	await lix.switchBranch({ branchId: "draft-id" });
	expect((await first)?.result.rows[0]?.get("value")).toBe("draft");
	staleBody?.enqueue(
		new TextEncoder().encode(
			JSON.stringify({
				error: { code: "LIX_STALE", message: "stale main error" },
			}),
		),
	);
	staleBody?.close();
	await Promise.resolve();
	await Promise.resolve();

	const later = events.next();
	const state = await Promise.race([
		later.then(
			() => "resolved",
			() => "rejected",
		),
		new Promise<"pending">((resolve) => setTimeout(() => resolve("pending"), 0)),
	]);
	expect(state).toBe("pending");
	events.close();
	expect(await later).toBeUndefined();
	await lix.close();
});

test("remote observe reconnects retryable failures with fresh headers", async () => {
	vi.useFakeTimers();
	try {
		let headerCalls = 0;
		let observeRequests = 0;
		const observedAuthorization: Array<string | null> = [];
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/@acme/workspace",
				headers: () => ({ Authorization: `Bearer token-${++headerCalls}` }),
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/.lix/v1/")) {
						return handshake();
					}
					observeRequests += 1;
					observedAuthorization.push(request.headers.get("authorization"));
					if (observeRequests <= 2) {
						return sseResponse(
							sseFrame("next", observePayload("first", 0, 0), 25),
						);
					}
					return heldSseResponse(
						sseFrame("next", observePayload("second", 0, 0)),
						request.signal,
					);
				},
			},
		});

		const events = lix.observe("SELECT value");
		expect((await events.next())?.result.rows[0]?.get("value")).toBe("first");
		const afterReconnect = events.next();
		await Promise.resolve();
		await Promise.resolve();
		await vi.advanceTimersByTimeAsync(200);
		const reconnected = await afterReconnect;
		expect(reconnected?.result.rows[0]?.get("value")).toBe("second");
		expect(reconnected?.sequence).toBe(1);
		expect(reconnected?.mutationSequence).toBe(0);
		expect(observedAuthorization).toEqual([
			"Bearer token-2",
			"Bearer token-3",
			"Bearer token-4",
		]);

		events.close();
		await lix.close();
	} finally {
		vi.useRealTimers();
	}
});

test("closing Lix resolves pending remote observation reads", async () => {
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				return new URL(request.url).pathname.endsWith("/.lix/v1/")
					? handshake()
					: heldSseResponse("", request.signal);
			},
		},
	});

	const events = lix.observe("SELECT value");
	const pending = events.next();
	await lix.close();
	expect(await pending).toBeUndefined();
	expect(await events.next()).toBeUndefined();
});

test("closing Lix stops observations before an earlier finite request settles", async () => {
	const executeStarted = deferred<void>();
	const releaseExecute = deferred<void>();
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/.lix/v1/")) return handshake();
				if (pathname.endsWith("/observe")) {
					return heldSseResponse("", request.signal);
				}
				executeStarted.resolve();
				await releaseExecute.promise;
				return Response.json({
					columns: [],
					rows: [],
					rowsAffected: 0,
					notices: [],
				});
			},
		},
	});

	const events = lix.observe("SELECT value");
	const pendingEvent = events.next();
	const executing = lix.execute("SELECT blocked");
	await executeStarted.promise;
	const closing = lix.close();
	expect(await pendingEvent).toBeUndefined();
	releaseExecute.resolve();
	await Promise.all([executing, closing]);
});

function handshake(): Response {
	return Response.json({ protocolVersion: 1, activeBranchId: "main-id" });
}

function observePayload(value: string, sequence: number, mutationSequence: number) {
	return {
		sequence,
		mutationSequence,
		result: {
			columns: ["value"],
			rows: [[{ kind: "text", value }]],
			rowsAffected: 0,
			notices: [],
		},
	};
}

function sseFrame(event: string, value: unknown, retry?: number): string {
	return `${retry === undefined ? "" : `retry: ${retry}\n`}event: ${event}\ndata: ${JSON.stringify(value)}\n\n`;
}

function sseResponse(body: string): Response {
	return new Response(body, {
		headers: { "content-type": "text/event-stream; charset=utf-8" },
	});
}

function heldSseResponse(body: string, signal: AbortSignal): Response {
	const encoded = new TextEncoder().encode(body);
	return new Response(
		new ReadableStream<Uint8Array>({
			start(controller) {
				if (encoded.length > 0) controller.enqueue(encoded);
				const abort = () => {
					try {
						controller.error(new DOMException("Aborted", "AbortError"));
					} catch {
						// The consumer already released the stream.
					}
				};
				if (signal.aborted) abort();
				else signal.addEventListener("abort", abort, { once: true });
			},
		}),
		{ headers: { "content-type": "text/event-stream" } },
	);
}

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	const promise = new Promise<T>((resolvePromise) => {
		resolve = resolvePromise;
	});
	return { promise, resolve };
}
