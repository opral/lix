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
				if (request.method === "DELETE") return closedSession();
				return new URL(request.url).pathname.endsWith("/lix/v1/")
					? handshake()
					: sseResponse(
							sseFrame(
								"next",
								multiplexObservePayload("observe-1", "hello", 0, 7),
							),
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
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
	expect(new URL(requests[1]?.url ?? "").pathname).toBe(
		"/@acme/workspace/lix/v1/observe/multiplex",
	);
	expect(await requests[1]?.json()).toEqual({
		subscriptions: [
			{
				id: "observe-1",
				sql: "SELECT $1 AS value",
				params: [{ kind: "text", value: "hello" }],
			},
		],
	});

	events.close();
	expect(await events.next()).toBeUndefined();
	await lix.close();
});

test("remote observe multiplexes more than six subscriptions without blocking execute", async () => {
	const observeRequests: Request[] = [];
	let liveObserveRequests = 0;
	let maximumLiveObserveRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/execute")) {
					return Response.json({
						columns: ["value"],
						rows: [[{ kind: "text", value: "executed" }]],
						rowsAffected: 0,
						notices: [],
					});
				}

				observeRequests.push(request.clone());
				liveObserveRequests += 1;
				maximumLiveObserveRequests = Math.max(
					maximumLiveObserveRequests,
					liveObserveRequests,
				);
				let released = false;
				const release = () => {
					if (released) return;
					released = true;
					liveObserveRequests -= 1;
				};
				request.signal.addEventListener("abort", release, { once: true });
				await Promise.resolve();
				if (request.signal.aborted) {
					release();
					throw new DOMException("Aborted", "AbortError");
				}
				const body = (await request.clone().json()) as {
					subscriptions: Array<{ id: string }>;
				};
				return heldSseResponse(
					body.subscriptions
						.map((subscription, index) =>
							sseFrame(
								"next",
								multiplexObservePayload(
									subscription.id,
									`value-${index}`,
									0,
									0,
								),
							),
						)
						.join(""),
					request.signal,
				);
			},
		},
	});

	const observations = Array.from({ length: 8 }, (_, index) =>
		lix.observe(`SELECT ${index} AS value`),
	);
	const initial = await Promise.all(
		observations.map((observation) => observation.next()),
	);
	expect(initial.map((event) => event?.result.rows[0]?.get("value"))).toEqual(
		Array.from({ length: 8 }, (_, index) => `value-${index}`),
	);
	expect(liveObserveRequests).toBe(1);
	expect(maximumLiveObserveRequests).toBe(1);
	expect(
		observeRequests.every((request) =>
			new URL(request.url).pathname.endsWith("/observe/multiplex"),
		),
	).toBe(true);
	const latestBody = (await observeRequests.at(-1)?.json()) as {
		subscriptions: unknown[];
	};
	expect(latestBody.subscriptions).toHaveLength(8);

	const executed = await lix.execute("SELECT 'executed' AS value");
	expect(executed.rows[0]?.get("value")).toBe("executed");
	expect(liveObserveRequests).toBe(1);

	await lix.close();
	expect(liveObserveRequests).toBe(0);
});

test("hub-wide protocol failures abort a held multiplex stream without reconnecting", async () => {
	vi.useFakeTimers();
	try {
		let observeRequests = 0;
		let liveObserveRequests = 0;
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/@acme/workspace",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/")) {
						return handshake();
					}
					if (request.method === "DELETE") return closedSession();
					observeRequests += 1;
					liveObserveRequests += 1;
					request.signal.addEventListener(
						"abort",
						() => {
							liveObserveRequests -= 1;
						},
						{ once: true },
					);
					return heldSseResponse(
						sseFrame("next", observePayload("missing subscription id", 0, 0)),
						request.signal,
					);
				},
			},
		});

		const events = lix.observe("SELECT value");
		await expect(events.next()).rejects.toMatchObject({
			code: "LIX_REMOTE_PROTOCOL_ERROR",
		});
		expect(liveObserveRequests).toBe(0);
		await vi.advanceTimersByTimeAsync(10_000);
		expect(observeRequests).toBe(1);

		await lix.close();
	} finally {
		vi.useRealTimers();
	}
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
					if (new URL(request.url).pathname.endsWith("/lix/v1/")) {
						return handshake();
					}
					if (request.method === "DELETE") return closedSession();
					observeRequests += 1;
					return observeRequests === 1
						? sseResponse(
								sseFrame("error", {
									subscriptionId: "observe-1",
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
								sseFrame(
									"next",
									multiplexObservePayload("observe-1", "recovered", 0, 2),
								),
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
			fetch: async (input, init) => {
				const request = new Request(input, init);
				if (new URL(request.url).pathname.endsWith("/lix/v1/")) {
					return handshake();
				}
				if (request.method === "DELETE") return closedSession();
				observeRequests += 1;
				return sseResponse(
					sseFrame("error", {
						subscriptionId: "observe-1",
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

test("a successful branch switch updates observations on the existing server stream", async () => {
	let observeController: ReadableStreamDefaultController<Uint8Array> | undefined;
	let observeRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/@acme/workspace",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/branch/switch")) {
					const body = (await request.json()) as { branchId: string };
					if (body.branchId === "missing-id") {
						return Response.json(
								{
									error: {
										code: "LIX_BRANCH_NOT_FOUND",
										message: "Branch not found",
									},
								},
								{ status: 404 },
							);
					}
					observeController?.enqueue(
						new TextEncoder().encode(
							sseFrame(
								"next",
								multiplexObservePayload("observe-1", body.branchId, 1, 1),
							),
						),
					);
					return Response.json({ branchId: body.branchId });
				}
				observeRequests += 1;
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							observeController = controller;
							controller.enqueue(
								new TextEncoder().encode(
									sseFrame(
										"next",
										multiplexObservePayload("observe-1", "main-id", 0, 0),
									),
								),
							);
						},
					}),
					{ headers: { "content-type": "text/event-stream" } },
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
	expect(observeRequests).toBe(1);
	await expect(
		lix.switchBranch({ branchId: "missing-id" }),
	).rejects.toMatchObject({ code: "LIX_BRANCH_NOT_FOUND" });
	expect(observeRequests).toBe(1);

	events.close();
	await lix.close();
});

test("remote observe reconnects retryable failures with fresh headers", async () => {
	vi.useFakeTimers();
	try {
		let headerCalls = 0;
		let observeRequests = 0;
		const observedAuthorization: Array<string | null> = [];
		const observedSessionIds: Array<string | null> = [];
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/@acme/workspace",
				headers: () => ({ Authorization: `Bearer token-${++headerCalls}` }),
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/")) {
						return handshake();
					}
					if (request.method === "DELETE") return closedSession();
					observeRequests += 1;
					observedAuthorization.push(request.headers.get("authorization"));
					observedSessionIds.push(request.headers.get("lix-session-id"));
					if (observeRequests <= 2) {
						return sseResponse(
							sseFrame(
								"next",
								multiplexObservePayload("observe-1", "first", 0, 0),
								25,
							),
						);
					}
					return heldSseResponse(
						sseFrame(
							"next",
							multiplexObservePayload("observe-1", "second", 0, 0),
						),
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
		expect(observedSessionIds).toEqual([
			"session-1",
			"session-1",
			"session-1",
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
				if (request.method === "DELETE") return closedSession();
				return new URL(request.url).pathname.endsWith("/lix/v1/")
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
				if (pathname.endsWith("/lix/v1/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/observe/multiplex")) {
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
	return Response.json({
		protocolVersion: 1,
		activeBranchId: "main-id",
		sessionId: "session-1",
	});
}

function closedSession(): Response {
	return new Response(null, { status: 204 });
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

function multiplexObservePayload(
	subscriptionId: string,
	value: string,
	sequence: number,
	mutationSequence: number,
) {
	return {
		subscriptionId,
		...observePayload(value, sequence, mutationSequence),
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
