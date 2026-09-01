import { expect, test, vi } from "vitest";
import { openLix } from "../index.js";

test("remote observe streams native Lix results", async () => {
	const requests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				if (request.method === "DELETE") return closedSession();
				const pathname = new URL(request.url).pathname;
				return pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")
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
	expect(initial?.result.rows[0]?.value).toBe("hello");
	expect(requests[1]?.headers.get("accept")).toBe("text/event-stream");
	expect(requests[1]?.headers.get("lix-session-id")).toBe("session-1");
	expect(new URL(requests[1]?.url ?? "").pathname).toBe(
		"/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/observe/multiplex",
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
	expect(
		requests.some((request) => new URL(request.url).pathname.endsWith("/execute")),
	).toBe(false);

	events.close();
	expect(await events.next()).toBeUndefined();
	await lix.close();
});

test("remote observe applies every blob delta before coalescing delivery", async () => {
	const body = [
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 0,
			mutationSequence: 10,
			result: {
				columns: [{ name: "content", type: "blob" }],
				rows: [[{ kind: "blob", base64: "YWJjZGVm" }]],
				rowsAffected: 0,
				notices: [],
			},
		}),
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 1,
			mutationSequence: 11,
			delta: {
				kind: "single-blob-splice",
				baseSequence: 0,
				prefixBytes: 2,
				suffixBytes: 2,
				insertBase64: "WFla",
			},
		}),
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 2,
			mutationSequence: 12,
			delta: {
				kind: "single-blob-splice",
				baseSequence: 1,
				prefixBytes: 3,
				suffixBytes: 2,
				insertBase64: "IQ==",
			},
		}),
	].join("");
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				if (request.method === "DELETE") return closedSession();
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/execute")) {
					return executeBlobResponse(new TextEncoder().encode("abcdef"));
				}
				return pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")
					? handshake()
					: sseResponse(body);
			},
		},
	});

	const events = lix.observe("SELECT content FROM lix_file WHERE id = $1", [
		"file-1",
	]);
	await new Promise((resolve) => setTimeout(resolve, 0));
	const latest = await events.next();
	expect(latest?.sequence).toBe(2);
	expect(latest?.mutationSequence).toBe(12);
	expect(
		new TextDecoder().decode(latest?.result.rows[0]?.content as Uint8Array),
	).toBe("abX!ef");

	events.close();
	await lix.close();
});

test("remote observe applies sequential row deltas before coalescing delivery", async () => {
	const body = [
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 0,
			mutationSequence: 10,
			result: {
				columns: [{ name: "value", type: "text" }],
				rows: [
					[{ kind: "text", value: "a" }],
					[{ kind: "text", value: "b" }],
					[{ kind: "text", value: "c" }],
				],
				rowsAffected: 0,
				notices: [],
			},
		}),
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 1,
			mutationSequence: 11,
			delta: {
				kind: "row-splice",
				baseSequence: 0,
				prefixRows: 1,
				deleteRows: 1,
				insertRows: [[{ kind: "text", value: "x" }]],
			},
		}),
		sseFrame("next", {
			subscriptionId: "observe-1",
			sequence: 2,
			mutationSequence: 12,
			delta: {
				kind: "row-splice",
				baseSequence: 1,
				prefixRows: 2,
				deleteRows: 0,
				insertRows: [[{ kind: "text", value: "y" }]],
			},
		}),
	].join("");
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				if (request.method === "DELETE") return closedSession();
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/execute")) {
					return executeValuesResponse(["a", "b", "c"]);
				}
				return pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")
					? handshake()
					: sseResponse(body);
			},
		},
	});

	const events = lix.observe("SELECT value FROM state ORDER BY value");
	await new Promise((resolve) => setTimeout(resolve, 0));
	const latest = await events.next();
	expect(latest?.sequence).toBe(2);
	expect(latest?.mutationSequence).toBe(12);
	expect(latest?.result.rows.map((row) => row.value)).toEqual([
		"a",
		"x",
		"y",
		"c",
	]);

	events.close();
	await lix.close();
});

test("adding an observation reconnects an established multiplex stream with the full membership", async () => {
	const observeRequests: Request[] = [];
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				observeRequests.push(request.clone());
				const body = (await request.clone().json()) as {
					subscriptions: Array<{ id: string; sql: string }>;
				};
				return heldSseResponse(
					body.subscriptions
						.map((subscription) =>
							sseFrame(
								"next",
								multiplexObservePayload(
									subscription.id,
									subscription.sql.includes("second") ? "second" : "first",
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

	const first = lix.observe("SELECT 'first' AS value");
	expect((await first.next())?.result.rows[0]?.value).toBe("first");
	const second = lix.observe("SELECT 'second' AS value");
	expect((await second.next())?.result.rows[0]?.value).toBe("second");

	const activeRequests = observeRequests.filter(
		(request) => !request.signal.aborted,
	);
	expect(activeRequests).toHaveLength(1);
	const activeBody = (await activeRequests[0]?.json()) as {
		subscriptions: Array<{ id: string }>;
	};
	expect(activeBody.subscriptions.map(({ id }) => id)).toEqual([
		"observe-1",
		"observe-2",
	]);

	first.close();
	second.close();
	await lix.close();
});

test("remote observe shards more than 32 subscriptions without blocking execute", async () => {
	const observeRequests: Request[] = [];
	let headerResolutions = 0;
	let liveObserveRequests = 0;
	let maximumLiveObserveRequests = 0;
	let rebalanced = false;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			headers: () => {
				headerResolutions += 1;
				return {};
			},
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/execute")) {
					const body = (await request.json()) as { sql: string };
					const value = /SELECT (\d+) AS value/.exec(body.sql)?.[1];
					return executeValueResponse(
						value === undefined
							? "executed"
							: `${rebalanced ? "rebalanced" : "value"}-${value}`,
					);
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
					subscriptions: Array<{ id: string; sql: string }>;
				};
				if (body.subscriptions.length > 32) {
					release();
					return Response.json(
						{
							error: {
								code: "LIX_SERVER_PROTOCOL_ERROR",
								message: "subscriptions must contain at most 32 entries",
							},
						},
						{ status: 400 },
					);
				}
				return heldSseResponse(
					body.subscriptions
						.map((subscription) => {
							const value =
								/SELECT (\d+) AS value/.exec(subscription.sql)?.[1] ?? "0";
							return sseFrame(
								"next",
								multiplexObservePayload(
									subscription.id,
									`${rebalanced ? "rebalanced" : "value"}-${value}`,
									0,
									rebalanced ? 1 : 0,
								),
							);
						})
						.join(""),
					request.signal,
				);
			},
		},
	});
	headerResolutions = 0;

	const observations = Array.from({ length: 33 }, (_, index) =>
		lix.observe(`SELECT ${index} AS value`),
	);
	const initial = await Promise.all(
		observations.map((observation) => observation.next()),
	);
	expect(initial.map((event) => event?.result.rows[0]?.value)).toEqual(
		Array.from({ length: 33 }, (_, index) => `value-${index}`),
	);
	expect(liveObserveRequests).toBe(2);
	expect(maximumLiveObserveRequests).toBe(2);
	const activeObserveRequests = observeRequests.filter(
		(request) => !request.signal.aborted,
	);
	expect(activeObserveRequests).toHaveLength(2);
	expect(headerResolutions).toBe(2);
	let submittedSubscriptions = 0;
	for (const request of activeObserveRequests) {
		const body = (await request.clone().json()) as { subscriptions: unknown[] };
		submittedSubscriptions += body.subscriptions.length;
	}
	expect(submittedSubscriptions).toBe(33);
	for (const request of observeRequests) {
		const body = (await request.clone().json()) as {
			subscriptions: unknown[];
		};
		expect(body.subscriptions.length).toBeLessThanOrEqual(32);
	}
	expect(
		observeRequests.every((request) =>
			new URL(request.url).pathname.endsWith("/observe/multiplex"),
		),
	).toBe(true);
	const latestBody = (await activeObserveRequests.at(-1)?.json()) as {
		subscriptions: unknown[];
	};
	expect(latestBody.subscriptions).toHaveLength(1);

	const executed = await lix.execute("SELECT 'executed' AS value");
	expect(executed.rows[0]?.value).toBe("executed");
	expect(liveObserveRequests).toBe(2);

	rebalanced = true;
	observations[0]?.close();
	await new Promise((resolve) => setTimeout(resolve, 0));
	expect(liveObserveRequests).toBe(1);
	const rebalancedRequests = observeRequests.filter(
		(request) => !request.signal.aborted,
	);
	expect(rebalancedRequests).toHaveLength(1);
	const rebalancedBody = (await rebalancedRequests[0]?.json()) as {
		subscriptions: Array<{ id: string }>;
	};
	expect(rebalancedBody.subscriptions).toHaveLength(32);
	expect(rebalancedBody.subscriptions.map(({ id }) => id)).not.toContain(
		"observe-1",
	);
	expect(rebalancedBody.subscriptions.map(({ id }) => id)).toContain(
		"observe-33",
	);
	const rebalancedEvent = await observations[32]?.next();
	expect(rebalancedEvent?.result.rows[0]?.value).toBe("rebalanced-32");
	expect(rebalancedEvent?.mutationSequence).toBe(1);

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
				url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
						return handshake();
					}
					if (request.method === "DELETE") return closedSession();
					if (new URL(request.url).pathname.endsWith("/execute")) {
						return executeValueResponse("recovered");
					}
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
			code: "LIX_SERVER_PROTOCOL_ERROR",
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
				url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
						return handshake();
					}
					if (request.method === "DELETE") return closedSession();
					if (new URL(request.url).pathname.endsWith("/execute")) {
						return executeValueResponse("recovered");
					}
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
		expect((await recovered)?.result.rows[0]?.value).toBe("recovered");
		expect(observeRequests).toBe(2);

		events.close();
		await lix.close();
	} finally {
		vi.useRealTimers();
	}
});

test("remote observe treats unmarked semantic errors as terminal", async () => {
	vi.useFakeTimers();
	try {
		let observeRequests = 0;
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
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
		await vi.advanceTimersByTimeAsync(100);
		expect(observeRequests).toBe(1);

		events.close();
		await lix.close();
	} finally {
		vi.useRealTimers();
	}
});

test("a successful branch switch restarts observations on the pinned session", async () => {
	let observeRequests = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/execute")) {
					return executeValueResponse(
						observeRequests === 1 ? "main-id" : "draft-id",
					);
				}
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
					return Response.json({ branchId: body.branchId });
				}
				observeRequests += 1;
				const observedBranch = observeRequests === 1 ? "main-id" : "draft-id";
				return new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							controller.enqueue(
								new TextEncoder().encode(
									sseFrame(
										"next",
										multiplexObservePayload(
											"observe-1",
											observedBranch,
											0,
											observeRequests - 1,
										),
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
	expect((await events.next())?.result.rows[0]?.value).toBe("main-id");
	expect(await lix.activeBranchId()).toBe("main-id");
	const afterSwitch = events.next();
	await lix.switchBranch({ branchId: "draft-id" });
	const switched = await afterSwitch;
	expect(switched?.result.rows[0]?.value).toBe("draft-id");
	expect(switched?.sequence).toBe(1);
	expect(await lix.activeBranchId()).toBe("draft-id");
	expect(observeRequests).toBe(2);
	await expect(
		lix.switchBranch({ branchId: "missing-id" }),
	).rejects.toMatchObject({ code: "LIX_BRANCH_NOT_FOUND" });
	expect(await lix.activeBranchId()).toBe("draft-id");
	expect(observeRequests).toBe(2);

	events.close();
	await lix.close();
});

test("a local branch switch setup failure preserves a healthy observation", async () => {
	let failHeaders = false;
	let observeRequests = 0;
	let branchSwitchRequests = 0;
	let observeSignal: AbortSignal | undefined;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			headers: () => {
				if (failHeaders) throw new Error("headers unavailable");
				return {};
			},
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) return handshake();
				if (request.method === "DELETE") return closedSession();
				if (pathname.endsWith("/execute")) return executeValueResponse("main-id");
				if (pathname.endsWith("/branch/switch")) {
					branchSwitchRequests += 1;
					return Response.json({ branchId: "draft-id" });
				}
				observeRequests += 1;
				observeSignal = request.signal;
				return heldSseResponse(
					sseFrame(
						"next",
						multiplexObservePayload("observe-1", "main-id", 0, 0),
					),
					request.signal,
				);
			},
		},
	});

	const events = lix.observe("SELECT active_branch");
	expect((await events.next())?.result.rows[0]?.value).toBe("main-id");
	failHeaders = true;
	await expect(
		lix.switchBranch({ branchId: "draft-id" }),
	).rejects.toThrow("headers unavailable");
	expect(branchSwitchRequests).toBe(0);
	expect(observeSignal?.aborted).toBe(false);
	expect(observeRequests).toBe(1);
	expect(await lix.activeBranchId()).toBe("main-id");

	failHeaders = false;
	events.close();
	await lix.close();
});

test("remote observe reconnects after a gone protocol session instead of failing", async () => {
	const requests: Request[] = [];
	let nextSession = 0;
	let observeCalls = 0;
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				requests.push(request.clone());
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
					if (request.headers.has("lix-session-id")) {
						return protocolSessionGone();
					}
					nextSession += 1;
					return Response.json({
						protocolVersion: 6,
						activeBranchId: "main-id",
						activeAccountId: "00000000-0000-7000-8000-000000000002",
						sessionId: `session-${nextSession}`,
					});
				}
				if (request.method === "DELETE") return closedSession();
				observeCalls += 1;
				if (observeCalls === 1) return protocolSessionGone();
				return heldSseResponse(
					sseFrame(
						"next",
						multiplexObservePayload("observe-1", "recovered", 0, 1),
					),
					request.signal,
				);
			},
		},
	});

	const events = lix.observe("SELECT value");
	expect((await events.next())?.result.rows[0]?.value).toBe(
		"recovered",
	);
	expect(observeCalls).toBe(2);
	expect(
		requests
			.filter((request) => request.method === "GET")
			.map((request) => ({
				sessionId: request.headers.get("lix-session-id"),
				activeBranchId: new URL(request.url).searchParams.get(
					"activeBranchId",
				),
			})),
	).toEqual([
		{ sessionId: null, activeBranchId: null },
		{ sessionId: null, activeBranchId: "main-id" },
	]);
	expect(
		requests
			.filter((request) =>
				new URL(request.url).pathname.endsWith("/observe/multiplex"),
			)
			.map((request) => request.headers.get("lix-session-id")),
	).toEqual(["session-1", "session-2"]);

	events.close();
	await lix.close();
});

test("remote observe recovers multiple expired shards with one handshake", async () => {
	let handshakeCalls = 0;
	let expiredShardResponses = 0;
	let observeCalls = 0;
	let valuePrefix = "value";
	const lix = await openLix({
		server: {
			mode: "remote",
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
					handshakeCalls += 1;
					return Response.json({
						protocolVersion: 6,
						activeBranchId: "main-id",
						activeAccountId: "00000000-0000-7000-8000-000000000002",
						sessionId: `session-${handshakeCalls}`,
					});
				}
				if (request.method === "DELETE") return closedSession();
				observeCalls += 1;
				const body = (await request.clone().json()) as {
					subscriptions: Array<{ id: string; sql: string }>;
				};
				if (expiredShardResponses > 0) {
					expiredShardResponses -= 1;
					return sseResponse(
						sseFrame("error", {
							error: {
								code: "LIX_ERROR_PROTOCOL_SESSION_GONE",
								message: "the protocol session expired",
							},
						}),
					);
				}
				return heldSseResponse(
					body.subscriptions
						.map((subscription) => {
							const value =
								/SELECT (\d+) AS value/.exec(subscription.sql)?.[1] ?? "0";
							return sseFrame(
								"next",
								multiplexObservePayload(
									subscription.id,
									`${valuePrefix}-${value}`,
									0,
									valuePrefix === "recovered" ? 2 : 1,
								),
							);
						})
						.join(""),
					request.signal,
				);
			},
		},
	});

	const observations = Array.from({ length: 33 }, (_, index) =>
		lix.observe(`SELECT ${index} AS value`),
	);
	const initial = await Promise.all(
		observations.map((observation) => observation.next()),
	);
	expect(initial.map((event) => event?.result.rows[0]?.value)).toEqual(
		Array.from({ length: 33 }, (_, index) => `value-${index}`),
	);
	const observeCallsBeforeExpiry = observeCalls;
	expiredShardResponses = 2;
	valuePrefix = "recovered";
	observations[0]?.close();
	const replacement = lix.observe("SELECT 33 AS value");
	const recovered = await Promise.all([
		...observations
			.slice(1)
			.map((observation) => observation.next()),
		replacement.next(),
	]);
	expect(recovered.map((event) => event?.result.rows[0]?.value)).toEqual(
		Array.from({ length: 33 }, (_, index) => `recovered-${index + 1}`),
	);
	expect(recovered.map((event) => event?.mutationSequence)).toEqual(
		Array.from({ length: 33 }, () => 2),
	);
	expect(expiredShardResponses).toBe(0);
	expect(observeCalls - observeCallsBeforeExpiry).toBe(4);
	expect(handshakeCalls).toBe(2);

	await lix.close();
});

test("remote observe fails if the recovered protocol session is also gone", async () => {
	vi.useFakeTimers();
	try {
		let handshakeCalls = 0;
		let observeCalls = 0;
		const lix = await openLix({
			server: {
				mode: "remote",
				url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
				fetch: async (input, init) => {
					const request = new Request(input, init);
					const pathname = new URL(request.url).pathname;
					if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
						handshakeCalls += 1;
						expect(request.headers.has("lix-session-id")).toBe(false);
						return Response.json({
							protocolVersion: 6,
							activeBranchId: "main-id",
							activeAccountId: "00000000-0000-7000-8000-000000000002",
							sessionId: `session-${handshakeCalls}`,
						});
					}
					if (request.method === "DELETE") return closedSession();
					observeCalls += 1;
					return protocolSessionGone();
				},
			},
		});

		const events = lix.observe("SELECT value");
		await expect(events.next()).rejects.toMatchObject({
			code: "LIX_ERROR_PROTOCOL_SESSION_GONE",
			status: 410,
		});
		expect(handshakeCalls).toBe(2);
		expect(observeCalls).toBe(2);
		await vi.advanceTimersByTimeAsync(10_000);
		expect(observeCalls).toBe(2);

		events.close();
		await lix.close();
	} finally {
		vi.useRealTimers();
	}
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
				url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
				headers: () => ({ Authorization: `Bearer token-${++headerCalls}` }),
				fetch: async (input, init) => {
					const request = new Request(input, init);
					if (new URL(request.url).pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) {
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
							multiplexObservePayload("observe-1", "second", 0, 1),
						),
						request.signal,
					);
				},
			},
		});

		const events = lix.observe("SELECT value");
		expect((await events.next())?.result.rows[0]?.value).toBe("first");
		const afterReconnect = events.next();
		await Promise.resolve();
		await Promise.resolve();
		await vi.advanceTimersByTimeAsync(200);
		const reconnected = await afterReconnect;
		expect(reconnected?.result.rows[0]?.value).toBe("second");
		expect(reconnected?.sequence).toBe(1);
		expect(reconnected?.mutationSequence).toBe(1);
		expect(observedAuthorization).toEqual([
			"Bearer token-2",
			"Bearer token-3",
			"Bearer token-4",
		]);
		expect(observedSessionIds).toEqual(["session-1", "session-1", "session-1"]);

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
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				if (request.method === "DELETE") return closedSession();
				return new URL(request.url).pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")
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
			url: "https://lixray.test/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
			fetch: async (input, init) => {
				const request = new Request(input, init);
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith("/lix/v1/01936f4e-7b6c-7c3d-8f9a-123456789abc/")) return handshake();
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
		protocolVersion: 6,
		activeBranchId: "main-id",
		activeAccountId: "00000000-0000-7000-8000-000000000002",
		sessionId: "session-1",
	});
}

function protocolSessionGone(): Response {
	return Response.json(
		{
			error: {
				code: "LIX_ERROR_PROTOCOL_SESSION_GONE",
				message:
					"the Lix protocol session is unknown, expired, or closed; open a new client session",
			},
		},
		{ status: 410 },
	);
}

function closedSession(): Response {
	return new Response(null, { status: 204 });
}

function executeValueResponse(value: string): Response {
	return Response.json({
		columns: [{ name: "value", type: "text" }],
		rows: [[{ kind: "text", value }]],
		rowsAffected: 0,
		notices: [],
	});
}

function executeValuesResponse(values: string[]): Response {
	return Response.json({
		columns: [{ name: "value", type: "text" }],
		rows: values.map((value) => [{ kind: "text", value }]),
		rowsAffected: 0,
		notices: [],
	});
}

function executeBlobResponse(bytes: Uint8Array): Response {
	let binary = "";
	for (const byte of bytes) binary += String.fromCharCode(byte);
	return Response.json({
		columns: [{ name: "content", type: "blob" }],
		rows: [[{ kind: "blob", base64: btoa(binary) }]],
		rowsAffected: 0,
		notices: [],
	});
}

function observePayload(
	value: string,
	sequence: number,
	mutationSequence: number,
) {
	return {
		sequence,
		mutationSequence,
		result: {
			columns: [{ name: "value", type: "text" }],
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
