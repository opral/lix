import { beforeEach, expect, test, vi } from "vitest";
import type { LixBinding } from "../binding-types.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";

const mocks = vi.hoisted(() => ({
	open: vi.fn(),
	requestAdmission: vi.fn(),
}));

vi.mock("#binding", () => ({
	openLixBinding: mocks.open,
	convertReplicaBinding: vi.fn(),
}));

vi.mock("./shared-admission.js", async (importOriginal) => {
	const actual = await importOriginal<typeof import("./shared-admission.js")>();
	return { ...actual, requestAdmission: mocks.requestAdmission };
});

vi.mock("./durable-local-admission.js", () => ({
	DurableLocalAdmission: class {
		async record() {}
		async read() { return undefined; }
		async remove() {}
	},
}));

import { createRepositoryHost } from "./repository-host.js";

beforeEach(() => {
	vi.clearAllMocks();
});

test("last detach sends DELETE with the admitted credentials after close cancels a pending fetch", async () => {
	const repositoryId = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
	const authorityUrl = `https://example.test/lix/${repositoryId}`;
	const identity = {
		repositoryId,
		principalId: "account-a",
		protocolEpoch: 31,
		storageEpoch: 87,
	};
	let currentToken = "admitted-token";
	mocks.requestAdmission.mockResolvedValue(identity);

	let nativeTransport!: import("../binding-types.js").SyncServerBindingOptions;
	let pendingLongPoll: Promise<Response> | undefined;
	const root = {
		activeAccountId: async () => identity.principalId,
		openAnotherSession: async () => ({
			setTelemetryParent() {},
			async activeBranchId() { return "branch-a"; },
			async activeAccountId() { return identity.principalId; },
			async close() {},
		}) as unknown as LixBinding,
		close: async () => {
			await pendingLongPoll?.catch(() => undefined);
			await expect(nativeTransport.transport!({
				url: `https://example.test/lix/v1/${repositoryId}/descriptor?after=1`,
				init: { method: "GET" },
				response: { mode: "buffered", maxBytes: 64 },
			})).rejects.toMatchObject({ code: "LIX_TRANSPORT_UNAVAILABLE" });
			const response = await nativeTransport.transport!({
				url: `https://example.test/lix/v1/${repositoryId}/session`,
				init: { method: "DELETE", headers: [["lix-session-id", "native-session"]] },
				response: { mode: "buffered", maxBytes: 64 },
			});
			expect(response.status).toBe(204);
		},
	} as unknown as LixBinding;
	mocks.open.mockImplementation(async (_storage, _telemetry, _parent, server) => {
		nativeTransport = server;
		return root;
	});

	const output: WorkerResponse[] = [];
	let onmessage: ((event: MessageEvent) => void) | null = null;
	let portClosed = false;
	const port = {
		postMessage(message: WorkerResponse) {
			output.push(message);
			if ("kind" in message && message.kind === "sync.headers") {
				queueMicrotask(() => send({
					kind: "sync.headers.result",
					requestId: message.requestId,
					result: { ok: true, headers: [["authorization", currentToken]] },
				}));
			}
			if ("kind" in message && message.kind === "sync.fetch") {
				const request = message.request;
				if (request.method === "DELETE") {
					queueMicrotask(() => send({
						kind: "sync.fetch.result",
						requestId: message.requestId,
						result: {
							ok: true,
							response: {
								status: 204,
								statusText: "No Content",
								headers: [],
								body: new Uint8Array(),
							},
						},
					}));
				}
			}
			if ("kind" in message && message.kind === "sync.fetch.cancel") {
				queueMicrotask(() => send({
					kind: "sync.fetch.result",
					requestId: message.requestId,
					result: {
						ok: false,
						error: {
							name: "HttpTransportError",
							message: "HTTP request was cancelled",
							code: "LIX_TRANSPORT_ABORTED",
						},
					},
				}));
			}
		},
		start() {},
		close() { portClosed = true; },
		get onmessage() { return onmessage; },
		set onmessage(value: ((event: MessageEvent) => void) | null) { onmessage = value; },
	} as unknown as MessagePort;
	const send = (message: WorkerInput | { kind: "repository.disconnect" }) => {
		if (!onmessage) throw new Error("Repository host did not install its message handler");
		onmessage({ data: message } as MessageEvent);
	};
	createRepositoryHost().connect(port);

	send({
		id: 1,
		sessionId: 0,
		operation: {
			kind: "open",
			storage: {
				kind: "jsStorage",
				moduleUrl: "file:///test-storage.js",
				options: { sharedEngineKey: "lix:opfs:close-test" },
			},
			telemetryEnabled: false,
			progressEnabled: false,
			server: { url: authorityUrl, dynamicHeaders: true },
		},
	});
	await vi.waitFor(() => expect(output).toContainEqual(expect.objectContaining({ id: 1, ok: true })));
	expect(mocks.requestAdmission).toHaveBeenCalledTimes(1);

	pendingLongPoll = nativeTransport.transport!({
		url: `https://example.test/lix/v1/${repositoryId}/descriptor?after=1`,
		init: { method: "GET" },
		response: { mode: "buffered", maxBytes: 64 },
	});
	await vi.waitFor(() => expect(output.some((message) =>
		"kind" in message && message.kind === "sync.fetch" && message.request.method === "GET",
	)).toBe(true));
	const headerCallbacksBeforeClose = output.filter((message) =>
		"kind" in message && message.kind === "sync.headers",
	).length;
	currentToken = "unverified-new-token";
	send({ kind: "repository.disconnect" });

	await vi.waitFor(() => expect(output.some((message) =>
		"kind" in message && message.kind === "repository.disconnected",
	)).toBe(true));
	const deleteRequest = output.find((message) =>
		"kind" in message && message.kind === "sync.fetch" && message.request.method === "DELETE",
	);
	expect(deleteRequest).toMatchObject({
		kind: "sync.fetch",
		request: {
			url: `https://example.test/lix/v1/${repositoryId}/session`,
			method: "DELETE",
			headers: [["authorization", "admitted-token"], ["lix-session-id", "native-session"]],
		},
	});
	expect(output.filter((message) =>
		"kind" in message && message.kind === "sync.headers",
	)).toHaveLength(headerCallbacksBeforeClose);
	expect(mocks.requestAdmission).toHaveBeenCalledTimes(1);
	expect(output.find((message) =>
		"kind" in message && message.kind === "repository.disconnected",
	)).toMatchObject({ kind: "repository.disconnected", error: undefined });
	expect(portClosed).toBe(true);
});
