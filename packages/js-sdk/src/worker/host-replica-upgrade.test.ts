import { beforeEach, expect, test, vi } from "vitest";
import type { LixBinding } from "../binding-types.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";
import { startWorkerHost } from "./host.js";

const bindings = vi.hoisted(() => ({
	convert: vi.fn(async () => undefined),
	cleanup: vi.fn(async () => 2),
}));
vi.mock("#binding", () => ({
	openLixBinding: vi.fn(),
	convertReplicaBinding: bindings.convert,
	retryReplicaMigrationCleanupBinding: bindings.cleanup,
	createHostedBinding: vi.fn(),
	deleteHostedBinding: vi.fn(),
}));
beforeEach(() => vi.clearAllMocks());

for (const kind of ["replica.convert", "replica.cleanup"] as const) {
	test(`${kind} runs in an unopened worker and preserves its result`, async () => {
		const responses: WorkerResponse[] = [];
		let receive!: (message: WorkerInput) => void;
		const controller = startWorkerHost({
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => { receive = listener; },
		});
		const storage = { kind: "memory" as const };
		receive({ id: 1, sessionId: 0, operation: {
			kind, storage,
			server: { dynamicHeaders: false, url: "https://example.com/lix/test", headers: [] },
		} });
		await vi.waitFor(() => expect(responses).toContainEqual(
			kind === "replica.convert" ? { id: 1, ok: true } : { id: 1, ok: true, value: 2 },
		));
		expect(kind === "replica.convert" ? bindings.convert : bindings.cleanup)
			.toHaveBeenCalledWith(storage, expect.objectContaining({ url: "https://example.com/lix/test" }), ...(kind === "replica.convert" ? [undefined] : []));
		await controller.close();
	});

	test(`${kind} still rejects storage held by an open session`, async () => {
		const responses: WorkerResponse[] = [];
		let receive!: (message: WorkerInput) => void;
		const controller = startWorkerHost({
			postMessage: (message) => responses.push(message),
			onMessage: (listener) => { receive = listener; },
		}, async () => ({ setTelemetryParent() {}, async close() {} }) as unknown as LixBinding);
		receive({ id: 1, sessionId: 0, operation: { kind: "open", storage: { kind: "memory" }, telemetryEnabled: false, progressEnabled: false } });
		await vi.waitFor(() => expect(responses).toContainEqual({ id: 1, ok: true }));
		receive({ id: 2, sessionId: 0, operation: { kind, storage: { kind: "memory" }, server: { dynamicHeaders: false, url: "https://example.com/lix/test", headers: [] } } });
		await vi.waitFor(() => expect(responses).toContainEqual(expect.objectContaining({ id: 2, ok: false, error: expect.objectContaining({ message: expect.stringContaining("requires closed storage") }) })));
		expect(bindings.convert).not.toHaveBeenCalled();
		expect(bindings.cleanup).not.toHaveBeenCalled();
		await controller.close();
	});
}
