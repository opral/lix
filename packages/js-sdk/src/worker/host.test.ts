import { expect, test, vi } from "vitest";
import type { BindingExecuteResult, LixBinding } from "../binding-types.js";
import type {
	WorkerHostEndpoint,
	WorkerInput,
	WorkerOperation,
	WorkerResponse,
} from "./protocol.js";

const { openLixBinding } = vi.hoisted(() => ({
	openLixBinding: vi.fn(),
}));

vi.mock("#binding", () => ({ openLixBinding }));

import { startWorkerHost } from "./host.js";

test("client state falls back to ordinary Lix SQL for older bindings", async () => {
	const execute = vi.fn(async (sql: string): Promise<BindingExecuteResult> => {
		if (sql.startsWith("SELECT key, value")) {
			return result(
				["key", "value"],
				[
					[
						{ kind: "text", value: "lix_client_state:atelier" },
						{ kind: "json", value: { panel: "right" } },
					],
					[
						{ kind: "text", value: "lix_workspace_branch_id" },
						{ kind: "text", value: "not-client-state" },
					],
				],
			);
		}
		if (sql.startsWith("SELECT value")) {
			return result(["value"], [[{ kind: "boolean", value: true }]]);
		}
		return result([], []);
	});
	openLixBinding.mockResolvedValue({
		execute,
		close: vi.fn(),
	} as unknown as LixBinding);
	const worker = testWorkerHost();
	await worker.request({
		kind: "open",
		storage: { kind: "memory" },
		telemetryEnabled: false,
	});

	await expect(
		worker.request({ kind: "clientState.entries" }),
	).resolves.toEqual([{ key: "atelier", value: { panel: "right" } }]);
	await expect(
		worker.request({ kind: "clientState.get", key: "enabled" }),
	).resolves.toBe(true);
	await worker.request({
		kind: "clientState.set",
		key: "atelier",
		value: { panel: "left" },
	});
	await worker.request({ kind: "clientState.delete", key: "atelier" });

	expect(execute).toHaveBeenNthCalledWith(
		2,
		expect.stringContaining("SELECT value FROM lix_key_value_by_branch"),
		[{ kind: "text", value: "lix_client_state:enabled" }],
	);
	expect(execute).toHaveBeenNthCalledWith(
		3,
		expect.stringContaining("INSERT INTO lix_key_value_by_branch"),
		[
			{ kind: "text", value: "lix_client_state:atelier" },
			{ kind: "json", value: { panel: "left" } },
		],
	);
	expect(execute).toHaveBeenNthCalledWith(
		4,
		expect.stringContaining("DELETE FROM lix_key_value_by_branch"),
		[{ kind: "text", value: "lix_client_state:atelier" }],
	);
});

function testWorkerHost(): {
	request(operation: WorkerOperation): Promise<unknown>;
} {
	let onMessage: ((message: WorkerInput) => void) | undefined;
	let nextRequestId = 1;
	const pending = new Map<
		number,
		{ resolve(value: unknown): void; reject(error: unknown): void }
	>();
	const endpoint: WorkerHostEndpoint = {
		onMessage(listener) {
			onMessage = listener;
		},
		postMessage(message: WorkerResponse) {
			if (!("id" in message)) return;
			const request = pending.get(message.id);
			if (!request) return;
			pending.delete(message.id);
			if (message.ok) request.resolve(message.value);
			else request.reject(new Error(message.error.message));
		},
	};
	startWorkerHost(endpoint);
	return {
		request(operation) {
			const id = nextRequestId++;
			return new Promise((resolve, reject) => {
				pending.set(id, { resolve, reject });
				onMessage?.({ id, operation });
			});
		},
	};
}

function result(
	columns: string[],
	rows: BindingExecuteResult["rows"],
): BindingExecuteResult {
	return { columns, rows, rowsAffected: 0, notices: [] };
}
