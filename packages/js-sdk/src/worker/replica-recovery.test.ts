import { expect, test, vi } from "vitest";
import type { LixBinding } from "../binding-types.js";
import { Lix } from "../lix.js";
import { BindingLease, LixWorkerClient, workerBinding } from "./client.js";
import { startWorkerHost } from "./host.js";
import type { WorkerInput, WorkerResponse } from "./protocol.js";

async function openHarness(binding: Partial<LixBinding>) {
	let receiveWorker!: (message: WorkerInput) => void;
	let receiveClient!: (message: WorkerResponse) => void;
	const client = new LixWorkerClient({
		postMessage: (message) => receiveWorker(structuredClone(message)),
		onMessage: (listener) => {
			receiveClient = listener;
		},
		onFatal() {},
		ref() {},
		unref() {},
		async terminate() {},
	});
	startWorkerHost(
		{
			postMessage: (message) => receiveClient(structuredClone(message)),
			onMessage: (listener) => {
				receiveWorker = listener;
			},
		},
		async () => ({ setTelemetryParent() {}, ...binding }) as LixBinding,
	);
	client.beginLease();
	await client.request({
		kind: "open",
		storage: { kind: "memory" },
		telemetryEnabled: false,
		progressEnabled: false,
	});
	return new Lix(
		workerBinding(client, new BindingLease(() => client.endLease()), 0),
	);
}

test("recovery errors and unresolved data survive the public and worker boundaries", async () => {
	const source = {
		id: "retained-77",
		sourceFormat: 77,
		repositoryId: "repo",
		accountId: "account",
		recoveryRequired: true,
	};
	const data = {
		version: 1,
		source,
		branches: [],
		commits: [],
		uploads: [],
		blobs: [],
		files: [],
		unresolved: ["missing blob"],
	};
	const failure = Object.assign(
		new Error("Recovery requires an available blob"),
		{ code: "LIX_ERROR_RECOVERY_INCOMPLETE", details: { sourceId: source.id } },
	);
	const lix = await openHarness({
		replicaRecoverySources: async () => [source],
		exportReplicaRecovery: async (id) => {
			expect(id).toBe(source.id);
			return data;
		},
		recoverReplica: async () => {
			throw failure;
		},
		close: async () => {},
	});
	try {
		await expect(lix.replicaRecoverySources()).resolves.toEqual([source]);
		await expect(lix.exportReplicaRecovery(source.id)).resolves.toEqual(data);
		await expect(lix.recoverReplica(source.id)).rejects.toMatchObject({
			message: failure.message,
			code: failure.code,
			details: failure.details,
		});
		await expect(lix.replicaRecoverySources()).resolves.toEqual([source]);
	} finally {
		await lix.close();
	}
});

test("close waits for in-flight recovery and rejects subsequent recovery work", async () => {
	let finish!: () => void;
	const pending = new Promise<void>((resolve) => {
		finish = resolve;
	});
	const recoveryStarted = vi.fn();
	const closed = vi.fn(async () => {});
	const receipt = {
		branchIds: ["recovery-77"],
		restoredFiles: 1,
		restoredRows: 3,
		unresolved: ["local-only state retained"],
	};
	const lix = await openHarness({
		recoverReplica: async () => {
			recoveryStarted();
			await pending;
			return receipt;
		},
		close: closed,
	});
	const recovering = lix.recoverReplica("retained-77");
	await vi.waitFor(() => expect(recoveryStarted).toHaveBeenCalledOnce());
	const closing = lix.close();
	await expect(lix.replicaRecoverySources()).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(closed).not.toHaveBeenCalled();
	finish();
	await expect(recovering).resolves.toEqual(receipt);
	await closing;
	expect(closed).toHaveBeenCalledOnce();
});
