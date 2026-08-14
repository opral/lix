import { expect, test, vi } from "vitest";
import type { LixBinding, LixStorageConfig } from "./binding-types.js";
import { IndexedDbStorage, openLix } from "./open-lix.js";

const workerMocks = vi.hoisted(() => ({
	clientStateGet: vi.fn(),
	close: vi.fn(),
	openLixWorkerBinding: vi.fn(),
}));

vi.mock("./worker/client.js", () => ({
	openLixWorkerBinding: workerMocks.openLixWorkerBinding,
}));

vi.mock("./remote/client.js", () => ({
	openRemoteLixBinding: vi.fn(),
}));

test("remote client storage closes when restoration reads fail", async () => {
	workerMocks.clientStateGet.mockRejectedValue(
		new Error("client state restoration failed"),
	);
	workerMocks.openLixWorkerBinding.mockImplementation(
		async (
			_storage: LixStorageConfig,
			onDisposed?: () => void,
		): Promise<LixBinding> => ({
			clientStateGet: workerMocks.clientStateGet,
			clientStateSet: vi.fn(),
			clientStateDelete: vi.fn(),
			close: async () => {
				workerMocks.close();
				onDisposed?.();
			},
		}) as LixBinding,
	);
	const options = {
		server: {
			mode: "remote" as const,
			url: "https://lixray.test/@acme/restoration-failure",
		},
		storage: new IndexedDbStorage({ name: "restoration-failure" }),
	};

	await expect(openLix(options)).rejects.toThrow(
		"client state restoration failed",
	);
	await expect(openLix(options)).rejects.toThrow(
		"client state restoration failed",
	);

	expect(workerMocks.openLixWorkerBinding).toHaveBeenCalledTimes(2);
	expect(workerMocks.close).toHaveBeenCalledTimes(2);
});
