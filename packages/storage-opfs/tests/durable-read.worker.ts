/// <reference lib="webworker" />

import type { LixStorageError, LixStorageProvider } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

type ProviderModule = {
	createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
};

const scope = globalThis as unknown as DedicatedWorkerGlobalScope;

scope.onmessage = (event: MessageEvent<{ name: string }>) => {
	void run(event.data.name).then(
		(code) => scope.postMessage({ ok: true, code }),
		(error) =>
			scope.postMessage({
				ok: false,
				error: error instanceof Error ? error.stack ?? error.message : String(error),
			}),
	);
};

async function run(name: string): Promise<string | undefined> {
	const storage = new OpfsStorage({ name });
	const providerModule = (await import(
		/* @vite-ignore */ storage.lixStorage.moduleUrl
	)) as ProviderModule;
	const provider = await providerModule.createLixStorageProvider(
		storage.lixStorage.options,
	);
	try {
		try {
			await provider.beginRead({
				consistency: "latest",
				durability: "durable",
			});
			return undefined;
		} catch (error) {
			return (error as LixStorageError).code;
		}
	} finally {
		await provider.close();
	}
}
