/// <reference lib="webworker" />

import type {
	LixStorageError,
	LixStorageProvider,
	LixStorageProviderRegistration,
} from "@lix-js/sdk";

type ProviderModule = {
	createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
};

const scope = globalThis as unknown as DedicatedWorkerGlobalScope;

scope.onmessage = (event: MessageEvent<{ registration: LixStorageProviderRegistration }>) => {
	void run(event.data.registration).then(
		(code) => scope.postMessage({ ok: true, code }),
		(error) =>
			scope.postMessage({
				ok: false,
				error: error instanceof Error ? error.stack ?? error.message : String(error),
			}),
	);
};

async function run(registration: LixStorageProviderRegistration): Promise<string | undefined> {
	const providerModule = (await import(
		/* @vite-ignore */ registration.moduleUrl
	)) as ProviderModule;
	const provider = await providerModule.createLixStorageProvider(
		registration.options,
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
