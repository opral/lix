import type { LixStorageProvider } from "@lix-js/sdk";

/** Real OPFS with one slow candidate read, allowing the real migration lease
 * heartbeat to commit. No synthetic storage error or fake snapshot is used. */
export async function createLixStorageProvider(options: {
	moduleUrl: string;
	providerOptions: unknown;
	notificationChannel: string;
}): Promise<LixStorageProvider> {
	const module = await import(/* @vite-ignore */ options.moduleUrl);
	const provider: LixStorageProvider = await module.createLixStorageProvider(
		options.providerOptions,
	);
	let delayed = false;
	return new Proxy(provider, {
		get(target, property) {
			if (property === "beginRead") {
				return async (...args: Parameters<LixStorageProvider["beginRead"]>) => {
					const read = await target.beginRead(...args);
					return new Proxy(read, {
						get(readTarget, readProperty) {
							if (readProperty === "beginScan") {
								return async (...scanArgs: Parameters<typeof read.beginScan>) => {
									const scan = await readTarget.beginScan(...scanArgs);
									return new Proxy(scan, {
										get(scanTarget, scanProperty) {
											if (scanProperty === "nextPage") {
												return async (...pageArgs: Parameters<typeof scan.nextPage>) => {
													// Fresh snapshot candidates occupy an epoch bank. Delay
													// the first actual candidate page, independent of its
													// logical space or position in the snapshot registry.
													if (!delayed && scanArgs[0].id >= 0x40000000) {
														delayed = true;
														const notification = new BroadcastChannel(options.notificationChannel);
														notification.postMessage("candidate-page-delayed");
														notification.close();
														await new Promise((resolve) => setTimeout(resolve, 1500));
													}
													return scanTarget.nextPage(...pageArgs);
												};
											}
											const value = Reflect.get(scanTarget, scanProperty);
											return typeof value === "function" ? value.bind(scanTarget) : value;
										},
									});
								};
							}
							const value = Reflect.get(readTarget, readProperty);
							return typeof value === "function" ? value.bind(readTarget) : value;
						},
					});
				};
			}
			const value = Reflect.get(target, property);
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
}
