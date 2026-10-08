// @ts-expect-error bundled direct entry
import { OpfsBackend } from "../dist/direct.js";
import type { OpfsBackend as Backend } from "../js/provider.js";
import type { LixStorageSpace } from "@lix-js/sdk";
self.onmessage = async () => {
	const source: Backend = await OpfsBackend.open(
		`migration-source:${crypto.randomUUID()}`,
	);
	const target: Backend = await OpfsBackend.open(
		`migration-target:${crypto.randomUUID()}`,
	);
	try {
		for (const id of [1, 33, 101, 1001]) {
			const space: LixStorageSpace = {
				id,
				name: `logical-${id}`,
				valueSemantics: "mutable",
				valueIntegrity: "backendVerified",
			};
			const write = await source.beginWrite({
				awaitDurable: true,
				preconditions: [],
				batchCapacityHintBytes: 4096,
			});
			await write.putMany(
				space,
				Array.from({ length: 70 }, (_, i) => ({
					key: new Uint8Array([i]),
					value: new TextEncoder().encode(`pending-receipt-${id}-${i}`),
				})),
			);
			await write.commit();
		}
		const largeSpace: LixStorageSpace = {
			id: 1002,
			name: "legacy-large-codec",
			valueSemantics: "mutable",
			valueIntegrity: "backendVerified",
		};
		const largeBytes = 17 * 1024 * 1024;
		const largeWrite = await source.beginWrite({
			awaitDurable: true,
			preconditions: [],
			batchCapacityHintBytes: largeBytes,
		});
		await largeWrite.putMany(largeSpace, [
			{
				key: new Uint8Array([0]),
				value: new Uint8Array(largeBytes).fill(0x5a),
			},
		]);
		await largeWrite.commit();
		const before = await source.migrationDigest();
		const copied = await source.copyForMigration(target);
		const targetDigest = await target.migrationDigest();
		const after = await source.migrationDigest();
		let nonemptyRejected = false;
		try {
			await source.copyForMigration(target);
		} catch {
			nonemptyRejected = true;
		}
		self.postMessage({
			before,
			copied,
			targetDigest,
			after,
			nonemptyRejected,
			largeBytes,
		});
	} catch (error) {
		self.postMessage({ error: String(error) });
	} finally {
		await target.close();
		await source.close();
	}
};
