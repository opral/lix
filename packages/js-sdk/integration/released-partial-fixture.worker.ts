// Replay frozen physical records, including derived indexes, pending work and
// epoch metadata. Canonical snapshot export omits part of this evidence.
// @ts-expect-error bundled direct backend has no declaration
import { OpfsBackend } from "../../storage-opfs/dist/direct.js";
import type { OpfsBackend as Backend } from "../../storage-opfs/js/provider.js";

self.onmessage = async ({ data }) => {
	let backend: Backend | undefined;
	try {
		const bytes = new Uint8Array(data.records);
		if (new TextDecoder().decode(bytes.subarray(0, 8)) !== "LIXOPFS1")
			throw new Error("Invalid physical fixture header");
		const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
		const count = view.getUint32(8);
		const entries = [];
		let offset = 12;
		for (let index = 0; index < count; index++) {
			if (offset + 12 > bytes.length) throw new Error("Truncated physical fixture");
			const id = view.getUint32(offset);
			const keyLength = view.getUint32(offset + 4);
			const valueLength = view.getUint32(offset + 8);
			offset += 12;
			if (offset + keyLength + valueLength > bytes.length)
				throw new Error("Truncated physical fixture record");
			const key = bytes.slice(offset, offset + keyLength);
			offset += keyLength;
			const value = bytes.slice(offset, offset + valueLength);
			offset += valueLength;
			entries.push({ id, key, value });
		}
		if (offset !== bytes.length) throw new Error("Trailing physical fixture bytes");
		backend = await OpfsBackend.open(data.name);
		const sessionToken = await backend.acquireSession();
		if (data.verifySource) {
			const read = await backend.beginRead({ sessionToken, consistency: "snapshot", durability: "durable" });
			for (const { id, key, value } of entries) {
				const [actual] = await read.getMany([{
					space: { id, name: `fixture.${id}`, valueSemantics: "mutable", valueIntegrity: "backendVerified" },
					keys: [key], options: { projection: "fullValue" },
				}]);
				if (actual?.kind !== "fullValue" || actual.value.length !== value.length ||
					!actual.value.every((byte, index) => byte === value[index]))
					throw new Error("Failed migration changed a frozen source record");
			}
			await backend.close();
			backend = undefined;
			self.postMessage({ ok: true });
			return;
		}
		if (!backend.isEmptyForMigration()) throw new Error("Fixture requires fresh storage");
		const write = await backend.beginWrite({
			sessionToken,
			awaitDurable: true,
			preconditions: [],
			batchCapacityHintBytes: bytes.length,
		});
		for (const { id, key, value } of entries) {
			await write.putMany({
				id, name: `fixture.${id}`,
				valueSemantics: "mutable", valueIntegrity: "backendVerified",
			}, [{ key, value }]);
		}
		await write.commit();
		await backend.close();
		backend = undefined;
		self.postMessage({ ok: true });
	} catch (error) {
		self.postMessage({ error: String(error) });
	} finally {
		await backend?.close();
		self.close();
	}
};
