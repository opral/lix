// @ts-expect-error packed provider entry has no declaration file.
import { OpfsBackend } from "../dist/direct.js";

self.onmessage = async (event) => {
	const name = event.data.name ?? `open-cleanup:${crypto.randomUUID()}`;
	try {
		if (event.data.scenario === "pool-failure" || event.data.scenario === "pool-failure-retry") {
			const original = FileSystemFileHandle.prototype.createSyncAccessHandle;
			const failure = new Error("interrupted OPFS handle acquisition");
			const existing = new Set<string>(event.data.files);
			FileSystemFileHandle.prototype.createSyncAccessHandle = async function (...args) {
				if (existing.has(this.name)) throw failure;
				return original.apply(this, args);
			};
			let error;
			try {
				await OpfsBackend.open(name);
			} catch (caught) {
				error = caught;
			} finally {
				FileSystemFileHandle.prototype.createSyncAccessHandle = original;
			}
			if (error !== failure) throw new Error("opening did not preserve access-handle failure");
			if (event.data.scenario === "pool-failure-retry") {
				const db = await OpfsBackend.open(name);
				let value;
				try {
					const read = await db.beginRead({ durability: "durable", consistency: "snapshot" });
					const values = await read.getMany([{ space: { id: 987, name: "open-cleanup", valueSemantics: "mutable", valueIntegrity: "backendVerified" }, keys: [new Uint8Array([1])], options: { projection: "fullValue" } }]);
					value = values[0]?.value?.[0];
				} finally { await db.close(); }
				self.postMessage({ value });
				return;
			}
			self.postMessage({ ok: true });
			return;
		}
		if (event.data.scenario === "seed" || event.data.scenario === "read") {
			const db = await OpfsBackend.open(name);
			const space = { id: 987, name: "open-cleanup", valueSemantics: "mutable", valueIntegrity: "backendVerified" };
			let result;
			try {
				if (event.data.scenario === "seed") {
					const write = await db.beginWrite({ awaitDurable: true, preconditions: [], batchCapacityHintBytes: 256 });
					await write.putMany(space, [{ key: new Uint8Array([1]), value: new Uint8Array([42]) }]);
					await write.commit();
					result = { ok: true };
				} else {
					const read = await db.beginRead({ durability: "durable", consistency: "snapshot" });
					const values = await read.getMany([{ space, keys: [new Uint8Array([1])], options: { projection: "fullValue" } }]);
					result = { value: values[0]?.value?.[0] };
				}
			} finally { await db.close(); }
			self.postMessage(result);
			return;
		}
		if (event.data.scenario === "sqlite-retry") {
			const compile = WebAssembly.compile;
			const failure = new Error("interrupted SQLite compilation");
			WebAssembly.compile = async () => { throw failure; };
			let original;
			try {
				await OpfsBackend.open(name);
			} catch (error) {
				original = error;
			} finally {
				WebAssembly.compile = compile;
			}
			if (original !== failure) throw new Error("opening did not preserve compilation error");
			const reopened = await OpfsBackend.open(name);
			await reopened.close();
			self.postMessage({ ok: true });
			return;
		}
		if (event.data.scenario === "aliases") {
			const first = await OpfsBackend.open(name + "\ud800");
			let alias;
			let code;
			try {
				alias = await OpfsBackend.open(name + "\ud801");
			} catch (error) {
				code = (error as { code?: string }).code;
			} finally {
				await alias?.close();
				await first.close();
			}
			self.postMessage({ code });
			return;
		}
		let original: unknown;
		try {
			await OpfsBackend.open(name, () => {
				throw new Error("ownership callback failed");
			});
		} catch (error) {
			original = error;
		}
		if (!(original instanceof Error) || original.message !== "ownership callback failed")
			throw new Error("opening did not preserve the callback error");
		const reopened = await OpfsBackend.open(name);
		await reopened.close();
		self.postMessage({ ok: true });
	} catch (error) {
		self.postMessage({ error: error instanceof Error ? error.message : String(error) });
	}
};
