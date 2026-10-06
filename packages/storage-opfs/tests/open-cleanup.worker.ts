// @ts-expect-error packed provider entry has no declaration file.
import { OpfsBackend } from "../dist/direct.js";

self.onmessage = async (event) => {
	const name = `open-cleanup:${crypto.randomUUID()}`;
	try {
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
