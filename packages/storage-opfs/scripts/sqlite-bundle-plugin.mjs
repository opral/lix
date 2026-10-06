import { readFile } from "node:fs/promises";

function replaceOnce(source, before, after) {
	if (source.split(before).length !== 2)
		throw new Error("SQLite package layout changed; review non-destructive pool cleanup");
	return source.replace(before, after);
}

// SQLite 3.53.0-build1 deletes the data directory when initial pool opening
// fails. Opening may fail while acquiring an existing file's access handle;
// cleanup must release handles and dispose the VFS without deleting data.
// Keep explicit removeVfs() destructive, as specified by upstream's API.
export function sqliteBundlePlugin() {
	return {
		name: "sqlite-preserve-repositories",
		setup(builder) {
			builder.onLoad(
				{ filter: /@sqlite\.org[\\/]sqlite-wasm[\\/]dist[\\/]index\.mjs$/ },
				async ({ path }) => {
					let source = await readFile(path, "utf8");
					const moduleStart = "//#region src/bin/sqlite3-worker1-promiser.mjs";
					const initializerStart = "//#region src/bin/sqlite3-bundler-friendly.mjs";
					const extraExport = ", sqlite3_worker1_promiser_default as sqlite3Worker1Promiser";
					if (!source.startsWith(moduleStart) || !source.includes(initializerStart))
						throw new Error("SQLite package layout changed; review initializer-only bundle");
					// The unused Worker1 API otherwise exposes an unrelated worker URL
					// to consumers rebundling our direct or migration provider.
					source = replaceOnce(source.slice(source.indexOf(initializerStart)), extraExport, "");
					source = replaceOnce(source,
						"async removeVfs() {\n\t\t\t\t\tif (!this.#cVfs.pointer || !this.#dhOpaque) return false;",
						"async removeVfs(removeFiles = true) {\n\t\t\t\t\tif (!this.#cVfs.pointer) return false;");
					source = replaceOnce(source,
						"await this.#dhVfsRoot.removeEntry(OPAQUE_DIR_NAME, { recursive: true });",
						"if (removeFiles && this.#dhOpaque) await this.#dhVfsRoot.removeEntry(OPAQUE_DIR_NAME, { recursive: true });");
					source = replaceOnce(source,
						"await this.#dhVfsParent.removeEntry(this.#dhVfsRoot.name, { recursive: true });",
						"if (removeFiles && this.#dhVfsParent) await this.#dhVfsParent.removeEntry(this.#dhVfsRoot.name, { recursive: true });");
					source = replaceOnce(source,
						"await thePool.removeVfs().catch(() => {});",
						"await thePool.removeVfs(false).catch(() => {});");
					return { contents: source, loader: "js" };
				},
			);
		},
	};
}
