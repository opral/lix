import { unzipSync } from "fflate";

const PLUGIN_STORAGE_ROOT = "/.lix_system/plugins/";
const PLUGIN_ARCHIVE_EXTENSION = ".lixplugin";
const MANIFEST_PATH = "manifest.json";

export function pluginArchivePathFromArchive(archiveBytes: Uint8Array): string {
	const files = unzipSync(archiveBytes);
	const manifestBytes = files[MANIFEST_PATH];
	if (!manifestBytes) {
		throw new Error(`Plugin archive is missing ${MANIFEST_PATH}`);
	}
	const manifest = JSON.parse(new TextDecoder().decode(manifestBytes)) as {
		key?: unknown;
	};
	if (typeof manifest.key !== "string") {
		throw new Error("Plugin manifest key must be a string");
	}
	const key = validatePluginKey(manifest.key);
	return `${PLUGIN_STORAGE_ROOT}${key}${PLUGIN_ARCHIVE_EXTENSION}`;
}

function validatePluginKey(key: string): string {
	if (
		key.length === 0 ||
		key === "." ||
		key === ".." ||
		key.includes("/") ||
		key.includes("\\")
	) {
		throw new Error(`Plugin manifest key '${key}' must be a path segment`);
	}
	return key;
}
