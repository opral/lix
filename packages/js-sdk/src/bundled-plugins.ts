import { readFile } from "node:fs/promises";

export type BundledPluginArchive = {
	key: string;
	path: string;
	archiveBytes: Uint8Array;
};

const BUNDLED_PLUGIN_MANIFEST = [
	{
		key: "plugin_md_v2",
		path: "/.lix_system/plugins/plugin_md_v2.lixplugin",
		fileName: "plugin_md_v2.lixplugin",
	},
	{
		key: "plugin_csv",
		path: "/.lix_system/plugins/plugin_csv.lixplugin",
		fileName: "plugin_csv.lixplugin",
	},
] as const;

export async function bundledPluginArchives(): Promise<BundledPluginArchive[]> {
	return await Promise.all(
		BUNDLED_PLUGIN_MANIFEST.map(async (plugin) => ({
			key: plugin.key,
			path: plugin.path,
			archiveBytes: await readBundledArchive(plugin.fileName),
		})),
	);
}

async function readBundledArchive(fileName: string): Promise<Uint8Array> {
	const urls = [
		new URL(`./bundled-plugins/${fileName}`, import.meta.url),
		new URL(`../dist/bundled-plugins/${fileName}`, import.meta.url),
	];
	for (const url of urls) {
		try {
			return new Uint8Array(await readFile(url));
		} catch {
			// Try the next build/source layout.
		}
	}
	return new Uint8Array(await readFile(urls[0]));
}
