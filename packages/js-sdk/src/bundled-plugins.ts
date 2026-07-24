export type BundledPluginArchive = {
	key: string;
	fileName: string;
	archiveBytes: Uint8Array;
};

const BUNDLED_PLUGIN_MANIFEST = [
	{
		key: "plugin_markdown_incremental_v2",
		fileName: "plugin_markdown_incremental_v2.lixplugin",
	},
	{
		key: "plugin_csv_v2",
		fileName: "plugin_csv_v2.lixplugin",
	},
] as const;

export async function bundledPluginArchives(): Promise<BundledPluginArchive[]> {
	return await Promise.all(
		BUNDLED_PLUGIN_MANIFEST.map(async (plugin) => ({
			key: plugin.key,
			fileName: plugin.fileName,
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
			if (url.protocol !== "file:") {
				const response = await fetch(url);
				if (response.ok) {
					return new Uint8Array(await response.arrayBuffer());
				}
				continue;
			}
			const moduleName = "node:fs/promises";
			const { readFile } = await import(/* @vite-ignore */ moduleName);
			return new Uint8Array(await readFile(url));
		} catch {
			// Try the next build/source layout.
		}
	}
	throw new Error(`Could not load bundled plugin archive ${fileName}`);
}
