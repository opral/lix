import { readFile } from "node:fs/promises";

export type TestPluginArchive = {
	key: string;
	fileName: string;
	archiveBytes: Uint8Array;
};

/** Frozen plugin binaries used to test the SDK host contract. */
export async function loadTestPluginArchives(
	api: "legacy-v2" | "v2" = "v2",
): Promise<TestPluginArchive[]> {
	return await Promise.all(
		(["plugin_csv", "plugin_markdown"] as const).map(async (key) => ({
			key,
			fileName: `${key}.lixplugin`,
			archiveBytes: new Uint8Array(
				await readFile(
					new URL(
						`../../lix/tests/fixtures/plugin-api/${api}/${key}.lixplugin`,
						import.meta.url,
					),
				),
			),
		})),
	);
}
