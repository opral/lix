import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";

async function loadPluginFixtures() {
	return await Promise.all(
		(["plugin_csv", "plugin_markdown"] as const).map(async (key) => {
			const url = new URL(
				`../../lix/tests/fixtures/plugin-api/v2/${key}.lixplugin`,
				import.meta.url,
			);
			const response = await fetch(url);
			if (!response.ok)
				throw new Error(`Could not load frozen plugin archive: ${response.status}`);
			return {
				key,
				fileName: `${key}.lixplugin`,
				archiveBytes: new Uint8Array(await response.arrayBuffer()),
			};
		}),
	);
}

registerPluginExecutionContract(
  "browser",
  async () => await import("@lix-js/sdk"),
  loadPluginFixtures,
);
