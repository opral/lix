import { bundledPluginArchives, openLix } from "@lix-js/sdk";

globalThis.__lixProductionSmoke = run();

async function run() {
	const lix = await openLix();
	try {
		const query = await lix.execute("SELECT $1 AS message", ["production"]);
		const archives = await bundledPluginArchives();
		const csvPlugin = archives.find((plugin) => plugin.key === "plugin_csv_v2");
		const markdownPlugin = archives.find(
			(plugin) => plugin.key === "plugin_markdown_incremental_v2",
		);
		if (!csvPlugin) throw new Error("Bundled CSV plugin is missing");
		if (!markdownPlugin) throw new Error("Bundled Markdown plugin is missing");
		return {
			message: query.rows[0]?.get("message"),
			bundledPluginKeys: [csvPlugin.key, markdownPlugin.key].sort(),
		};
	} finally {
		await lix.close();
	}
}
