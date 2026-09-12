import { bundledPluginArchives, openLix } from "@lix-js/sdk";

globalThis.__lixProductionSmoke = run();

async function run() {
	const lix = await openLix();
	try {
		const query = await lix.execute("SELECT $1 AS message", ["production"]);
		const archives = await bundledPluginArchives();
		const csvPlugin = archives.find((plugin) => plugin.key === "plugin_csv");
		const markdownPlugin = archives.find(
			(plugin) => plugin.key === "plugin_markdown",
		);
		if (!csvPlugin) throw new Error("Bundled CSV plugin is missing");
		if (!markdownPlugin) throw new Error("Bundled Markdown plugin is missing");
        await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
            `/.lix/plugins/${csvPlugin.fileName}`, csvPlugin.archiveBytes,
        ]);
        await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
            "/people.csv", new TextEncoder().encode("name,age\nAda,36\n"),
        ]);
        const rows = await lix.execute("SELECT cells FROM csv_row ORDER BY order_key");
        if (JSON.stringify(rows.rows.map(row => row.cells)) !== JSON.stringify([["name", "age"], ["Ada", "36"]])) {
            throw new Error("CSV plugin did not execute in the production browser worker");
        }
		return {
			message: query.rows[0]?.message,
			bundledPluginKeys: [csvPlugin.key, markdownPlugin.key].sort(),
		};
	} finally {
		await lix.close();
	}
}
