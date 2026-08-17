import { bundledPluginArchives, openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

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
		const storage = new OpfsStorage({ name: "packed-vite-production" });
		const persistent = await openLix({ storage });
		await persistent.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
			["packed-vite", "persistent-production"],
		);
		await persistent.close();
		const reopened = await openLix({ storage });
		const persisted = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["packed-vite"],
		);
		await reopened.close();
		return {
			message: query.rows[0]?.get("message"),
			opfsMessage: persisted.rows[0]?.get("value"),
			bundledPluginKeys: [csvPlugin.key, markdownPlugin.key].sort(),
		};
	} finally {
		await lix.close();
	}
}
