import { bundledPluginArchives, openLix } from "@lix-js/sdk";

globalThis.__lixProductionSmoke = run();

async function run() {
	const lix = await openLix();
	try {
		const query = await lix.execute("SELECT $1 AS message", ["production"]);
		const archives = await bundledPluginArchives();
		const csvPlugin = archives.find((plugin) => plugin.key === "plugin_csv");
		const markdownPlugin = archives.find(
			(plugin) => plugin.key === "plugin_md_v2",
		);
		if (!csvPlugin) throw new Error("Bundled CSV plugin is missing");
		if (!markdownPlugin) throw new Error("Bundled Markdown plugin is missing");

		for (const plugin of [csvPlugin, markdownPlugin]) {
			await writeFile(
				lix,
				`/.lix/plugins/${plugin.key}.lixplugin`,
				plugin.archiveBytes,
			);
		}

		const csvSource = "name,age\nAda,36\nGrace,37\n";
		await writeFile(lix, "/people.csv", new TextEncoder().encode(csvSource));
		const markdownSource = "# Heading\n\nParagraph with **bold** text.\n";
		await writeFile(
			lix,
			"/notes.md",
			new TextEncoder().encode(markdownSource),
		);

		const csvRows = await lix.execute(
			"SELECT cells FROM csv_row ORDER BY order_key",
		);
		const markdownNodes = await lix.execute(
			"SELECT kind FROM markdown_node ORDER BY kind",
		);
		return {
			message: query.rows[0]?.get("message"),
			csv: {
				cells: csvRows.rows.map((row) => row.get("cells")),
				rendered: new TextDecoder().decode(
					await readFile(lix, "/people.csv"),
				),
			},
			markdown: {
				kinds: markdownNodes.rows.map((row) => row.get("kind")),
				rendered: new TextDecoder().decode(
					await readFile(lix, "/notes.md"),
				),
			},
		};
	} finally {
		await lix.close();
	}
}

async function writeFile(lix, path, data) {
	await lix.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) " +
			"ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		[path, data],
	);
}

async function readFile(lix, path) {
	const result = await lix.execute(
		"SELECT data FROM lix_file WHERE path = $1",
		[path],
	);
	return result.rows[0]?.value("data").asBytes() ?? new Uint8Array();
}
