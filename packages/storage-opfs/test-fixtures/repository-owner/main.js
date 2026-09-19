import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
let lix, transaction, observation, child;
window.api = {
	async open(name) {
		lix = await openLix({ storage: new OpfsStorage({ name }) });
	},
	async close() {
		await lix.close();
	},
	async read(key) {
		return (
			await lix.execute("SELECT value FROM lix_key_value WHERE key = $1", [key])
		).rows[0]?.value;
	},
	async write(key, value) {
		await lix.execute(
			"INSERT INTO lix_key_value (key,value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
			[key, value],
		);
	},
	async childBranch() {
		const branch = await lix.createBranch({ name: "recovery-context" });
		child = await lix.openAnotherSession({ branchId: branch.id });
		return branch.id;
	},
	childBranchId() {
		return child.activeBranchId();
	},
	closeChild() {
		return child.close();
	},
	async begin() {
		transaction = await lix.beginTransaction();
	},
	async txWrite(key, value) {
		await transaction.execute(
			"INSERT INTO lix_key_value (key,value) VALUES ($1,$2)",
			[key, value],
		);
	},
	async rollback() {
		await transaction.rollback();
	},
	async watch() {
		observation = await lix.observe(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["observe"],
		);
		return observation
			.next()
			.then((result) => (result.done ? undefined : result.value));
	},
	next() {
		return observation
			.next()
			.then((result) => (result.done ? undefined : result.value));
	},
	unwatch() {
		observation.return?.();
	},
};
