import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";
import { loadTestPluginArchives } from "./plugin-test-archives.node.js";

registerPluginExecutionContract(
	"Node",
	async () => await import("./index.js"),
	loadTestPluginArchives,
);
