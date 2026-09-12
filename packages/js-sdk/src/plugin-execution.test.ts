import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";

registerPluginExecutionContract("Node", async () => await import("./index.js"));
