import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";

registerPluginExecutionContract(
  "browser",
  async () => await import("@lix-js/sdk"),
);
