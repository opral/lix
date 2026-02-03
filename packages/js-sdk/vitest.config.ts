import { defineConfig } from "vitest/config";

export default defineConfig({
  server: {
    deps: {
      inline: ["@lix-js/engine-wasm"],
    },
  },
  test: {
    environment: "node",
  },
});
