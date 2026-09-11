import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { defineConfig, mergeConfig } from "vitest/config";
import browserConfig from "./vitest.browser.config.js";

// A separate opt-in configuration keeps an external authority prerequisite out
// of the ordinary browser suite. The manifest is produced by the native fixture.
const config = mergeConfig(browserConfig, defineConfig({
 plugins: [{
  name: "partial-replica-profile-artifacts",
  configureServer(server) {
   server.middlewares.use(async (request, response, next) => {
    try {
     if (request.url === "/__partial_sync_profile.json") {
      const path = process.env.LIX_PARTIAL_PROFILE_MANIFEST;
      if (!path) throw new Error("Set LIX_PARTIAL_PROFILE_MANIFEST to the seeded authority manifest");
      response.setHeader("content-type", "application/json");
      response.end(await readFile(resolve(path)));
     } else if (request.url === "/__partial_sync_profile_result" && request.method === "POST") {
      const chunks: Buffer[] = []; let bytes = 0;
      for await (const chunk of request) {
       const value = Buffer.from(chunk); bytes += value.length;
       if (bytes > 16 * 1024 * 1024) throw new Error("Profile result exceeds artifact budget");
       chunks.push(value);
      }
      const artifact = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      const path = resolve(process.env.LIX_PARTIAL_PROFILE_RESULT ?? "partial-sync-profile.json");
      await writeFile(path, JSON.stringify(artifact, null, 2) + "\n");
      response.end("saved");
     } else next();
    } catch (error) { response.statusCode = 500; response.end(String(error)); }
   });
  },
 }],
 test: { include: ["tests/partial-sync.bench.browser.test.ts"], fileParallelism: false },
}));

config.test!.include = ["tests/partial-sync.bench.browser.test.ts"];
export default config;
