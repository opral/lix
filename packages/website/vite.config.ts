import { defineConfig, loadEnv } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import viteTsConfigPaths from "vite-tsconfig-paths";
import tailwindcss from "@tailwindcss/vite";
import { pluginReadmeSync } from "./scripts/plugin-readme-sync";
import { githubStarsPlugin } from "./src/ssg/github-stars-plugin";
import { viteStaticCopy } from "vite-plugin-static-copy";

const config = defineConfig(({ mode, command }) => {
  const isTest = process.env.VITEST === "true" || mode === "test";
  const env = loadEnv(mode, process.cwd(), "");
  const githubToken =
    process.env.LIX_WEBSITE_GITHUB_TOKEN ?? env.LIX_WEBSITE_GITHUB_TOKEN;

  return {
    plugins: [
      pluginReadmeSync(),
      githubStarsPlugin({
        token: githubToken,
      }),
      // this is the plugin that enables path aliases
      viteTsConfigPaths({
        projects: ["./tsconfig.json"],
      }),
      tailwindcss(),
      !isTest &&
        viteStaticCopy({
          targets: [
            {
              src: "../../blog/**",
              dest: "../client/blog",
            },
          ],
          watch: command === "serve" ? { reloadPageOnChange: true } : undefined,
        }),
      tanstackStart({
        prerender: {
          enabled: true,
          autoSubfolderIndex: true,
          autoStaticPathsDiscovery: true,
          crawlLinks: true,
          concurrency: 8,
          retryCount: 2,
          retryDelay: 1000,
          maxRedirects: 5,
          failOnError: true,
        },
        sitemap: {
          enabled: true,
          host: "https://lix.dev",
        },
      }),
      viteReact(),
    ].filter(Boolean),
  };
});

export default config;
