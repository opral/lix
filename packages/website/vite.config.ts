import { defineConfig, loadEnv, type Plugin } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import viteTsConfigPaths from "vite-tsconfig-paths";
import tailwindcss from "@tailwindcss/vite";
import { pluginReadmeSync } from "./scripts/plugin-readme-sync";
import { githubStarsPlugin } from "./src/ssg/github-stars-plugin";
import { viteStaticCopy } from "vite-plugin-static-copy";
import path from "path";
import fs from "fs";

const mimeTypes: Record<string, string> = {
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".ico": "image/x-icon",
};

/**
 * Serves blog assets from the blog directory in dev mode.
 */
function blogAssetsPlugin(): Plugin {
  return {
    name: "blog-assets",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        if (req.url?.startsWith("/blog/") && !req.url.endsWith("/")) {
          const assetPath = req.url.replace("/blog/", "");
          const filePath = path.resolve(__dirname, "../../blog", assetPath);
          if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
            const ext = path.extname(filePath).toLowerCase();
            const contentType = mimeTypes[ext] || "application/octet-stream";
            res.setHeader("Content-Type", contentType);
            return res.end(fs.readFileSync(filePath));
          }
        }
        next();
      });
    },
  };
}

const config = defineConfig(({ mode, command }) => {
  const isTest = process.env.VITEST === "true" || mode === "test";
  const env = loadEnv(mode, process.cwd(), "");
  const githubToken =
    process.env.LIX_WEBSITE_GITHUB_TOKEN ?? env.LIX_WEBSITE_GITHUB_TOKEN;

  return {
    server: {
      fs: {
        allow: ["../..", "."],
      },
    },
    plugins: [
      command === "serve" && blogAssetsPlugin(),
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
