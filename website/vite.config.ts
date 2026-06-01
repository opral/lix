import { defineConfig, loadEnv, type Plugin } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { githubStarsPlugin } from "./src/ssg/github-stars-plugin";
import { viteStaticCopy } from "vite-plugin-static-copy";
import path from "path";
import fs from "fs";
import type { ViteDevServer } from "vite";

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
          const filePath = path.resolve(__dirname, "../blog", assetPath);
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

/**
 * Keeps the docs route module graph in sync when root docs files are added or
 * removed while the dev server is already running.
 */
function docsContentWatchPlugin(): Plugin {
  const docsDir = path.resolve(__dirname, "../docs");
  const docsRouteFiles = [
    path.resolve(__dirname, "src/routes/docs/$slugId.tsx"),
    path.resolve(__dirname, "src/routes/docs/index.tsx"),
  ];

  const invalidateDocsRoutes = (server: ViteDevServer) => {
    for (const routeFile of docsRouteFiles) {
      const modules = server.moduleGraph.getModulesByFile(routeFile);
      if (!modules) continue;
      for (const module of modules) {
        server.moduleGraph.invalidateModule(module);
      }
    }
    server.ws.send({ type: "full-reload" });
  };

  const isDocsFile = (file: string) => {
    const normalizedFile = path.normalize(file);
    return normalizedFile.startsWith(docsDir + path.sep);
  };

  return {
    name: "docs-content-watch",
    configureServer(server) {
      server.watcher.add(docsDir);
      server.watcher.on("add", (file) => {
        if (isDocsFile(file)) invalidateDocsRoutes(server);
      });
      server.watcher.on("unlink", (file) => {
        if (isDocsFile(file)) invalidateDocsRoutes(server);
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
        allow: ["..", "."],
      },
    },
    resolve: {
      tsconfigPaths: true,
    },
    plugins: [
      command === "serve" && blogAssetsPlugin(),
      command === "serve" && docsContentWatchPlugin(),
      githubStarsPlugin({
        token: githubToken,
      }),
      tailwindcss(),
      command === "build" &&
        !isTest &&
        viteStaticCopy({
          targets: [
            {
              src: "../blog/**",
              dest: "../client",
            },
          ],
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
