import { defineConfig } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import viteTsConfigPaths from "vite-tsconfig-paths";
import tailwindcss from "@tailwindcss/vite";
import { pluginReadmeSync } from "./scripts/plugin-readme-sync";
import { githubStarsPlugin } from "./src/ssg/github-stars-plugin";

const config = defineConfig({
  plugins: [
    pluginReadmeSync(),
    githubStarsPlugin({
      token: process.env.LIX_WEBSITE_GITHUB_TOKEN,
    }),
    // this is the plugin that enables path aliases
    viteTsConfigPaths({
      projects: ["./tsconfig.json"],
    }),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
      },
      sitemap: {
        host: "https://lix.dev",
      },
    }),
    viteReact(),
  ],
});

export default config;
