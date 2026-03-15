import fs from "node:fs";
import path from "node:path";

const SITE_URL = "https://lix.dev";
const SITEMAP_PATH = path.resolve("dist/client/sitemap.xml");
const ALIAS_URLS = new Set([`${SITE_URL}/docs`, `${SITE_URL}/guide`]);

function isAliasUrl(url) {
  return ALIAS_URLS.has(url) || url.startsWith(`${SITE_URL}/guide/`);
}

if (fs.existsSync(SITEMAP_PATH)) {
  const sitemap = fs.readFileSync(SITEMAP_PATH, "utf8");
  const filtered = sitemap.replace(
    /<url>\s*<loc>([^<]+)<\/loc>[\s\S]*?<\/url>/g,
    (match, loc) => (isAliasUrl(loc) ? "" : match),
  );
  fs.writeFileSync(SITEMAP_PATH, filtered.trimEnd().concat("\n"));
}
