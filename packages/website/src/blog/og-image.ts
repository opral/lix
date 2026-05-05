import { buildCanonicalUrl } from "../lib/seo";

export function resolveOgImageUrl(value: string, folderName: string): string {
  if (isAbsoluteUrl(value)) return value;
  const base = `${buildCanonicalUrl(`/blog/${folderName}`)}/`;
  return new URL(value, base).toString();
}

export function resolveBlogAssetPath(value: string, folderName: string): string {
  if (isAbsoluteUrl(value)) return value;
  if (value.startsWith("/")) return value;
  const normalized = value.replace(/^\.\//, "");
  return `/blog/${folderName}/${normalized}`;
}

function isAbsoluteUrl(value: string): boolean {
  return /^[a-z][a-z0-9+.-]*:/.test(value);
}
