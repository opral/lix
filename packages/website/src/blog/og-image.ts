import { buildCanonicalUrl } from "../lib/seo";

export function resolveOgImageUrl(value: string, folderName: string): string {
  if (isAbsoluteUrl(value)) return value;
  const base = buildCanonicalUrl(`/blog/${folderName}/`);
  return new URL(value, base).toString();
}

function isAbsoluteUrl(value: string): boolean {
  return /^[a-z][a-z0-9+.-]*:/.test(value);
}
