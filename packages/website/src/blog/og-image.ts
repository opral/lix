import { buildCanonicalUrl } from "../lib/seo";

export function resolveOgImageUrl(value: string, slug: string): string {
  if (isAbsoluteUrl(value)) return value;
  const base = buildCanonicalUrl(`/blog/${slug}/`);
  return new URL(value, base).toString();
}

function isAbsoluteUrl(value: string): boolean {
  return /^[a-z][a-z0-9+.-]*:/.test(value);
}
