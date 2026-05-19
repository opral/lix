const alertTitles: Record<string, string> = {
  note: "Note",
  tip: "Tip",
  important: "Important",
  warning: "Warning",
  caution: "Caution",
};

/**
 * Normalizes HTML emitted by @opral/markdown-wc for the docs shell.
 */
export function normalizeMarkdownHtml(html: string): string {
  return wrapTables(
    normalizeDoubleEncodedEntities(normalizeGithubAlerts(html)),
  );
}

function normalizeGithubAlerts(html: string): string {
  return html.replace(
    /<blockquote data-mwc-alert="([^"]+)">([\s\S]*?)<\/blockquote>/g,
    (_match, rawKind: string, body: string) => {
      const kind = rawKind.toLowerCase();
      const title = alertTitles[kind] ?? kind;
      const normalizedBody = body
        .replace(
          /<span data-mwc-alert-marker="">\s*\[![^\]]+\]\s*<\/span>\s*/g,
          "",
        )
        .trim();

      return `<div class="callout ${escapeAttribute(kind)}"><p class="callout-title">${escapeHtml(title)}</p>${normalizedBody}</div>`;
    },
  );
}

function wrapTables(html: string): string {
  return html.replace(
    /(^|<\/(?:p|pre|div|blockquote|ul|ol|h[1-6])>\s*)<table\b([\s\S]*?<\/table>)/g,
    (_match, prefix: string, table: string) =>
      `${prefix}<div class="table-wrapper"><table${table}</div>`,
  );
}

function normalizeDoubleEncodedEntities(html: string): string {
  let normalized = html;
  for (let i = 0; i < 2; i += 1) {
    normalized = normalized.replace(
      /&amp;((?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z][a-zA-Z0-9]+);)/g,
      "&$1",
    );
  }
  return normalized;
}

function escapeHtml(input: string): string {
  return input
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function escapeAttribute(input: string): string {
  return escapeHtml(input).replace(/"/g, "&quot;");
}
