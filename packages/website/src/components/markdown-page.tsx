import { useEffect, useState } from "react";
import { splitTitleFromHtml } from "../lib/seo";

type CopyStatus = "idle" | "copied";

/**
 * Copy icon used for the markdown copy button.
 *
 * @example
 * <CopyMarkdownIcon className="h-4 w-4" />
 */
const CopyMarkdownIcon = ({ className = "" }: { className?: string }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    aria-hidden="true"
  >
    <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);

/**
 * Check icon shown on copy success.
 *
 * @example
 * <CopyCheckIcon className="h-4 w-4" />
 */
const CopyCheckIcon = ({ className = "" }: { className?: string }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    width="24"
    height="24"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={className}
    aria-hidden="true"
  >
    <path d="M20 6 9 17l-5-5" />
  </svg>
);

/**
 * Renders pre-parsed markdown HTML inside the docs layout.
 *
 * @example
 * <MarkdownPage html="<h1>Hi from Lix</h1>" markdown="# Hi from Lix" />
 */
export function MarkdownPage({
  html,
  markdown,
  imports,
}: {
  html: string;
  markdown?: string;
  imports?: string[];
}) {
  const [copyStatus, setCopyStatus] = useState<CopyStatus>("idle");

  useEffect(() => {
    // @ts-expect-error - JS-only module
    import("./markdown-page.interactive.js");
  }, [html]);

  useEffect(() => {
    if (!imports || imports.length === 0) return;

    for (const url of imports) {
      if (!url) continue;
      const existing = document.querySelector(
        `script[data-mdwc-import="${url}"]`,
      );
      if (existing) continue;

      const script = document.createElement("script");
      script.type = "module";
      script.src = url;
      script.setAttribute("data-mdwc-import", url);
      document.head.appendChild(script);
    }
  }, [imports]);

  const { title, body } = splitTitleFromHtml(html);

  const handleCopy = () => {
    if (!markdown) return;
    const clipboard = navigator?.clipboard;
    if (!clipboard?.writeText) return;

    clipboard.writeText(markdown).then(() => {
      setCopyStatus("copied");
      window.setTimeout(() => setCopyStatus("idle"), 2000);
    });
  };

  return (
    <article className="markdown-wc-body">
      {title && (
        <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <h1 className="text-[28px] font-semibold leading-10 tracking-[-0.02em] text-slate-900">
            {title}
          </h1>
          <button
            type="button"
            onClick={handleCopy}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-900"
            aria-label="Copy markdown"
          >
            {copyStatus === "copied" ? (
              <CopyCheckIcon className="h-4 w-4 animate-[copy-arrow_0.4s_ease-out]" />
            ) : (
              <CopyMarkdownIcon className="h-4 w-4" />
            )}
            {copyStatus === "copied" ? "Copied" : "Copy Markdown"}
          </button>
        </div>
      )}
      <div dangerouslySetInnerHTML={{ __html: body }} />
    </article>
  );
}
