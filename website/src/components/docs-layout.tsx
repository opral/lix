import { Link } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { Footer } from "./footer";
import { Header, MenuIcon } from "./header";

export type SidebarSection = {
  label: string;
  items: Array<{
    label: string;
    href: string;
    relativePath: string;
  }>;
};

export type PageTocItem = {
  id: string;
  label: string;
  level: number;
};

/**
 * Three-column documentation shell: sidebar, content, and "On this page" TOC.
 *
 * The sidebar is driven from the docs table of contents and highlights the
 * active entry based on the current doc relative path.
 *
 * @example
 * <DocsLayout
 *   toc={toc}
 *   sidebarSections={[
 *     { label: "Overview", items: [{ label: "What is Lix?", href: "/docs/what-is-lix", relativePath: "./what-is-lix.md" }] },
 *   ]}
 *   activeRelativePath="./what-is-lix.md"
 *   pageToc={[{ id: "intro", label: "Intro", level: 2 }]}
 * >
 *   <MarkdownPage html="<h1>Hello</h1>" markdown="# Hello" />
 * </DocsLayout>
 */
export function DocsLayout({
  sidebarSections,
  activeRelativePath,
  pageToc,
  children,
}: {
  sidebarSections: SidebarSection[];
  activeRelativePath?: string;
  pageToc?: PageTocItem[];
  children: React.ReactNode;
}) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const hasPageToc = Boolean(pageToc && pageToc.length > 0);
  const [activeTocId, setActiveTocId] = useState<string | null>(null);

  useEffect(() => {
    if (!pageToc || pageToc.length === 0) return;

    const headings = pageToc
      .map((item) => document.getElementById(item.id))
      .filter((node): node is HTMLElement => Boolean(node));

    if (headings.length === 0) return;

    const updateActiveHeading = () => {
      const activationOffset = 96;
      let activeHeading = headings[0];

      for (const heading of headings) {
        if (heading.getBoundingClientRect().top <= activationOffset) {
          activeHeading = heading;
        } else {
          break;
        }
      }

      setActiveTocId((current) =>
        current === activeHeading.id ? current : activeHeading.id,
      );
    };

    updateActiveHeading();
    window.addEventListener("scroll", updateActiveHeading, { passive: true });
    window.addEventListener("resize", updateActiveHeading);

    return () => {
      window.removeEventListener("scroll", updateActiveHeading);
      window.removeEventListener("resize", updateActiveHeading);
    };
  }, [pageToc]);

  const SidebarContent = () => (
    <nav
      aria-label="Documentation sidebar"
      className="flex flex-col gap-8 px-7 pb-16 pt-9"
    >
      {sidebarSections.map((section) => (
        <section key={section.label} className="flex flex-col gap-2.5">
          <span className="font-mono text-[11px] uppercase tracking-[0.09em] text-ink-faint">
            {section.label}
          </span>
          <div className="flex flex-col gap-[7px]">
            {section.items.map((item) => {
              const isActive = item.relativePath === activeRelativePath;
              return (
                <Link
                  key={item.href}
                  to={item.href}
                  onClick={() => setIsMobileMenuOpen(false)}
                  className={
                    isActive
                      ? "text-[13.5px] font-semibold text-cyan-deep"
                      : "text-[13.5px] text-ink-muted transition-colors hover:text-cyan-deep"
                  }
                >
                  {item.label}
                </Link>
              );
            })}
          </div>
        </section>
      ))}
    </nav>
  );

  return (
    <div className="min-h-screen bg-paper text-ink">
      <div className="sticky top-0 z-50">
        <Header />
        {/* Mobile menu bar - below header, above content */}
        <div className="border-b border-line bg-paper lg:hidden">
          <div className="mx-auto flex w-full max-w-[1376px] items-center px-8 py-2">
            <button
              onClick={() => setIsMobileMenuOpen(true)}
              className="flex items-center gap-2 text-sm font-medium text-ink-secondary"
              aria-label="Open menu"
            >
              <MenuIcon className="h-5 w-5" />
              <span>Menu</span>
            </button>
          </div>
        </div>
      </div>
      {/* Mobile sidebar overlay */}
      {isMobileMenuOpen && (
        <>
          <div
            className="fixed inset-0 z-40 bg-black/50 lg:hidden"
            onClick={() => setIsMobileMenuOpen(false)}
            aria-hidden="true"
          />
          <aside className="fixed inset-y-0 left-0 z-50 w-full overflow-y-auto border-r border-line bg-paper lg:hidden">
            <div className="sticky top-0 flex items-center justify-end bg-paper px-6 py-3">
              <button
                onClick={() => setIsMobileMenuOpen(false)}
                className="text-ink-muted hover:text-ink"
                aria-label="Close menu"
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="h-5 w-5"
                  aria-hidden="true"
                >
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>
            <SidebarContent />
          </aside>
        </>
      )}
      <div className="grid grid-cols-1 items-stretch lg:grid-cols-[264px_minmax(0,1fr)] xl:grid-cols-[264px_minmax(0,1fr)_224px]">
        <aside className="hidden border-r border-line lg:block">
          <div className="sticky top-[50px] max-h-[calc(100vh-50px)] overflow-y-auto">
            <SidebarContent />
          </div>
        </aside>

        <main className="min-w-0 px-6 pb-[88px] pt-12 sm:px-16">
          <div className="w-full max-w-[720px]">{children}</div>
        </main>

        {hasPageToc && (
          <aside className="hidden xl:block">
            <nav
              aria-label="On this page"
              className="sticky top-[50px] py-[52px] pl-2 pr-7"
            >
              <div className="flex flex-col gap-[9px] border-l border-line pl-4">
                <span className="mb-[3px] font-mono text-[11px] uppercase tracking-[0.09em] text-ink-faint">
                  On this page
                </span>
                {pageToc?.map((item) => {
                  const isActive = item.id === activeTocId;
                  return (
                    <a
                      key={item.id}
                      href={`#${item.id}`}
                      className={[
                        item.level > 2 ? "pl-3" : "",
                        isActive
                          ? "text-[13px] font-semibold text-cyan-deep"
                          : "text-[13px] text-ink-muted transition-colors hover:text-cyan-deep",
                      ].join(" ")}
                    >
                      {item.label}
                    </a>
                  );
                })}
              </div>
            </nav>
          </aside>
        )}
      </div>
      <Footer />
    </div>
  );
}
