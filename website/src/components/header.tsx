import { Link, useRouterState } from "@tanstack/react-router";
import { getGithubStars } from "../github-stars-cache";

export type HeaderWidth = "narrow" | "wide";

const widthClass: Record<HeaderWidth, string> = {
  narrow: "max-w-[1100px]",
  wide: "max-w-[1376px]",
};

const navLinks = [
  { href: "/docs/what-is-lix", label: "Docs", activePrefix: "/docs" },
  { href: "/plugins", label: "Plugins", activePrefix: "/plugins" },
  { href: "/blog", label: "Blog", activePrefix: "/blog" },
];

const socialLinks = [
  { href: "https://discord.gg/gdMPPWy57R", label: "Discord" },
  { href: "https://x.com/lixCCS", label: "X" },
];

/**
 * Hamburger menu icon for mobile navigation.
 *
 * @example
 * <MenuIcon className="h-5 w-5" />
 */
export const MenuIcon = ({ className = "" }) => (
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
    <line x1="3" y1="6" x2="21" y2="6" />
    <line x1="3" y1="12" x2="21" y2="12" />
    <line x1="3" y1="18" x2="21" y2="18" />
  </svg>
);

function formatStars(count: number) {
  if (count >= 1000) {
    return `${(count / 1000).toFixed(1).replace(/\.0$/, "")}k`;
  }
  return count.toString();
}

/**
 * Site header with wordmark, navigation, and social links.
 *
 * @example
 * <Header width="narrow" />
 */
export function Header({ width = "wide" }: { width?: HeaderWidth }) {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const githubStars = getGithubStars("opral/lix");

  const isActive = (href: string, activePrefix?: string) => {
    const candidate = activePrefix ?? href;
    const normalized = candidate === "/" ? "/" : candidate.replace(/\/$/, "");
    if (normalized === "/") return pathname === "/";
    return pathname === normalized || pathname.startsWith(`${normalized}/`);
  };

  return (
    <header className="sticky top-0 z-50 border-b border-line bg-[rgba(251,250,247,0.92)] backdrop-blur-[8px]">
      <div
        className={`mx-auto flex h-[50px] w-full items-center justify-between gap-5 px-8 ${widthClass[width]}`}
      >
        <Link
          to="/"
          className="text-[21px] font-bold leading-none tracking-[-0.03em] text-cyan-bright"
          aria-label="lix home"
        >
          lix
        </Link>
        <nav className="flex items-center gap-[22px]">
          {navLinks.map(({ href, label, activePrefix }) => (
            <Link
              key={href}
              to={href}
              className={
                isActive(href, activePrefix)
                  ? "hidden text-[13.5px] font-semibold text-cyan-deep sm:block"
                  : "hidden text-[13.5px] font-medium text-ink-secondary transition-colors hover:text-cyan-deep sm:block"
              }
              aria-current={isActive(href, activePrefix) ? "page" : undefined}
            >
              {label}
            </Link>
          ))}
          <span
            className="hidden h-4 w-px bg-line-strong sm:block"
            aria-hidden="true"
          />
          {socialLinks.map(({ href, label }) => (
            <a
              key={label}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="hidden text-[13.5px] font-medium text-ink-muted transition-colors hover:text-cyan-deep sm:block"
            >
              {label}
            </a>
          ))}
          <a
            href="https://github.com/opral/lix"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-[7px] text-[13.5px] font-medium text-ink-secondary transition-colors hover:text-cyan-deep"
          >
            GitHub
            {githubStars !== null && (
              <span
                className="flex items-center gap-1 rounded border border-line-strong px-1.5 py-0.5 font-mono text-xs font-normal leading-none text-ink-muted"
                title={`${githubStars.toLocaleString()} GitHub stars`}
                aria-label={`${githubStars.toLocaleString()} GitHub stars`}
              >
                <svg
                  viewBox="0 0 16 16"
                  fill="currentColor"
                  className="h-3 w-3"
                  aria-hidden="true"
                >
                  <path d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.75.75 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.818 6.374a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25z" />
                </svg>
                {formatStars(githubStars)}
              </span>
            )}
          </a>
        </nav>
      </div>
    </header>
  );
}
