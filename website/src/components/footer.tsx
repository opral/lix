import type { HeaderWidth } from "./header";

const widthClass: Record<HeaderWidth, string> = {
  narrow: "max-w-[1100px]",
  wide: "max-w-[1376px]",
};

const footerLinks = [
  { href: "https://github.com/opral/lix", label: "GitHub" },
  { href: "https://x.com/lixCCS", label: "X" },
  { href: "https://discord.gg/gdMPPWy57R", label: "Discord" },
];

/**
 * Site footer with attribution and social links.
 *
 * @example
 * <Footer width="narrow" />
 */
export function Footer({ width = "wide" }: { width?: HeaderWidth }) {
  return (
    <footer className="border-t border-line">
      <div
        className={`mx-auto flex w-full flex-wrap items-center justify-between gap-6 px-8 py-7 ${widthClass[width]}`}
      >
        <span className="text-[13.5px] text-ink-faint">
          Lix is built by{" "}
          <a
            href="https://opral.com"
            target="_blank"
            rel="noopener noreferrer"
            className="text-ink-muted underline decoration-[#DDDBD3] underline-offset-[3px] transition-colors hover:text-cyan-deep"
          >
            Opral
          </a>
          .
        </span>
        <div className="flex gap-6">
          {footerLinks.map(({ href, label }) => (
            <a
              key={label}
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-[13.5px] text-ink-muted transition-colors hover:text-cyan-deep"
            >
              {label}
            </a>
          ))}
        </div>
      </div>
    </footer>
  );
}
