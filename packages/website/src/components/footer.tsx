const footerLinks = [
  { href: "/docs", label: "Go to Docs", emoji: "📘" },
  { href: "https://discord.gg/gdMPPWy57R", label: "Join Discord", emoji: "💬" },
  {
    href: "https://github.com/opral/lix-sdk",
    label: "Visit GitHub",
    emoji: "🐙",
  },
  {
    href: "https://opral.substack.com/t/lix",
    label: "Read Substack",
    emoji: "→",
  },
];

export function Footer() {
  return (
    <footer className="bg-white">
      <div className="flex flex-col gap-3 px-6 py-14 sm:flex-row sm:justify-center sm:gap-6">
        {footerLinks.map((link) => (
          <a
            key={link.href}
            href={link.href}
            className="inline-flex items-center justify-center gap-2 text-base font-medium text-gray-700 transition-colors hover:text-[#0692B6]"
          >
            <span aria-hidden>{link.emoji}</span>
            {link.label}
          </a>
        ))}
      </div>
    </footer>
  );
}
