import { getGithubStars } from "../github-stars-cache";

const footerLinks = [
  { href: "/docs", label: "Docs", emoji: "📘" },
  { href: "/blog", label: "Blog", emoji: "📝" },
  { href: "/rfc", label: "RFCs", emoji: "📄" },
];

export function Footer() {
  const githubStars = getGithubStars("opral/lix");

  const formatStars = (count: number) => {
    if (count >= 1000) {
      return `${(count / 1000).toFixed(1).replace(/\.0$/, "")}k`;
    }
    return count.toString();
  };

  return (
    <footer className="bg-white">
      <div className="border-t border-gray-200">
        <div className="flex flex-col gap-3 px-6 py-10 sm:flex-row sm:justify-center sm:gap-8">
          {footerLinks.map((link) => (
            <a
              key={link.href}
              href={link.href}
              className="inline-flex items-center justify-center gap-2 text-sm font-medium text-gray-500 transition-colors hover:text-gray-900"
            >
              <span aria-hidden>{link.emoji}</span>
              {link.label}
            </a>
          ))}
          <a
            href="https://discord.gg/gdMPPWy57R"
            className="inline-flex items-center justify-center gap-2 text-sm font-medium text-gray-500 transition-colors hover:text-gray-900"
          >
            <span aria-hidden>💬</span>
            Discord
            <img
              src="https://img.shields.io/discord/897438559458430986?label=%20&color=f3f4f6&style=flat-square"
              alt="Discord online members"
              className="h-4"
            />
          </a>
          <a
            href="https://github.com/opral/lix"
            className="inline-flex items-center justify-center gap-2 text-sm font-medium text-gray-500 transition-colors hover:text-gray-900"
            title={
              githubStars
                ? `${githubStars.toLocaleString()} GitHub stars`
                : "Star us on GitHub"
            }
          >
            <span aria-hidden>⭐</span>
            Star on GitHub
            {githubStars !== null && (
              <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-600">
                {formatStars(githubStars)}
              </span>
            )}
          </a>
        </div>
      </div>
    </footer>
  );
}
