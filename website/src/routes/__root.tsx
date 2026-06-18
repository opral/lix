import {
  HeadContent,
  Scripts,
  createRootRoute,
  useRouter,
} from "@tanstack/react-router";
import React from "react";
import { PostHogProvider } from "posthog-js/react";
import appCss from "../styles.css?url";

const GA_MEASUREMENT_ID = "G-1M7SY9LBT7";
const posthogOptions = {
  api_host: import.meta.env.VITE_PUBLIC_POSTHOG_HOST,
  defaults: "2025-11-30",
} as const;

const googleAnalyticsScripts = import.meta.env.PROD
  ? [
      {
        async: true,
        src: `https://www.googletagmanager.com/gtag/js?id=${GA_MEASUREMENT_ID}`,
      },
      {
        children: `
          window.dataLayer = window.dataLayer || [];
          function gtag(){window.dataLayer.push(arguments);}
          window.gtag = gtag;
          gtag('js', new Date());
          gtag('config', '${GA_MEASUREMENT_ID}', { send_page_view: false });
        `,
      },
    ]
  : [];

export const Route = createRootRoute({
  head: () => ({
    meta: [
      {
        charSet: "utf-8",
      },
      {
        name: "viewport",
        content: "width=device-width, initial-scale=1",
      },
      {
        title: "Lix",
      },
      {
        name: "theme-color",
        content: "#ffffff",
      },
      {
        name: "robots",
        content: "index, follow",
      },
    ],
    links: [
      {
        rel: "stylesheet",
        href: appCss,
      },
      {
        rel: "icon",
        type: "image/svg+xml",
        href: "/favicon.svg",
      },
      {
        rel: "manifest",
        href: "/manifest.json",
      },
    ],
    scripts: [
      {
        type: "application/ld+json",
        children: JSON.stringify({
          "@context": "https://schema.org",
          "@type": "Organization",
          name: "Lix",
          url: "https://lix.dev",
          logo: "https://lix.dev/icon.svg",
          sameAs: [
            "https://github.com/opral/lix",
            "https://x.com/lixCCS",
            "https://discord.gg/gdMPPWy57R",
          ],
        }),
      },
      ...googleAnalyticsScripts,
    ],
  }),

  notFoundComponent: NotFoundPage,
  shellComponent: RootDocument,
});

function GoogleAnalyticsPageViews() {
  const router = useRouter();

  React.useEffect(() => {
    if (!import.meta.env.PROD) return;
    const gtag = (window as any).gtag;
    if (typeof gtag !== "function") return;

    const sendPageView = (location: {
      href: string;
      publicHref?: string;
      pathname: string;
      search: string;
      hash: string;
    }) => {
      const publicPath =
        location.publicHref ??
        `${location.pathname}${location.search}${location.hash}`;

      gtag("event", "page_view", {
        page_location: new URL(publicPath, window.location.origin).href,
        page_path: publicPath,
        page_title: document.title,
      });
    };

    sendPageView(router.history.location);
    const unsubscribe = router.history.subscribe(({ location }) => {
      sendPageView(location);
    });

    return () => {
      unsubscribe();
    };
  }, []);

  return null;
}

function RootDocument({ children }: { children: React.ReactNode }) {
  // Only render PostHogProvider on the client side to avoid hydration mismatches.
  // PostHog is a client-side only library and will cause React error #418 if
  // rendered during SSR.
  const [isMounted, setIsMounted] = React.useState(false);

  React.useEffect(() => {
    setIsMounted(true);
  }, []);

  const appContent =
    import.meta.env.PROD &&
    isMounted &&
    import.meta.env.VITE_PUBLIC_POSTHOG_KEY ? (
      <PostHogProvider
        apiKey={import.meta.env.VITE_PUBLIC_POSTHOG_KEY}
        options={posthogOptions}
      >
        {children}
      </PostHogProvider>
    ) : (
      children
    );

  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <GoogleAnalyticsPageViews />
        {appContent}
        <Scripts />
      </body>
    </html>
  );
}

/**
 * Fallback UI for unmatched routes.
 *
 * @example
 * <NotFoundPage />
 */
function NotFoundPage() {
  return (
    <div className="mx-auto flex min-h-[60vh] max-w-3xl flex-col justify-center px-6 py-16 text-slate-900">
      <p className="text-xs font-semibold uppercase tracking-[0.35em] text-slate-500">
        404
      </p>
      <h1 className="mt-4 text-3xl font-semibold leading-tight sm:text-4xl">
        Page not found
      </h1>
      <p className="mt-3 text-base text-slate-600">
        The page you are looking for does not exist.
      </p>
    </div>
  );
}
