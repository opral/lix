import {
  HeadContent,
  Scripts,
  createRootRoute,
  useRouter,
} from "@tanstack/react-router";
import React from "react";
import appCss from "../styles.css?url";

const GA_MEASUREMENT_ID = "G-3GEP4W5688";

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
  }),

  notFoundComponent: NotFoundPage,
  shellComponent: RootDocument,
});

function GoogleAnalytics() {
  const router = useRouter();

  React.useEffect(() => {
    if (!import.meta.env.PROD) return;
    if ((window as any).__gaInitialized) return;
    (window as any).__gaInitialized = true;

    (window as any).dataLayer = (window as any).dataLayer || [];
    function gtag(...args: unknown[]) {
      (window as any).dataLayer.push(args);
    }
    (window as any).gtag = gtag;

    const script = document.createElement("script");
    script.async = true;
    script.src = `https://www.googletagmanager.com/gtag/js?id=${GA_MEASUREMENT_ID}`;
    document.head.appendChild(script);

    gtag("js", new Date());
    gtag("config", GA_MEASUREMENT_ID, { send_page_view: false });

    const sendPageView = (location: {
      href: string;
      pathname: string;
      search: string;
      hash: string;
    }) => {
      gtag("event", "page_view", {
        page_location: location.href,
        page_path: `${location.pathname}${location.search}${location.hash}`,
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
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <GoogleAnalytics />
        {children}
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
