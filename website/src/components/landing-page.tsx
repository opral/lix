import { useState } from "react";
import { getGithubStars } from "../github-stars-cache";
import { DownloadsChart, coarseWeeklyDownloads } from "./downloads-chart";
import { Footer } from "./footer";
import { Header } from "./header";

const INSTALL_COMMAND = "npm install @lix-js/sdk";

const JsLogo = ({ className = "" }) => (
  <svg viewBox="0 0 24 24" className={className} aria-hidden="true">
    <rect width="24" height="24" rx="3" fill="#F7DF1E" />
    <text
      x="12"
      y="17"
      textAnchor="middle"
      fontSize="11"
      fontWeight="bold"
      fill="#000"
      fontFamily="sans-serif"
    >
      JS
    </text>
  </svg>
);

const PythonLogo = ({ className = "" }) => (
  <svg viewBox="16 16 32 32" className={className} aria-hidden="true">
    <path
      fill="#387EB8"
      d="M31.885 16c-8.124 0-7.617 3.523-7.617 3.523l.01 3.65h7.752v1.095H21.197S16 23.678 16 31.876c0 8.196 4.537 7.906 4.537 7.906h2.708v-3.804s-.146-4.537 4.465-4.537h7.688s4.32.07 4.32-4.175v-7.019S40.374 16 31.885 16zm-4.275 2.454a1.394 1.394 0 1 1 0 2.79 1.393 1.393 0 0 1-1.395-1.395c0-.771.624-1.395 1.395-1.395z"
    />
    <path
      fill="#FFC331"
      d="M32.115 47.833c8.124 0 7.617-3.523 7.617-3.523l-.01-3.65H31.97v-1.095h10.832S48 40.155 48 31.958c0-8.197-4.537-7.906-4.537-7.906h-2.708v3.803s.146 4.537-4.465 4.537h-7.688s-4.32-.07-4.32 4.175v7.019s-.656 4.247 7.833 4.247zm4.275-2.454a1.393 1.393 0 0 1-1.395-1.395 1.394 1.394 0 1 1 1.395 1.395z"
    />
  </svg>
);

const RustLogo = ({ className = "" }) => (
  <svg viewBox="0 0 224 224" className={className} aria-hidden="true">
    <path
      fill="#CE422B"
      d="M218.46 109.358l-9.062-5.614c-.076-.882-.162-1.762-.258-2.642l7.803-7.265a3.107 3.107 0 00.933-2.89 3.093 3.093 0 00-1.967-2.312l-9.97-3.715c-.25-.863-.512-1.72-.781-2.58l6.214-8.628a3.114 3.114 0 00-.592-4.263 3.134 3.134 0 00-1.431-.637l-10.507-1.709a80.869 80.869 0 00-1.263-2.353l4.417-9.7a3.12 3.12 0 00-.243-3.035 3.106 3.106 0 00-2.705-1.385l-10.671.372a85.152 85.152 0 00-1.685-2.044l2.456-10.381a3.125 3.125 0 00-3.762-3.763l-10.384 2.456a88.996 88.996 0 00-2.047-1.684l.373-10.671a3.11 3.11 0 00-1.385-2.704 3.127 3.127 0 00-3.034-.246l-9.681 4.417c-.782-.429-1.567-.854-2.353-1.265l-1.713-10.506a3.098 3.098 0 00-1.887-2.373 3.108 3.108 0 00-3.014.35l-8.628 6.213c-.85-.27-1.703-.53-2.56-.778l-3.716-9.97a3.111 3.111 0 00-2.311-1.97 3.134 3.134 0 00-2.89.933l-7.266 7.802a93.746 93.746 0 00-2.643-.258l-5.614-9.082A3.125 3.125 0 00111.97 4c-1.09 0-2.085.56-2.642 1.478l-5.615 9.081a93.32 93.32 0 00-2.642.259l-7.266-7.802a3.13 3.13 0 00-2.89-.933 3.106 3.106 0 00-2.312 1.97l-3.715 9.97c-.857.247-1.71.506-2.56.778L73.7 12.588a3.101 3.101 0 00-3.014-.35A3.127 3.127 0 0068.8 14.61l-1.713 10.506c-.79.41-1.575.832-2.353 1.265l-9.681-4.417a3.125 3.125 0 00-4.42 2.95l.372 10.67c-.69.553-1.373 1.115-2.048 1.685l-10.383-2.456a3.143 3.143 0 00-2.93.832 3.124 3.124 0 00-.833 2.93l2.436 10.383a93.897 93.897 0 00-1.68 2.043l-10.672-.372a3.138 3.138 0 00-2.704 1.385 3.126 3.126 0 00-.246 3.035l4.418 9.7c-.43.779-.855 1.563-1.266 2.353l-10.507 1.71a3.097 3.097 0 00-2.373 1.886 3.117 3.117 0 00.35 3.013l6.214 8.628a89.12 89.12 0 00-.78 2.58l-9.97 3.715a3.117 3.117 0 00-1.035 5.202l7.803 7.265c-.098.879-.184 1.76-.258 2.642l-9.062 5.614A3.122 3.122 0 004 112.021c0 1.092.56 2.084 1.478 2.642l9.062 5.614c.074.882.16 1.762.258 2.642l-7.803 7.265a3.117 3.117 0 001.034 5.201l9.97 3.716a110 110 0 00.78 2.58l-6.212 8.627a3.112 3.112 0 00.6 4.27c.419.33.916.547 1.443.63l10.507 1.709c.407.792.83 1.576 1.265 2.353l-4.417 9.68a3.126 3.126 0 002.95 4.42l10.65-.374c.553.69 1.115 1.372 1.685 2.047l-2.435 10.383a3.09 3.09 0 00.831 2.91 3.117 3.117 0 002.931.83l10.384-2.436a82.268 82.268 0 002.047 1.68l-.371 10.671a3.11 3.11 0 001.385 2.704 3.125 3.125 0 003.034.241l9.681-4.416c.779.432 1.563.854 2.353 1.265l1.713 10.505a3.147 3.147 0 001.887 2.395 3.111 3.111 0 003.014-.349l8.628-6.213c.853.271 1.71.535 2.58.783l3.716 9.969a3.112 3.112 0 002.312 1.967 3.112 3.112 0 002.89-.933l7.266-7.802c.877.101 1.761.186 2.642.264l5.615 9.061a3.12 3.12 0 002.642 1.478 3.165 3.165 0 002.663-1.478l5.614-9.061c.884-.078 1.765-.163 2.643-.264l7.265 7.802a3.106 3.106 0 002.89.933 3.105 3.105 0 002.312-1.967l3.716-9.969c.863-.248 1.719-.512 2.58-.783l8.629 6.213a3.12 3.12 0 004.9-2.045l1.713-10.506c.793-.411 1.577-.838 2.353-1.265l9.681 4.416a3.13 3.13 0 003.035-.241 3.126 3.126 0 001.385-2.704l-.372-10.671a81.794 81.794 0 002.046-1.68l10.383 2.436a3.123 3.123 0 003.763-3.74l-2.436-10.382a84.588 84.588 0 001.68-2.048l10.672.374a3.104 3.104 0 002.704-1.385 3.118 3.118 0 00.244-3.035l-4.417-9.68c.43-.779.852-1.563 1.263-2.353l10.507-1.709a3.08 3.08 0 002.373-1.886 3.11 3.11 0 00-.35-3.014l-6.214-8.627c.272-.857.532-1.717.781-2.58l9.97-3.716a3.109 3.109 0 001.967-2.311 3.107 3.107 0 00-.933-2.89l-7.803-7.265c.096-.88.182-1.761.258-2.642l9.062-5.614a3.11 3.11 0 001.478-2.642 3.157 3.157 0 00-1.476-2.663h-.064zm-60.687 75.337c-3.468-.747-5.656-4.169-4.913-7.637a6.412 6.412 0 017.617-4.933c3.468.741 5.676 4.169 4.933 7.637a6.414 6.414 0 01-7.617 4.933h-.02zm-3.076-20.847c-3.158-.677-6.275 1.334-6.936 4.5l-3.22 15.026c-9.929 4.5-21.055 7.018-32.614 7.018-11.89 0-23.12-2.622-33.234-7.328l-3.22-15.026c-.677-3.158-3.778-5.18-6.936-4.499l-13.273 2.848a80.222 80.222 0 01-6.853-8.091h64.61c.731 0 1.218-.132 1.218-.797v-22.91c0-.665-.487-.797-1.218-.797H94.133v-14.469h20.415c1.864 0 9.97.533 12.551 10.898.811 3.179 2.601 13.54 3.818 16.863 1.214 3.715 6.152 11.146 11.415 11.146h32.202c.365 0 .755-.041 1.166-.116a80.56 80.56 0 01-7.307 8.587l-13.583-2.911-.113.058zm-89.38 20.537a6.407 6.407 0 01-7.617-4.933c-.74-3.467 1.462-6.894 4.934-7.637a6.417 6.417 0 017.617 4.933c.74 3.468-1.464 6.894-4.934 7.637zm-24.564-99.28a6.438 6.438 0 01-3.261 8.484c-3.241 1.438-7.019-.025-8.464-3.261-1.445-3.237.025-7.039 3.262-8.483a6.416 6.416 0 018.463 3.26zM33.22 102.94l13.83-6.15c2.952-1.311 4.294-4.769 2.972-7.72l-2.848-6.44H58.36v50.362h-22.5a79.158 79.158 0 01-3.014-21.672c0-2.869.155-5.697.452-8.483l-.08.103zm60.687-4.892v-14.86h26.629c1.376 0 9.722 1.59 9.722 7.822 0 5.18-6.399 7.038-11.663 7.038h-24.77.082zm96.811 13.375c0 1.973-.072 3.922-.216 5.862h-8.113c-.811 0-1.137.532-1.137 1.327v3.715c0 8.752-4.934 10.671-9.268 11.146-4.129.464-8.691-1.726-9.248-4.252-2.436-13.684-6.482-16.595-12.881-21.672 7.948-5.036 16.204-12.487 16.204-22.498 0-10.753-7.369-17.523-12.385-20.847-7.059-4.644-14.862-5.572-16.968-5.572H52.899c11.374-12.673 26.835-21.673 44.174-24.975l9.887 10.361a5.849 5.849 0 008.278.19l11.064-10.568c23.119 4.314 42.729 18.721 54.082 38.598l-7.576 17.09c-1.306 2.951.027 6.419 2.973 7.72l14.573 6.48c.255 2.607.383 5.224.384 7.843l-.021.052zM106.912 24.94a6.398 6.398 0 019.062.209 6.437 6.437 0 01-.213 9.082 6.396 6.396 0 01-9.062-.21 6.436 6.436 0 01.213-9.083v.002zm75.137 60.476a6.402 6.402 0 018.463-3.26 6.425 6.425 0 013.261 8.482 6.402 6.402 0 01-8.463 3.261 6.425 6.425 0 01-3.261-8.483z"
    />
  </svg>
);

const GoLogo = ({ className = "" }) => (
  <svg viewBox="0 0 207 78" className={className} aria-hidden="true">
    <g fill="#00ADD8" fillRule="evenodd">
      <path d="m16.2 24.1c-.4 0-.5-.2-.3-.5l2.1-2.7c.2-.3.7-.5 1.1-.5h35.7c.4 0 .5.3.3.6l-1.7 2.6c-.2.3-.7.6-1 .6z" />
      <path d="m1.1 33.3c-.4 0-.5-.2-.3-.5l2.1-2.7c.2-.3.7-.5 1.1-.5h45.6c.4 0 .6.3.5.6l-.8 2.4c-.1.4-.5.6-.9.6z" />
      <path d="m25.3 42.5c-.4 0-.5-.3-.3-.6l1.4-2.5c.2-.3.6-.6 1-.6h20c.4 0 .6.3.6.7l-.2 2.4c0 .4-.4.7-.7.7z" />
      <g transform="translate(55)">
        <path d="m74.1 22.3c-6.3 1.6-10.6 2.8-16.8 4.4-1.5.4-1.6.5-2.9-1-1.5-1.7-2.6-2.8-4.7-3.8-6.3-3.1-12.4-2.2-18.1 1.5-6.8 4.4-10.3 10.9-10.2 19 .1 8 5.6 14.6 13.5 15.7 6.8.9 12.5-1.5 17-6.6.9-1.1 1.7-2.3 2.7-3.7-3.6 0-8.1 0-19.3 0-2.1 0-2.6-1.3-1.9-3 1.3-3.1 3.7-8.3 5.1-10.9.3-.6 1-1.6 2.5-1.6h36.4c-.2 2.7-.2 5.4-.6 8.1-1.1 7.2-3.8 13.8-8.2 19.6-7.2 9.5-16.6 15.4-28.5 17-9.8 1.3-18.9-.6-26.9-6.6-7.4-5.6-11.6-13-12.7-22.2-1.3-10.9 1.9-20.7 8.5-29.3 7.1-9.3 16.5-15.2 28-17.3 9.4-1.7 18.4-.6 26.5 4.9 5.3 3.5 9.1 8.3 11.6 14.1.6.9.2 1.4-1 1.7z" />
        <path
          d="m107.2 77.6c-9.1-.2-17.4-2.8-24.4-8.8-5.9-5.1-9.6-11.6-10.8-19.3-1.8-11.3 1.3-21.3 8.1-30.2 7.3-9.6 16.1-14.6 28-16.7 10.2-1.8 19.8-.8 28.5 5.1 7.9 5.4 12.8 12.7 14.1 22.3 1.7 13.5-2.2 24.5-11.5 33.9-6.6 6.7-14.7 10.9-24 12.8-2.7.5-5.4.6-8 .9zm23.8-40.4c-.1-1.3-.1-2.3-.3-3.3-1.8-9.9-10.9-15.5-20.4-13.3-9.3 2.1-15.3 8-17.5 17.4-1.8 7.8 2 15.7 9.2 18.9 5.5 2.4 11 2.1 16.3-.6 7.9-4.1 12.2-10.5 12.7-19.1z"
          fillRule="nonzero"
        />
      </g>
    </g>
  </svg>
);

const sdkLanguages = [
  {
    label: "Rust",
    href: "https://crates.io/crates/lix",
    title: "The Rust SDK on crates.io.",
    Logo: RustLogo,
  },
  {
    label: "Python",
    href: "https://github.com/opral/lix/issues/373",
    title: "The Python SDK is planned. Upvote the issue on GitHub.",
    Logo: PythonLogo,
  },
  {
    label: "Go",
    href: "https://github.com/opral/lix/issues/370",
    title: "The Go SDK is planned. Upvote the issue on GitHub.",
    Logo: GoLogo,
  },
];

/**
 * Copyable `npm install` command button.
 *
 * @example
 * <CopyInstallButton background="white" />
 */
function CopyInstallButton({
  background = "white",
}: {
  background?: "white" | "paper";
}) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    if (navigator.clipboard) navigator.clipboard.writeText(INSTALL_COMMAND);
    setCopied(true);
    setTimeout(() => setCopied(false), 1600);
  };

  return (
    <button
      onClick={copy}
      className={`flex h-10 cursor-pointer items-center gap-3 rounded-lg border border-[#DDDBD3] px-3.5 font-mono text-[13.5px] text-ink transition-colors hover:border-cyan-bright ${
        background === "white"
          ? "bg-white shadow-[0_1px_2px_rgba(20,23,26,0.04)]"
          : "bg-paper"
      }`}
    >
      <span className="text-[#A8ACB2]">$</span>
      <span>{INSTALL_COMMAND}</span>
      <span className="ml-1.5 font-sans text-xs font-semibold uppercase tracking-[0.04em] text-cyan-deep">
        {copied ? "copied" : "copy"}
      </span>
    </button>
  );
}

/**
 * Hero diagram: your product sits on top of a Lix repo that holds files, a SQL
 * database, and version control.
 *
 * @example
 * <LixRepoDiagram />
 */
function LixRepoDiagram() {
  return (
    <div className="w-[300px] shrink-0 self-center sm:w-[340px]">
      <svg
        viewBox="0 0 400 326"
        className="block h-auto w-full"
        role="img"
        aria-label="Your product uses a Lix repo containing files, a database, and version control."
      >
        <defs>
          <marker
            id="hero-repo-arrow"
            markerWidth="8"
            markerHeight="8"
            refX="6"
            refY="4"
            orient="auto"
          >
            <path d="M0 0 L7 4 L0 8 Z" fill="#8A8F96" />
          </marker>
        </defs>
        <rect
          x="115"
          y="10"
          width="170"
          height="40"
          rx="20"
          fill="#FFFFFF"
          stroke="#C9C7BF"
        />
        <text
          x="200"
          y="35"
          textAnchor="middle"
          className="font-mono text-[11px] font-bold tracking-[0.07em]"
          fill="#15171B"
        >
          YOUR PRODUCT
        </text>
        <line
          x1="200"
          y1="50"
          x2="200"
          y2="76"
          stroke="#8A8F96"
          strokeWidth="1.2"
          markerEnd="url(#hero-repo-arrow)"
        />
        <rect
          x="20"
          y="82"
          width="360"
          height="230"
          rx="14"
          fill="rgba(7,182,213,0.05)"
          stroke="#07B6D5"
          strokeWidth="1.5"
        />
        <text
          x="200"
          y="112"
          textAnchor="middle"
          className="font-mono text-[11.5px] font-bold tracking-[0.09em]"
          fill="#0891AC"
        >
          LIX REPO
        </text>

        <g transform="translate(110, 196)">
          <rect
            x="-14"
            y="-28"
            width="30"
            height="40"
            rx="3"
            fill="#FBFAF7"
            stroke="#C9C7BF"
            strokeWidth="1.5"
          />
          <rect
            x="-19"
            y="-23"
            width="30"
            height="40"
            rx="3"
            fill="#FFFFFF"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
          <line
            x1="-12"
            y1="-13"
            x2="4"
            y2="-13"
            stroke="#C9C7BF"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
          <line
            x1="-12"
            y1="-5"
            x2="4"
            y2="-5"
            stroke="#C9C7BF"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
          <line
            x1="-12"
            y1="3"
            x2="-3"
            y2="3"
            stroke="#07B6D5"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </g>
        <text
          x="110"
          y="248"
          textAnchor="middle"
          className="font-mono text-[10.5px]"
          fill="#6B7076"
        >
          files
        </text>

        <g transform="translate(200, 196)">
          <ellipse
            cx="0"
            cy="-16"
            rx="19"
            ry="6"
            fill="#FFFFFF"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
          <path
            d="M-19 -16 V10 C-19 13.3 -10.5 16 0 16 C10.5 16 19 13.3 19 10 V-16"
            fill="none"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
          <path
            d="M-19 -3 C-19 0.3 -10.5 3 0 3 C10.5 3 19 0.3 19 -3"
            fill="none"
            stroke="#C9C7BF"
            strokeWidth="1.5"
          />
          <line
            x1="-7"
            y1="9"
            x2="7"
            y2="9"
            stroke="#07B6D5"
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </g>
        <text
          x="200"
          y="248"
          textAnchor="middle"
          className="font-mono text-[10.5px]"
          fill="#6B7076"
        >
          database
        </text>

        <g transform="translate(290, 196)">
          <line
            x1="-26"
            y1="12"
            x2="26"
            y2="12"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
          <path
            d="M-18 12 C-18 -2 -8 -10 0 -10 C10 -10 18 -2 18 12"
            fill="none"
            stroke="#07B6D5"
            strokeWidth="1.5"
          />
          <circle cx="0" cy="-10" r="3.2" fill="#07B6D5" />
          <circle
            cx="-18"
            cy="12"
            r="3.2"
            fill="#FFFFFF"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
          <circle
            cx="18"
            cy="12"
            r="3.2"
            fill="#FFFFFF"
            stroke="#8A8F96"
            strokeWidth="1.5"
          />
        </g>
        <text
          x="290"
          y="248"
          textAnchor="middle"
          className="font-mono text-[10.5px]"
          fill="#6B7076"
        >
          version control
        </text>
      </svg>
    </div>
  );
}

const whatYouGet = [
  {
    title: "Files, in any format",
    description:
      "Text and Markdown, but also DOCX, XLSX, and CAD. Plugins map any format to versioned rows.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="28"
          y="6"
          width="30"
          height="38"
          rx="3"
          fill="#FBFAF7"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="24"
          y="10"
          width="30"
          height="38"
          rx="3"
          fill="#FBFAF7"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="20"
          y="14"
          width="30"
          height="38"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="26"
          y1="24"
          x2="44"
          y2="24"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="31"
          x2="44"
          y2="31"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="38"
          x2="36"
          y2="38"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "SQL database",
    description:
      "File content, app data, and history live in an ACID OLTP database. Query millions of rows with SQL.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <ellipse
          cx="40"
          cy="15"
          rx="19"
          ry="6"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <path
          d="M21 15 V41 C21 44.3 29.5 47 40 47 C50.5 47 59 44.3 59 41 V15"
          fill="none"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <path
          d="M21 28 C21 31.3 29.5 34 40 34 C50.5 34 59 31.3 59 28"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <line
          x1="33"
          y1="40"
          x2="47"
          y2="40"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
  {
    title: "Version control",
    description:
      "Semantic changes: the clause, cell, or row that changed, not a byte blob. Review, merge, and roll back.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <line
          x1="14"
          y1="40"
          x2="66"
          y2="40"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <path
          d="M22 40 C22 26 30 18 40 18 C50 18 58 26 58 40"
          fill="none"
          stroke="#07B6D5"
          strokeWidth="1.5"
        />
        <circle cx="40" cy="18" r="3.5" fill="#07B6D5" />
        <circle
          cx="22"
          cy="40"
          r="3.5"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <circle
          cx="58"
          cy="40"
          r="3.5"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
      </svg>
    ),
  },
  {
    title: "Real-time collaboration",
    description:
      "Companies work live, not in pull requests. People and agents share a repository and see changes as they happen.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <rect
          x="18"
          y="10"
          width="44"
          height="36"
          rx="4"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <line
          x1="26"
          y1="20"
          x2="54"
          y2="20"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <line
          x1="26"
          y1="28"
          x2="46"
          y2="28"
          stroke="#C9C7BF"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
        <path d="M32 34 L32 42 L38 38.5 Z" fill="#07B6D5" />
        <path d="M50 24 L50 32 L56 28.5 Z" fill="#8A8F96" />
      </svg>
    ),
  },
  {
    title: "Permissions",
    badge: "soon",
    description:
      "Finance, legal, and contractors need different access. Permissions will live inside the repository: per file, per group, and versioned like any other change.",
    icon: (
      <svg viewBox="0 0 80 56" className="block h-11 w-16" aria-hidden="true">
        <path
          d="M32 25 V18 a8 8 0 0 1 16 0 V25"
          fill="none"
          stroke="#C9C7BF"
          strokeWidth="1.5"
        />
        <rect
          x="27"
          y="25"
          width="26"
          height="21"
          rx="3"
          fill="#FFFFFF"
          stroke="#8A8F96"
          strokeWidth="1.5"
        />
        <circle cx="40" cy="33.5" r="2.6" fill="#07B6D5" />
        <line
          x1="40"
          y1="36"
          x2="40"
          y2="40.5"
          stroke="#07B6D5"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    ),
  },
];

/**
 * Landing page for lix.dev.
 *
 * @example
 * <LandingPage readmeHtml={html} />
 */
function LandingPage({ readmeHtml }: { readmeHtml?: string }) {
  const githubStars = getGithubStars("opral/lix");

  return (
    <div className="min-h-screen bg-paper text-ink">
      <Header width="narrow" />
      <main className="mx-auto w-full max-w-[1100px] px-8">
        {/* Hero */}
        <section className="flex flex-wrap items-center justify-between gap-12 pt-16">
          <div className="min-w-[300px] max-w-[620px] flex-1">
            <p className="mb-4 font-mono text-xs uppercase tracking-[0.08em] text-ink-faint">
              Open source · MIT
            </p>
            <h1 className="text-balance text-[30px] font-bold leading-[1.1] tracking-[-0.03em] sm:text-[40px]">
              Files, a SQL database, and version control in one repository
            </h1>
            <p className="mt-4 max-w-[620px] text-base leading-[1.6] text-ink-secondary">
              Lix is a repository you embed in your product. Agents read and
              write normal files. Your product queries SQL. Lix versions
              everything both write: branch, diff, merge, roll back.
            </p>
            <div className="mt-6 flex flex-wrap items-center gap-5">
              <span className="flex items-center gap-1.5 border-b-2 border-ink pb-0.5 text-[13px] font-semibold text-ink">
                <JsLogo className="h-3.5 w-3.5" />
                JavaScript
              </span>
              {sdkLanguages.map(({ label, href, title, Logo }) => (
                <a
                  key={label}
                  href={href}
                  target="_blank"
                  rel="noopener noreferrer"
                  title={title}
                  className="flex items-center gap-1.5 border-b-2 border-transparent pb-0.5 text-[13px] text-ink-muted transition-colors hover:text-cyan-deep"
                >
                  <Logo className="h-3.5 w-3.5" />
                  {label}
                </a>
              ))}
            </div>
            <div className="mt-3 flex flex-wrap items-center gap-3">
              <CopyInstallButton background="white" />
              <a
                href="/docs/what-is-lix"
                className="flex h-10 items-center rounded-lg bg-ink px-4 text-[13.5px] font-semibold text-paper transition-colors hover:text-paper"
              >
                Read the docs
              </a>
            </div>
          </div>
          <LixRepoDiagram />
        </section>

        {/* Stats */}
        <section className="mt-12 flex flex-wrap gap-10 border-y border-line py-5">
          <div className="flex flex-col gap-1">
            <span className="text-[22px] font-bold leading-none tracking-[-0.02em]">
              {coarseWeeklyDownloads()}
            </span>
            <span className="text-[13px] text-ink-muted">weekly downloads</span>
          </div>
          {githubStars !== null && (
            <a
              href="https://github.com/opral/lix"
              target="_blank"
              rel="noopener noreferrer"
              className="group flex flex-col gap-1"
            >
              <span className="text-[22px] font-bold leading-none tracking-[-0.02em] transition-colors group-hover:text-cyan-deep">
                {githubStars.toLocaleString("en-US")}
              </span>
              <span className="text-[13px] text-ink-muted">GitHub stars</span>
            </a>
          )}
        </section>

        {/* Adoption chart */}
        <DownloadsChart />

        {/* What you get */}
        <section className="pt-14">
          <h2 className="mb-1 text-[22px] font-bold tracking-[-0.02em]">
            What you get
          </h2>
          <p className="mt-3 max-w-[640px] text-[15px] leading-[1.6] text-ink-secondary">
            Files for AI agents, SQL for your app, version control for review
            flows. All in one repository.
          </p>
          <div className="mt-5">
            {whatYouGet.map((item, index) => (
              <div
                key={item.title}
                className={`grid grid-cols-1 items-center gap-3 border-t border-line py-3 sm:grid-cols-[84px_210px_1fr] sm:gap-7 ${
                  index === whatYouGet.length - 1 ? "border-b" : ""
                }`}
              >
                {item.icon}
                <span className="flex flex-wrap items-center gap-2 text-[15px] font-semibold tracking-[-0.01em]">
                  {item.title}
                  {"badge" in item && (
                    <span className="rounded border border-line-strong px-1.5 py-0.5 font-mono text-[10px] font-normal uppercase tracking-[0.08em] text-ink-muted">
                      {item.badge}
                    </span>
                  )}
                </span>
                <span className="text-[15px] leading-[1.6] text-ink-secondary">
                  {item.description}
                </span>
              </div>
            ))}
          </div>
        </section>

        {/* README */}
        {readmeHtml && (
          <section className="-mx-8 pt-14 sm:mx-0">
            <div className="overflow-hidden border-y border-line bg-white sm:rounded-xl sm:border-x">
              <div className="flex flex-wrap items-center justify-between gap-5 border-b border-line-soft bg-[#FDFCFA] px-4 py-2.5 sm:px-7">
                <span className="font-mono text-xs text-ink-faint">
                  README.md · opral/lix
                </span>
                <a
                  href="https://github.com/opral/lix"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1.5 font-mono text-xs text-ink-muted transition-colors hover:text-cyan-deep"
                >
                  <svg
                    viewBox="0 0 24 24"
                    fill="currentColor"
                    className="h-3.5 w-3.5"
                    aria-hidden="true"
                  >
                    <path d="M12 2a10 10 0 00-3.16 19.49c.5.09.68-.21.68-.47v-1.69c-2.78.6-3.37-1.34-3.37-1.34a2.64 2.64 0 00-1.1-1.46c-.9-.62.07-.6.07-.6a2.08 2.08 0 011.52 1 2.1 2.1 0 002.87.82 2.11 2.11 0 01.63-1.32c-2.22-.25-4.56-1.11-4.56-4.95a3.88 3.88 0 011-2.7 3.6 3.6 0 01.1-2.67s.84-.27 2.75 1a9.5 9.5 0 015 0c1.91-1.29 2.75-1 2.75-1a3.6 3.6 0 01.1 2.67 3.87 3.87 0 011 2.7c0 3.85-2.34 4.7-4.57 4.95a2.37 2.37 0 01.68 1.84v2.72c0 .27.18.57.69.47A10 10 0 0012 2z" />
                  </svg>
                  view on GitHub →
                </a>
              </div>
              <div className="px-4 pb-10 pt-8 sm:px-10">
                <article
                  className="markdown-wc-body"
                  dangerouslySetInnerHTML={{ __html: readmeHtml }}
                />
              </div>
            </div>
          </section>
        )}

        {/* CTA */}
        <section className="pb-16 pt-14">
          <div className="flex flex-wrap items-center justify-between gap-8 rounded-xl border border-line bg-white px-6 py-7 sm:px-10">
            <div className="flex flex-col gap-1.5">
              <h2 className="text-[19px] font-bold tracking-[-0.02em]">
                Start with the SDK
              </h2>
              <p className="text-[14.5px] text-ink-muted">
                MIT licensed. Runs in the browser and on the server.
              </p>
            </div>
            <CopyInstallButton background="paper" />
          </div>
        </section>
      </main>
      <Footer width="narrow" />
    </div>
  );
}

export default LandingPage;
