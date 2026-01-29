import { useRouterState } from "@tanstack/react-router";
import { getGithubStars } from "../github-stars-cache";
import { Footer } from "./footer";
/**
 * Lix logo used across the landing page.
 *
 * @example
 * <LixLogo className="h-6 w-6" />
 */
const LixLogo = ({ className = "" }) => (
  <svg
    width="30"
    height="22"
    viewBox="0 0 26 18"
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
    className={className}
  >
    <g id="Group 162">
      <path
        id="Vector"
        d="M14.7618 5.74842L16.9208 9.85984L22.3675 0.358398H25.7133L19.0723 11.6284L22.5712 17.5085H19.2407L16.9208 13.443L14.6393 17.5085H11.2705L14.7618 11.6284L11.393 5.74842H14.7618Z"
        fill="currentColor"
      />
      <path
        id="Vector_2"
        d="M6.16211 17.5081V5.74805H9.42368V17.5081H6.16211Z"
        fill="currentColor"
      />
      <path
        id="Vector_3"
        d="M3.52112 0.393555V17.6416H0.287109V0.393555H3.52112Z"
        fill="currentColor"
      />
      <path
        id="Rectangle 391"
        d="M6.21582 0.393555H14.8399V3.08856H6.21582V0.393555Z"
        fill="currentColor"
      />
    </g>
  </svg>
);

/**
 * GitHub mark icon used in the site header.
 *
 * @example
 * <GitHubIcon className="h-5 w-5" />
 */
const GitHubIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 24 24"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M12 2a10 10 0 00-3.16 19.49c.5.09.68-.21.68-.47v-1.69c-2.78.6-3.37-1.34-3.37-1.34a2.64 2.64 0 00-1.1-1.46c-.9-.62.07-.6.07-.6a2.08 2.08 0 011.52 1 2.1 2.1 0 002.87.82 2.11 2.11 0 01.63-1.32c-2.22-.25-4.56-1.11-4.56-4.95a3.88 3.88 0 011-2.7 3.6 3.6 0 01.1-2.67s.84-.27 2.75 1a9.5 9.5 0 015 0c1.91-1.29 2.75-1 2.75-1a3.6 3.6 0 01.1 2.67 3.87 3.87 0 011 2.7c0 3.85-2.34 4.7-4.57 4.95a2.37 2.37 0 01.68 1.84v2.72c0 .27.18.57.69.47A10 10 0 0012 2z" />
  </svg>
);

/**
 * Discord icon used in the site header.
 *
 * @example
 * <DiscordIcon className="h-5 w-5" />
 */
const DiscordIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 71 55"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M60.1045 4.8978C55.5792 2.8214 50.7265 1.2916 45.6527 0.41542C45.5603 0.39851 45.468 0.440769 45.4204 0.525289C44.7963 1.6353 44.105 3.0834 43.6209 4.2216C38.1637 3.4046 32.7345 3.4046 27.3892 4.2216C26.905 3.0581 26.1886 1.6353 25.5617 0.525289C25.5141 0.443589 25.4218 0.40133 25.3294 0.41542C20.2584 1.2888 15.4057 2.8186 10.8776 4.8978C10.8384 4.9147 10.8048 4.9429 10.7825 4.9793C1.57795 18.7309 -0.943561 32.1443 0.293408 45.3914C0.299005 45.4562 0.335386 45.5182 0.385761 45.5574C6.45866 50.0174 12.3413 52.7249 18.1147 54.5195C18.2071 54.5477 18.3052 54.5131 18.363 54.4376C19.7295 52.5728 20.9469 50.6063 21.9907 48.5383C22.0527 48.4172 21.9931 48.2735 21.8674 48.2259C19.9366 47.4931 18.0979 46.6 16.3292 45.5858C16.1893 45.5033 16.1789 45.3039 16.3116 45.2082C16.679 44.9293 17.0464 44.6391 17.4034 44.346C17.4654 44.2947 17.5534 44.2843 17.6228 44.3189C29.2558 49.8743 41.8354 49.8743 53.3179 44.3189C53.3873 44.2817 53.4753 44.292 53.5401 44.3433C53.8971 44.6364 54.2645 44.9293 54.6346 45.2082C54.7673 45.3039 54.7594 45.5033 54.6195 45.5858C52.8508 46.6197 51.0121 47.4931 49.0775 48.223C48.9518 48.2706 48.894 48.4172 48.956 48.5383C50.0198 50.6034 51.2372 52.5699 52.5872 54.4347C52.6414 54.5131 52.7423 54.5477 52.8347 54.5195C58.6464 52.7249 64.529 50.0174 70.6019 45.5574C70.6559 45.5182 70.6894 45.459 70.695 45.3942C72.1747 30.0791 68.2147 16.7757 60.1968 4.9821C60.1772 4.9429 60.1436 4.9147 60.1045 4.8978ZM23.7259 37.3253C20.2276 37.3253 17.3451 34.1136 17.3451 30.1693C17.3451 26.225 20.1717 23.0133 23.7259 23.0133C27.308 23.0133 30.1626 26.2532 30.1066 30.1693C30.1066 34.1136 27.28 37.3253 23.7259 37.3253ZM47.2012 37.3253C43.7029 37.3253 40.8203 34.1136 40.8203 30.1693C40.8203 26.225 43.6469 23.0133 47.2012 23.0133C50.7833 23.0133 53.6379 26.2532 53.5819 30.1693C53.5819 34.1136 50.7833 37.3253 47.2012 37.3253Z" />
  </svg>
);

/**
 * X (formerly Twitter) icon used in the site header.
 *
 * @example
 * <XIcon className="h-5 w-5" />
 */
const XIcon = ({ className = "" }) => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 1200 1227"
    fill="currentColor"
    className={className}
    aria-hidden="true"
  >
    <path d="M714.163 519.284 1160.89 0h-105.86L667.137 450.887 357.328 0H0l468.492 681.821L0 1226.37h105.866l409.625-476.152 327.181 476.152H1200L714.137 519.284h.026ZM569.165 687.828l-47.468-67.894-377.686-540.24h162.604l304.797 435.991 47.468 67.894 396.2 566.721H892.476L569.165 687.854v-.026Z" />
  </svg>
);

/**
 * JavaScript icon for code tabs.
 */
const JsIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 24 24"
    className={className}
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <rect width="24" height="24" fill="#F7DF1E" rx="2" />
    <path
      d="M6 18l2-12h4l2 12h-2l-1-4h-4l-1 4H6z" // Placeholder path if text doesn't work well, but let's try text
      fill="none"
    />
    <text
      x="12"
      y="17"
      textAnchor="middle"
      fontSize="11"
      fontWeight="bold"
      fill="black"
      fontFamily="sans-serif"
    >
      JS
    </text>
  </svg>
);

/**
 * Python icon for code tabs.
 */
const PythonIcon = ({ className = "" }) => (
  <svg
    viewBox="16 16 32 32"
    className={className}
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fill="url(#python__a)"
      d="M31.885 16c-8.124 0-7.617 3.523-7.617 3.523l.01 3.65h7.752v1.095H21.197S16 23.678 16 31.876c0 8.196 4.537 7.906 4.537 7.906h2.708v-3.804s-.146-4.537 4.465-4.537h7.688s4.32.07 4.32-4.175v-7.019S40.374 16 31.885 16zm-4.275 2.454a1.394 1.394 0 1 1 0 2.79 1.393 1.393 0 0 1-1.395-1.395c0-.771.624-1.395 1.395-1.395z"
    />
    <path
      fill="url(#python__b)"
      d="M32.115 47.833c8.124 0 7.617-3.523 7.617-3.523l-.01-3.65H31.97v-1.095h10.832S48 40.155 48 31.958c0-8.197-4.537-7.906-4.537-7.906h-2.708v3.803s.146 4.537-4.465 4.537h-7.688s-4.32-.07-4.32 4.175v7.019s-.656 4.247 7.833 4.247zm4.275-2.454a1.393 1.393 0 0 1-1.395-1.395 1.394 1.394 0 1 1 1.395 1.395z"
    />
    <defs>
      <linearGradient
        id="python__a"
        x1="19.075"
        x2="34.898"
        y1="18.782"
        y2="34.658"
        gradientUnits="userSpaceOnUse"
      >
        <stop stopColor="#387EB8" />
        <stop offset="1" stopColor="#366994" />
      </linearGradient>
      <linearGradient
        id="python__b"
        x1="28.809"
        x2="45.803"
        y1="28.882"
        y2="45.163"
        gradientUnits="userSpaceOnUse"
      >
        <stop stopColor="#FFE052" />
        <stop offset="1" stopColor="#FFC331" />
      </linearGradient>
    </defs>
  </svg>
);

/**
 * Rust icon for code tabs.
 */
const RustIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 224 224"
    className={className}
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      fill="#000"
      d="M218.46 109.358l-9.062-5.614c-.076-.882-.162-1.762-.258-2.642l7.803-7.265a3.107 3.107 0 00.933-2.89 3.093 3.093 0 00-1.967-2.312l-9.97-3.715c-.25-.863-.512-1.72-.781-2.58l6.214-8.628a3.114 3.114 0 00-.592-4.263 3.134 3.134 0 00-1.431-.637l-10.507-1.709a80.869 80.869 0 00-1.263-2.353l4.417-9.7a3.12 3.12 0 00-.243-3.035 3.106 3.106 0 00-2.705-1.385l-10.671.372a85.152 85.152 0 00-1.685-2.044l2.456-10.381a3.125 3.125 0 00-3.762-3.763l-10.384 2.456a88.996 88.996 0 00-2.047-1.684l.373-10.671a3.11 3.11 0 00-1.385-2.704 3.127 3.127 0 00-3.034-.246l-9.681 4.417c-.782-.429-1.567-.854-2.353-1.265l-1.713-10.506a3.098 3.098 0 00-1.887-2.373 3.108 3.108 0 00-3.014.35l-8.628 6.213c-.85-.27-1.703-.53-2.56-.778l-3.716-9.97a3.111 3.111 0 00-2.311-1.97 3.134 3.134 0 00-2.89.933l-7.266 7.802a93.746 93.746 0 00-2.643-.258l-5.614-9.082A3.125 3.125 0 00111.97 4c-1.09 0-2.085.56-2.642 1.478l-5.615 9.081a93.32 93.32 0 00-2.642.259l-7.266-7.802a3.13 3.13 0 00-2.89-.933 3.106 3.106 0 00-2.312 1.97l-3.715 9.97c-.857.247-1.71.506-2.56.778L73.7 12.588a3.101 3.101 0 00-3.014-.35A3.127 3.127 0 0068.8 14.61l-1.713 10.506c-.79.41-1.575.832-2.353 1.265l-9.681-4.417a3.125 3.125 0 00-4.42 2.95l.372 10.67c-.69.553-1.373 1.115-2.048 1.685l-10.383-2.456a3.143 3.143 0 00-2.93.832 3.124 3.124 0 00-.833 2.93l2.436 10.383a93.897 93.897 0 00-1.68 2.043l-10.672-.372a3.138 3.138 0 00-2.704 1.385 3.126 3.126 0 00-.246 3.035l4.418 9.7c-.43.779-.855 1.563-1.266 2.353l-10.507 1.71a3.097 3.097 0 00-2.373 1.886 3.117 3.117 0 00.35 3.013l6.214 8.628a89.12 89.12 0 00-.78 2.58l-9.97 3.715a3.117 3.117 0 00-1.035 5.202l7.803 7.265c-.098.879-.184 1.76-.258 2.642l-9.062 5.614A3.122 3.122 0 004 112.021c0 1.092.56 2.084 1.478 2.642l9.062 5.614c.074.882.16 1.762.258 2.642l-7.803 7.265a3.117 3.117 0 001.034 5.201l9.97 3.716a110 110 0 00.78 2.58l-6.212 8.627a3.112 3.112 0 00.6 4.27c.419.33.916.547 1.443.63l10.507 1.709c.407.792.83 1.576 1.265 2.353l-4.417 9.68a3.126 3.126 0 002.95 4.42l10.65-.374c.553.69 1.115 1.372 1.685 2.047l-2.435 10.383a3.09 3.09 0 00.831 2.91 3.117 3.117 0 002.931.83l10.384-2.436a82.268 82.268 0 002.047 1.68l-.371 10.671a3.11 3.11 0 001.385 2.704 3.125 3.125 0 003.034.241l9.681-4.416c.779.432 1.563.854 2.353 1.265l1.713 10.505a3.147 3.147 0 001.887 2.395 3.111 3.111 0 003.014-.349l8.628-6.213c.853.271 1.71.535 2.58.783l3.716 9.969a3.112 3.112 0 002.312 1.967 3.112 3.112 0 002.89-.933l7.266-7.802c.877.101 1.761.186 2.642.264l5.615 9.061a3.12 3.12 0 002.642 1.478 3.165 3.165 0 002.663-1.478l5.614-9.061c.884-.078 1.765-.163 2.643-.264l7.265 7.802a3.106 3.106 0 002.89.933 3.105 3.105 0 002.312-1.967l3.716-9.969c.863-.248 1.719-.512 2.58-.783l8.629 6.213a3.12 3.12 0 004.9-2.045l1.713-10.506c.793-.411 1.577-.838 2.353-1.265l9.681 4.416a3.13 3.13 0 003.035-.241 3.126 3.126 0 001.385-2.704l-.372-10.671a81.794 81.794 0 002.046-1.68l10.383 2.436a3.123 3.123 0 003.763-3.74l-2.436-10.382a84.588 84.588 0 001.68-2.048l10.672.374a3.104 3.104 0 002.704-1.385 3.118 3.118 0 00.244-3.035l-4.417-9.68c.43-.779.852-1.563 1.263-2.353l10.507-1.709a3.08 3.08 0 002.373-1.886 3.11 3.11 0 00-.35-3.014l-6.214-8.627c.272-.857.532-1.717.781-2.58l9.97-3.716a3.109 3.109 0 001.967-2.311 3.107 3.107 0 00-.933-2.89l-7.803-7.265c.096-.88.182-1.761.258-2.642l9.062-5.614a3.11 3.11 0 001.478-2.642 3.157 3.157 0 00-1.476-2.663h-.064zm-60.687 75.337c-3.468-.747-5.656-4.169-4.913-7.637a6.412 6.412 0 017.617-4.933c3.468.741 5.676 4.169 4.933 7.637a6.414 6.414 0 01-7.617 4.933h-.02zm-3.076-20.847c-3.158-.677-6.275 1.334-6.936 4.5l-3.22 15.026c-9.929 4.5-21.055 7.018-32.614 7.018-11.89 0-23.12-2.622-33.234-7.328l-3.22-15.026c-.677-3.158-3.778-5.18-6.936-4.499l-13.273 2.848a80.222 80.222 0 01-6.853-8.091h64.61c.731 0 1.218-.132 1.218-.797v-22.91c0-.665-.487-.797-1.218-.797H94.133v-14.469h20.415c1.864 0 9.97.533 12.551 10.898.811 3.179 2.601 13.54 3.818 16.863 1.214 3.715 6.152 11.146 11.415 11.146h32.202c.365 0 .755-.041 1.166-.116a80.56 80.56 0 01-7.307 8.587l-13.583-2.911-.113.058zm-89.38 20.537a6.407 6.407 0 01-7.617-4.933c-.74-3.467 1.462-6.894 4.934-7.637a6.417 6.417 0 017.617 4.933c.74 3.468-1.464 6.894-4.934 7.637zm-24.564-99.28a6.438 6.438 0 01-3.261 8.484c-3.241 1.438-7.019-.025-8.464-3.261-1.445-3.237.025-7.039 3.262-8.483a6.416 6.416 0 018.463 3.26zM33.22 102.94l13.83-6.15c2.952-1.311 4.294-4.769 2.972-7.72l-2.848-6.44H58.36v50.362h-22.5a79.158 79.158 0 01-3.014-21.672c0-2.869.155-5.697.452-8.483l-.08.103zm60.687-4.892v-14.86h26.629c1.376 0 9.722 1.59 9.722 7.822 0 5.18-6.399 7.038-11.663 7.038h-24.77.082zm96.811 13.375c0 1.973-.072 3.922-.216 5.862h-8.113c-.811 0-1.137.532-1.137 1.327v3.715c0 8.752-4.934 10.671-9.268 11.146-4.129.464-8.691-1.726-9.248-4.252-2.436-13.684-6.482-16.595-12.881-21.672 7.948-5.036 16.204-12.487 16.204-22.498 0-10.753-7.369-17.523-12.385-20.847-7.059-4.644-14.862-5.572-16.968-5.572H52.899c11.374-12.673 26.835-21.673 44.174-24.975l9.887 10.361a5.849 5.849 0 008.278.19l11.064-10.568c23.119 4.314 42.729 18.721 54.082 38.598l-7.576 17.09c-1.306 2.951.027 6.419 2.973 7.72l14.573 6.48c.255 2.607.383 5.224.384 7.843l-.021.052zM106.912 24.94a6.398 6.398 0 019.062.209 6.437 6.437 0 01-.213 9.082 6.396 6.396 0 01-9.062-.21 6.436 6.436 0 01.213-9.083v.002zm75.137 60.476a6.402 6.402 0 018.463-3.26 6.425 6.425 0 013.261 8.482 6.402 6.402 0 01-8.463 3.261 6.425 6.425 0 01-3.261-8.483z"
    />
  </svg>
);

/**
 * Go icon for code tabs.
 */
const GoIcon = ({ className = "" }) => (
  <svg
    viewBox="0 0 207 180"
    className={className}
    xmlns="http://www.w3.org/2000/svg"
  >
    <g fill="#00ADD8" fillRule="evenodd" transform="translate(0, 51)">
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

/**
 * Landing page for the Lix documentation site.
 *
 * @example
 * <LandingPage />
 */
function LandingPage({ readmeHtml }: { readmeHtml?: string }) {
  const docsPath = "/docs";
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const githubStars = getGithubStars("opral/lix");

  const formatStars = (count: number) => {
    if (count >= 1000) {
      return `${(count / 1000).toFixed(1).replace(/\.0$/, "")}k`;
    }
    return count.toString();
  };

  const navLinks = [
    { href: docsPath, label: "Docs" },
    { href: "/plugins/", label: "Plugins" },
    { href: "/blog/", label: "Blog" },
  ];

  const isActive = (href: string) =>
    href === "/"
      ? pathname === "/"
      : pathname === href || pathname.startsWith(`${href.replace(/\/$/, "")}/`);

  const socialLinks = [
    {
      href: "https://discord.gg/gdMPPWy57R",
      label: "Discord",
      Icon: DiscordIcon,
      sizeClass: "h-5 w-5",
    },
    {
      href: "https://x.com/lixCCS",
      label: "X",
      Icon: XIcon,
      sizeClass: "h-4 w-4",
    },
  ];

  return (
    <div className="font-sans text-gray-900 bg-white">
      <header className="sticky top-0 z-50 border-b border-gray-200 bg-white/80 backdrop-blur">
        <div className="mx-auto flex w-full max-w-[1440px] items-center justify-between pl-6 pr-6 py-3">
          <a
            href="/"
            className="flex items-center text-[#0891B2]"
            aria-label="lix home"
          >
            <LixLogo className="h-7 w-7" />
            <span className="sr-only">lix</span>
          </a>
          <div className="flex items-center gap-6">
            <nav className="hidden items-center gap-4 text-sm font-medium text-gray-700 sm:flex">
              {navLinks.map(({ href, label }) => (
                <a
                  key={href}
                  href={href}
                  className={
                    isActive(href)
                      ? href.startsWith("/plugins")
                        ? "px-2 py-1 text-[#0891B2] hover:text-[#0692B6]"
                        : "px-2 py-1 text-[#0891B2]"
                      : "px-2 py-1 transition-colors hover:text-[#0692B6]"
                  }
                  aria-current={isActive(href) ? "page" : undefined}
                >
                  {label}
                </a>
              ))}
            </nav>
            <div
              className="hidden h-4 w-px bg-gray-200 sm:block"
              aria-hidden="true"
            />
            <div className="flex items-center gap-3">
              {socialLinks.map(({ href, label, Icon, sizeClass }) => (
                <a
                  key={label}
                  href={href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-gray-900 transition-colors hover:text-gray-900"
                  aria-label={label}
                >
                  <Icon className={sizeClass ?? "h-5 w-5"} />
                </a>
              ))}
              <div className="h-4 w-px bg-gray-200" aria-hidden="true" />
              <a
                href="https://github.com/opral/lix"
                target="_blank"
                rel="noopener noreferrer"
                className="group inline-flex items-center gap-1.5 text-sm font-medium text-gray-700 transition-colors hover:text-gray-700"
              >
                <GitHubIcon className="h-5 w-5" />
                GitHub
                {githubStars !== null && (
                  <span
                    className="inline-flex items-center gap-1 text-gray-500 transition-colors group-hover:text-gray-500"
                    title={`${githubStars.toLocaleString()} GitHub stars`}
                    aria-label={`${githubStars.toLocaleString()} GitHub stars`}
                  >
                    <span className="relative h-3.5 w-3.5" aria-hidden="true">
                      <svg
                        className="absolute inset-0 h-3.5 w-3.5 text-gray-400 group-hover:opacity-0 transition-opacity"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      >
                        <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2" />
                      </svg>
                      <svg
                        className="absolute inset-0 h-3.5 w-3.5 text-yellow-500 opacity-0 group-hover:opacity-100 transition-opacity"
                        viewBox="0 0 16 16"
                        fill="currentColor"
                      >
                        <path d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.75.75 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.818 6.374a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25z" />
                      </svg>
                    </span>
                    <span>{formatStars(githubStars)}</span>
                  </span>
                )}
              </a>
            </div>
          </div>
        </div>
      </header>
      {/* Main content */}
      <main className="relative px-4 sm:px-6">
        {/* Hero Section - Simplified */}
        <section className="relative pt-20 pb-12 px-4 sm:px-6">
          <div className="relative max-w-4xl mx-auto text-center">
            {/* Beta Chip */}
            <a
              href="https://github.com/opral/lix/issues/374"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 mb-6 px-3 py-1.5 rounded-full bg-amber-50 border border-amber-200 text-sm text-amber-800 hover:bg-amber-100 transition-colors"
            >
              <span className="inline-block w-2 h-2 rounded-full bg-amber-400" />
              <span>
                <span className="font-medium">Lix is in alpha</span> · Follow
                progress to v1.0
              </span>
              <svg
                className="w-3.5 h-3.5"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  d="M9 5l7 7-7 7"
                />
              </svg>
            </a>
            <h1 className="text-gray-900 font-bold leading-[1.1] text-4xl sm:text-5xl md:text-6xl tracking-tight">
              Embeddable version control system for AI agents
            </h1>

            <p className="text-gray-500 text-lg sm:text-xl max-w-4xl mx-auto mt-8">
              Lix is a version control system that can be imported as a library. Use it to, for example, enable human-in-the-loop workflows for AI agents like diffs and reviews.
            </p>

            {/* Trust signals */}
            <div className="flex items-center justify-center gap-8 sm:gap-12 mt-12">
              <div className="flex flex-col items-center">
                <svg
                  className="w-5 h-5 text-gray-400 mb-1.5"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3"
                  />
                </svg>
                <div className="text-2xl font-bold text-gray-900">90k+</div>
                <div className="text-sm text-gray-500 mt-1">
                  Weekly downloads
                </div>
              </div>
              <div className="w-px h-14 bg-gray-200" />
              <div className="flex flex-col items-center">
                <svg
                  className="w-5 h-5 text-gray-400 mb-1.5"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={1.5}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M3 6l3 1m0 0l-3 9a5.002 5.002 0 006.001 0M6 7l3 9M6 7l6-2m6 2l3-1m-3 1l-3 9a5.002 5.002 0 006.001 0M18 7l3 9m-3-9l-6-2m0-2v2m0 16V5m0 16H9m3 0h3"
                  />
                </svg>
                <div className="text-2xl font-bold text-gray-900">MIT</div>
                <div className="text-sm text-gray-500 mt-1">Open Source</div>
              </div>
            </div>

            <div className="flex flex-col sm:flex-row items-center justify-center gap-3 mt-10">
              <a
                href={docsPath}
                className="inline-flex items-center justify-center h-11 px-6 rounded-lg text-sm font-medium bg-[#0891b2] text-white hover:bg-[#0e7490] transition-colors"
              >
                Read the docs
                <svg
                  className="h-4 w-4 ml-2"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  strokeWidth={2}
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    d="M14 5l7 7m0 0l-7 7m7-7H3"
                  />
                </svg>
              </a>
              <a
                href="https://github.com/opral/lix"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex h-11 items-center justify-center gap-2 px-5 rounded-lg border border-gray-300 bg-white text-sm text-gray-800 transition-colors duration-200 hover:bg-gray-50"
                title={
                  githubStars
                    ? `${githubStars.toLocaleString()} GitHub stars`
                    : "Star us on GitHub"
                }
              >
                <svg
                  className="w-5 h-5"
                  viewBox="0 0 24 24"
                  fill="currentColor"
                >
                  <path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.166 6.839 9.489.5.092.682-.217.682-.48 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.645.35-1.087.636-1.337-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.268 2.75 1.026A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.026 2.747-1.026.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z" />
                </svg>
                Star on GitHub
                {githubStars !== null && (
                  <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-600">
                    {githubStars >= 1000
                      ? `${(githubStars / 1000).toFixed(1)}k`
                      : githubStars}
                  </span>
                )}
              </a>
            </div>

            {/* Hero code snippet with language tabs */}
            <div className="mt-12 w-full max-w-2xl mx-auto">
              <div className="rounded-xl border border-gray-200 bg-white overflow-hidden">
                <div className="flex items-center px-4 border-b border-gray-200 bg-gray-50">
                  <div className="flex gap-6 text-sm">
                    <button className="flex items-center gap-2 text-gray-900 font-medium border-b-2 border-gray-900 py-3 px-1 cursor-pointer">
                      <JsIcon className="h-4 w-4" />
                      JavaScript
                    </button>
                    <a
                      href="https://github.com/opral/lix/issues/370"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <PythonIcon className="h-4 w-4" />
                      Python
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/371"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <RustIcon className="h-4 w-4" />
                      Rust
                    </a>
                    <a
                      href="https://github.com/opral/lix/issues/373"
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center gap-2 text-gray-400 py-3 px-1 hover:text-gray-600 transition-colors cursor-pointer"
                    >
                      <GoIcon className="h-4 w-4" />
                      Go
                    </a>
                  </div>
                </div>
                <div className="p-5 text-sm leading-relaxed font-mono text-left overflow-x-auto whitespace-pre-wrap">
                  <span className="text-indigo-600">import</span>{" "}
                  <span className="text-gray-900">
                    {"{ openLix, InMemoryEnvironment }"}
                  </span>{" "}
                  <span className="text-indigo-600">from</span>{" "}
                  <span className="text-amber-600">"@lix-js/sdk"</span>
                  <span className="text-gray-900">;</span>
                  <br />
                  <span className="text-indigo-600">import</span>{" "}
                  <span className="text-gray-900">{"{ plugin "}</span>
                  <span className="text-indigo-600">as</span>
                  <span className="text-gray-900">{" json }"}</span>{" "}
                  <span className="text-indigo-600">from</span>{" "}
                  <span className="text-amber-600">"@lix-js/plugin-json"</span>
                  <span className="text-gray-900">;</span>
                  <br />
                  <br />
                  <span className="text-indigo-600">const</span>{" "}
                  <span className="text-gray-900">lix</span>{" "}
                  <span className="text-gray-900">= openLix</span>
                  <span className="text-gray-900">{"({"}</span>
                  <br />
                  <span className="text-sky-600">{"  environment"}</span>
                  <span className="text-gray-900">{": "}</span>
                  <span className="text-indigo-600">new</span>
                  <span className="text-gray-900">
                    {" InMemoryEnvironment(),"}
                  </span>
                  <br />
                  <span className="text-sky-600">{"  providePlugins"}</span>
                  <span className="text-gray-900">{": ["}</span>
                  <span className="text-gray-900">json</span>
                  <span className="text-gray-900">{"]"}</span>
                  <br />
                  <span className="text-gray-900">{"})"}</span>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Value Props - Lightweight */}
        <section className="py-12 px-6 sm:px-12 md:px-16 bg-white">
          <div className="max-w-6xl mx-auto">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-8 sm:gap-12">
              <div className="flex flex-col items-center sm:items-start gap-4">
                {/* Library/dependency illustration */}
                <div className="w-full max-w-[220px] h-32 rounded-lg border border-gray-200 bg-white p-4">
                  <div className="text-xs text-gray-400 mb-3">dependencies</div>
                  <div className="space-y-2 font-mono text-xs">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="w-2.5 h-2.5 rounded bg-gray-200"></div>
                        <span className="text-gray-400">http</span>
                      </div>
                      <span className="text-gray-300">2.1</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="w-2.5 h-2.5 rounded bg-gray-200"></div>
                        <span className="text-gray-400">db</span>
                      </div>
                      <span className="text-gray-300">3.0</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <div className="w-2.5 h-2.5 rounded bg-cyan-200 border border-cyan-400"></div>
                        <span className="text-gray-700 font-medium">lix</span>
                      </div>
                      <span className="text-gray-400">1.0</span>
                    </div>
                  </div>
                </div>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    It's just a library
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Lix is a library you import. Get branching, diff, rollback in your existing stack.
                  </p>
                </div>
              </div>
              <div className="flex flex-col items-center sm:items-start gap-4">
                {/* Diff illustration - semantic/field-level */}
                <div className="w-full max-w-[220px] h-32 rounded-lg border border-gray-200 bg-white p-4">
                  <div className="text-xs text-gray-400 mb-3">config.json</div>
                  <div className="space-y-3 text-sm">
                    <div className="flex items-center justify-between">
                      <span className="text-gray-600">title</span>
                      <div className="flex items-center gap-1.5">
                        <span className="bg-red-50 text-red-700 px-1 rounded line-through">
                          Draft
                        </span>
                        <span className="text-gray-300">→</span>
                        <span className="bg-green-50 text-green-700 px-1 rounded">
                          Final
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-gray-600">price</span>
                      <div className="flex items-center gap-1.5">
                        <span className="bg-red-50 text-red-700 px-1 rounded line-through">
                          10
                        </span>
                        <span className="text-gray-300">→</span>
                        <span className="bg-green-50 text-green-700 px-1 rounded">
                          12
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    Tracks semantic changes
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Lix stores semantic changes via plugins. Diffs, blame, and history are queryable via SQL.
                  </p>
                </div>
              </div>
              <div className="flex flex-col items-center sm:items-start gap-4">
                {/* Trace illustration */}
                <div className="w-full max-w-[220px] h-32 rounded-lg border border-gray-200 bg-white p-4 font-mono text-xs">
                  <div className="flex items-center gap-2 text-gray-400">
                    <span>12:03</span>
                    <svg
                      className="w-3 h-3"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                    >
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                      <polyline points="14 2 14 8 20 8" />
                      <line x1="16" y1="13" x2="8" y2="13" />
                      <line x1="16" y1="17" x2="8" y2="17" />
                    </svg>
                    <span className="text-gray-600">edit config.json</span>
                  </div>
                  <div className="flex items-center gap-2 text-gray-400 mt-1.5">
                    <span>12:04</span>
                    <svg
                      className="w-3 h-3"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                    >
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                      <polyline points="14 2 14 8 20 8" />
                      <line x1="16" y1="13" x2="8" y2="13" />
                      <line x1="16" y1="17" x2="8" y2="17" />
                    </svg>
                    <span className="text-gray-600">update data.xlsx</span>
                  </div>
                  <div className="flex items-center gap-2 mt-1.5">
                    <span className="text-gray-400">12:05</span>
                    <div className="w-3 h-3 rounded-full bg-green-500 flex items-center justify-center">
                      <svg
                        className="w-2 h-2 text-white"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="3"
                      >
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    </div>
                    <span className="text-gray-900 font-medium">approved</span>
                  </div>
                  <div className="flex items-center gap-2 text-gray-400 mt-1.5">
                    <span>12:06</span>
                    <svg
                      className="w-3 h-3"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                    >
                      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                      <polyline points="14 2 14 8 20 8" />
                      <line x1="16" y1="13" x2="8" y2="13" />
                      <line x1="16" y1="17" x2="8" y2="17" />
                    </svg>
                    <span className="text-gray-600">edit report.pdf</span>
                  </div>
                </div>
                <div className="text-center sm:text-left">
                  <h3 className="text-lg font-semibold text-gray-900">
                    Approval workflows for agents
                  </h3>
                  <p className="text-gray-600 text-base mt-2">
                    Agents propose changes in isolated versions. Humans review, approve, and merge.
                  </p>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* README Content */}
        {readmeHtml && (
          <section className="py-16 px-6 sm:px-12 md:px-16 bg-white border-t border-gray-200">
            <div className="max-w-4xl mx-auto">
              {/* GitHub README banner */}
              <a
                href="https://github.com/opral/lix"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between mb-10 px-4 py-3 rounded-lg border border-gray-200 bg-gray-50 hover:bg-gray-100 transition-colors group"
              >
                <div className="flex items-center gap-3">
                  <svg
                    className="w-5 h-5 text-gray-700"
                    viewBox="0 0 24 24"
                    fill="currentColor"
                  >
                    <path d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.166 6.839 9.489.5.092.682-.217.682-.48 0-.237-.008-.866-.013-1.7-2.782.603-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.645.35-1.087.636-1.337-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.268 2.75 1.026A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.026 2.747-1.026.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.161 22 16.416 22 12c0-5.523-4.477-10-10-10z" />
                  </svg>
                  <div>
                    <span className="text-sm font-medium text-gray-900">
                      README.md
                    </span>
                    <span className="text-sm text-gray-500 ml-2">
                      from opral/lix
                    </span>
                  </div>
                </div>
                <div className="flex items-center gap-1.5 text-sm text-gray-600 group-hover:text-gray-900">
                  View on GitHub
                  <svg
                    className="w-4 h-4"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2}
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M14 5l7 7m0 0l-7 7m7-7H3"
                    />
                  </svg>
                </div>
              </a>
              <article
                className="markdown-wc-body"
                dangerouslySetInnerHTML={{ __html: readmeHtml }}
              />
            </div>
          </section>
        )}

        <Footer />
      </main>
    </div>
  );
}

export default LandingPage;
