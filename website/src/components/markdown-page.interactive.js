import "./doc-code-snippet-element";

const COPY_BUTTON_ATTR = "data-mwc-copy-button";
const LANG_TABS_ATTR = "data-mwc-lang-tabs";

const SDK_TABS = [
  { label: "JavaScript" },
  { label: "Python", href: "https://github.com/opral/lix/issues/373" },
  { label: "Rust", href: "https://github.com/opral/lix/issues/371" },
  { label: "Go", href: "https://github.com/opral/lix/issues/370" },
];

function ensureLanguageTabs(root = document) {
  const blocks = root.querySelectorAll("pre[data-mwc-codeblock]");
  for (const pre of blocks) {
    if (pre.hasAttribute(LANG_TABS_ATTR)) continue;
    const code = pre.querySelector("code");
    if (!code) continue;
    const isJsBlock = /\blanguage-(ts|tsx|js|jsx|typescript|javascript)\b/.test(
      code.className,
    );
    if (!isJsBlock) continue;
    pre.setAttribute(LANG_TABS_ATTR, "");

    const tabs = document.createElement("div");
    tabs.className = "mwc-lang-tabs";
    for (const tab of SDK_TABS) {
      let el;
      if (tab.href) {
        el = document.createElement("a");
        el.href = tab.href;
        el.target = "_blank";
        el.rel = "noopener noreferrer";
        el.title = `The ${tab.label} SDK is planned. Upvote the issue on GitHub.`;
        el.className = "mwc-lang-tab";
      } else {
        el = document.createElement("span");
        el.className = "mwc-lang-tab mwc-lang-tab-active";
      }
      el.textContent = tab.label;
      tabs.appendChild(el);
    }
    pre.parentNode?.insertBefore(tabs, pre);
  }
}

function ensureCopyButtons(root = document) {
  const blocks = root.querySelectorAll("pre[data-mwc-codeblock]");
  for (const pre of blocks) {
    if (pre.querySelector(`[${COPY_BUTTON_ATTR}]`)) continue;

    const button = document.createElement("button");
    button.type = "button";
    button.setAttribute(COPY_BUTTON_ATTR, "");
    button.className = "mwc-copy-button";
    button.textContent = "Copy";
    pre.appendChild(button);
  }
}

function handleCopyClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;
  const button = target.closest(`[${COPY_BUTTON_ATTR}]`);
  if (!button) return;

  const pre = button.closest("pre[data-mwc-codeblock]");
  const code = pre?.querySelector("code")?.textContent ?? "";
  navigator.clipboard.writeText(code);

  const previous = button.textContent;
  button.textContent = "Copied";
  window.setTimeout(() => {
    button.textContent = previous || "Copy";
  }, 1500);
}

function initCopyButtons() {
  if (window.__lixDocsCopyButtonsInitialized) return;
  window.__lixDocsCopyButtonsInitialized = true;

  ensureCopyButtons();
  ensureLanguageTabs();
  document.addEventListener("click", handleCopyClick);

  const observer = new MutationObserver(() => {
    ensureCopyButtons();
    ensureLanguageTabs();
  });
  observer.observe(document.body, { childList: true, subtree: true });
}

if (typeof window !== "undefined") {
  initCopyButtons();
}
