---
type: patch
---

Improved Markdown import performance for large plain-prose documents.

Lix now avoids redundant canonical rendering work when Markdown source is already
canonical literal prose, while preserving the existing behavior for rich Markdown.
