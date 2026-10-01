---
type: patch
---

Made `binaryen` and `@bytecodealliance/jco-transpile` optional dependencies of `@lix-js/sdk`.

These two packages (about 104 MB installed) are only loaded when a JS-hosted plugin component is compiled, so installs that skip optional dependencies no longer download them. Compiling a component without them now fails with an error that names the packages to install.
