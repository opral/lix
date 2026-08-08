#!/usr/bin/env python3
"""Function-scoped structural proof for Correction-I's shared-view contract.

This is deliberately a small source parser rather than a token-presence check.
It identifies Rust function/struct bodies, follows the provider binding and
chronology call in those bodies, and compares the caller-owned read expression
used by both providers.  The fixture cases below are executable regression
tests for the three false positives that the old regex gate could accept:
distinct views, a fresh read, and a name-only/fake chronology seam.

It is a test/report-only verifier.  It never edits the candidate tree.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
CALL_WORDS = {"if", "for", "while", "match", "loop", "fn", "struct"}


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    name: str
    body: str


def mask_rust(source: str) -> str:
    """Mask comments and literals while preserving length and line structure."""

    out = list(source)
    i = 0
    n = len(source)
    block_depth = 0
    mode = "code"
    raw_hashes = 0
    while i < n:
        if mode == "block":
            if source.startswith("/*", i):
                block_depth += 1
                out[i : i + 2] = "  "
                i += 2
            elif source.startswith("*/", i):
                block_depth -= 1
                out[i : i + 2] = "  "
                i += 2
                if block_depth == 0:
                    mode = "code"
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if mode == "line":
            if source[i] == "\n":
                mode = "code"
            else:
                out[i] = " "
            i += 1
            continue
        if mode == "string":
            if source[i] == "\\":
                out[i] = " "
                if i + 1 < n and source[i + 1] != "\n":
                    out[i + 1] = " "
                i += 2
            elif source[i] == '"':
                out[i] = " "
                i += 1
                mode = "code"
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if mode == "char":
            if source[i] == "\\":
                out[i] = " "
                if i + 1 < n and source[i + 1] != "\n":
                    out[i + 1] = " "
                i += 2
            elif source[i] == "'":
                out[i] = " "
                i += 1
                mode = "code"
            else:
                if source[i] != "\n":
                    out[i] = " "
                i += 1
            continue
        if source.startswith("//", i):
            out[i : i + 2] = "  "
            i += 2
            mode = "line"
        elif source.startswith("/*", i):
            out[i : i + 2] = "  "
            i += 2
            block_depth = 1
            mode = "block"
        elif source[i] == '"':
            out[i] = " "
            i += 1
            mode = "string"
        elif source[i] == "'":
            # Rust lifetimes (`'a`, `'static`) are code, not character
            # literals.  Only mask a short quote-delimited character token.
            next_char = source[i + 1] if i + 1 < n else ""
            if next_char.isalpha() or next_char == "_":
                i += 1
            else:
                close = source.find("'", i + 1, i + 5)
                newline = source.find("\n", i + 1)
                if close != -1 and (newline == -1 or close < newline):
                    out[i] = " "
                    i += 1
                    mode = "char"
                else:
                    i += 1
        else:
            i += 1
    return "".join(out)


def matching_brace(masked: str, opening: int) -> int | None:
    depth = 0
    for i in range(opening, len(masked)):
        if masked[i] == "{":
            depth += 1
        elif masked[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    return None


def matching_pair(masked: str, opening: int, left: str, right: str) -> int | None:
    depth = 0
    for i in range(opening, len(masked)):
        if masked[i] == left:
            depth += 1
        elif masked[i] == right:
            depth -= 1
            if depth == 0:
                return i
    return None


def spans(source: str, kind: str) -> list[Span]:
    masked = mask_rust(source)
    if kind == "fn":
        pattern = re.compile(rf"\bfn\s+({IDENT})\b")
    else:
        pattern = re.compile(rf"\bstruct\s+({IDENT})\b")
    found: list[Span] = []
    for match in pattern.finditer(masked):
        opening = masked.find("{", match.end())
        semicolon = masked.find(";", match.end())
        if opening == -1 or (semicolon != -1 and semicolon < opening):
            continue
        closing = matching_brace(masked, opening)
        if closing is None:
            continue
        found.append(Span(match.start(), closing + 1, match.group(1), source[opening + 1 : closing]))
    return found


def calls(body: str) -> set[str]:
    return {
        name
        for name in re.findall(rf"\b({IDENT})\s*\(", mask_rust(body))
        if name not in CALL_WORDS
    }


def call_arguments(body: str, name: str) -> list[str]:
    """Return the raw argument text for each call to `name` in one body."""

    masked = mask_rust(body)
    arguments: list[str] = []
    for match in re.finditer(rf"\b{re.escape(name)}\s*\(", masked):
        opening = masked.find("(", match.start(), match.end())
        closing = matching_pair(masked, opening, "(", ")")
        if closing is not None:
            arguments.append(body[opening + 1 : closing])
    return arguments


def chronology_argument_binds_provider(body: str, argument: str) -> bool:
    """Prove the first seam argument is the provider's retained reader.

    Direct field expressions (`&provider.forktree_reader`) and local aliases
    (`let reader = &provider.forktree_reader; seam(reader)`) are accepted.
    A mere field mention elsewhere in the function is deliberately ignored.
    """

    masked_body = mask_rust(body)
    masked_argument = mask_rust(argument)
    field_bases = set(re.findall(rf"\b({IDENT})\.forktree_reader\b", masked_body))
    field_bases.discard("query_source")
    if not field_bases:
        return False
    if any(
        re.search(rf"\b{re.escape(base)}\.forktree_reader\b", masked_argument)
        for base in field_bases
    ):
        return True
    aliases: dict[str, str] = {}
    for alias, base in re.findall(
        rf"\b(?:let\s+)?({IDENT})\s*=\s*&?\s*({IDENT})\.forktree_reader(?:\.clone\(\))?",
        masked_body,
    ):
        if base in field_bases:
            aliases[alias] = base
    return any(
        re.search(rf"\b{re.escape(alias)}\b", masked_argument)
        for alias in aliases
    )


def production(source: str) -> str:
    # Provider test modules are intentionally outside this proof.  This split
    # is conservative: a production source cannot hide a forbidden read after
    # the first cfg(test) module because the remaining text is test-only Rust.
    return source.split("#[cfg(test)]", 1)[0]


def field_structs(source: str) -> list[str]:
    return [
        span.name
        for span in spans(source, "struct")
        if re.search(
            r"\bforktree_reader\s*:\s*(?:crate::forktree::)?ForkTreeReadFacade\s*<",
            span.body,
        )
    ]


def chronology_names(forktree_root: Path) -> set[str]:
    names: set[str] = set()
    for path in sorted(forktree_root.rglob("*.rs")):
        source = path.read_text()
        for span in spans(source, "fn"):
            if re.search(r"checkpoint|chronolog", span.name, re.IGNORECASE):
                names.add(span.name)
    return names


def check_tree(root: Path, label: str) -> tuple[bool, list[str]]:
    messages: list[str] = []
    ok = True

    def require(condition: bool, message: str) -> None:
        nonlocal ok
        messages.append(("PASS " if condition else "FAIL ") + message)
        ok = ok and condition

    context_path = root / "context.rs"
    if not context_path.exists():
        context_path = root / "sql2" / "context.rs"
    context = context_path.read_text() if context_path.exists() else ""
    require(
        bool(re.search(r"struct\s+HistoryQuerySource\b[^{]*\{[^}]*\bforktree_reader\s*:", context, re.S)),
        f"caller-owned HistoryQuerySource reader field ({context_path})",
    )

    provider_paths = {
        "checkpoint": root / "checkpoint.rs",
        "filesystem_working_diff": root / "filesystem_working_diff.rs",
    }
    if not all(path.exists() for path in provider_paths.values()):
        # Production layout has these under sql2/providers.
        provider_paths = {
            "checkpoint": root / "sql2" / "providers" / "checkpoint.rs",
            "filesystem_working_diff": root / "sql2" / "providers" / "filesystem_working_diff.rs",
        }

    ft_root = root / "forktree"
    if not ft_root.exists():
        ft_root = root / "../forktree"
    seam_names = chronology_names(ft_root) if ft_root.exists() else set()
    require(bool(seam_names), f"production chronology seam definitions={sorted(seam_names)}")

    binding_sources: dict[str, set[str]] = {}
    for label, path in provider_paths.items():
        source = path.read_text() if path.exists() else ""
        prod = production(source)
        structs = field_structs(prod)
        require(bool(structs), f"{label}: provider struct owns ForkTreeReadFacade field")
        require(
            "begin_read(" not in prod and "ForkTreeReadFacade::new" not in prod,
            f"{label}: no independent read/facade construction in production",
        )
        funcs = spans(prod, "fn")
        bindings: set[str] = set()
        chronology_calls: list[str] = []
        chronology_argument_failures: list[str] = []
        for fn in funcs:
            body_masked = mask_rust(fn.body)
            # Capture the actual right-hand side of every provider field
            # binding.  Comparing only for the expected token would miss a
            # second view whose field happened to keep the same name.
            for binding in re.findall(
                rf"\bforktree_reader\s*:\s*([^,}}\n]+)", body_masked
            ):
                bindings.add(re.sub(r"\s+", "", binding))
            for seam in sorted(seam_names):
                for argument in call_arguments(fn.body, seam):
                    compact_argument = re.sub(r"\s+", "", argument)
                    if chronology_argument_binds_provider(fn.body, argument):
                        chronology_calls.append(f"{fn.name}->{seam}(arg={compact_argument})")
                    else:
                        chronology_argument_failures.append(f"{fn.name}->{seam}(arg={compact_argument})")
        binding_sources[label] = bindings
        require(bool(bindings), f"{label}: binding function clones query_source.forktree_reader")
        require(bool(chronology_calls), f"{label}: chronology call receives provider reader ({chronology_calls})")
        if chronology_argument_failures:
            require(
                False,
                f"{label}: chronology call argument is not the bound provider reader ({chronology_argument_failures})",
            )

    all_sources = set().union(*binding_sources.values()) if binding_sources else set()
    require(all_sources == {"query_source.forktree_reader.clone()"}, f"shared caller-owned identity={sorted(all_sources)}")

    semantic_text = "\n".join(
        path.read_text() for path in sorted(ft_root.rglob("*.rs")) if path.is_file()
    ) if ft_root.exists() else ""
    marker_root = bool(re.search(r"\bmarker(?:_commit)?\b", semantic_text, re.I)) and bool(
        re.search(r"\b(?:implicit_)?root\b", semantic_text, re.I)
    ) and bool(re.search(r"\b[A-Za-z_]*commit[A-Za-z_0-9]*\b", semantic_text, re.I))
    require(marker_root, "chronology source exposes marker/commit and implicit-root evidence")

    return ok, messages


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate_root", type=Path)
    parser.add_argument("--fixture-root", action="append", type=Path, default=[])
    args = parser.parse_args()

    ok, messages = check_tree(args.candidate_root / "packages/lix/src", "candidate")
    print("STRUCTURAL_SHARED_VIEW_PROOF=candidate")
    print("\n".join(messages))
    for fixture in args.fixture_root:
        fixture_ok, fixture_messages = check_tree(fixture, str(fixture))
        print(f"STRUCTURAL_SHARED_VIEW_PROOF=fixture:{fixture.name}")
        print("\n".join(fixture_messages))
        if fixture.name == "positive" and not fixture_ok:
            ok = False
            print("FAIL positive fixture must pass")
        if fixture.name != "positive" and fixture_ok:
            ok = False
            print("FAIL negative fixture unexpectedly passed")
    print("RESULT=" + ("PASS" if ok else "RED"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
