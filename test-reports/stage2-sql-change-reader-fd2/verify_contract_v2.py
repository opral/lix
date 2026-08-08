#!/usr/bin/env python3
"""Executable fixture model and balanced source contract for the fd2 oracle.

This is intentionally test/report-only.  It does not import or execute the
production crate.  The source checks operate on Rust tokens with comments and
string literals removed, then use balanced delimiters to keep proof local to
the relevant functions/calls.
"""

from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


class ContractError(Exception):
    pass


def need(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def obj(value: object, label: str) -> dict:
    need(isinstance(value, dict), f"{label} must be an object")
    return value


def text_field(value: dict, key: str, label: str) -> str:
    result = value.get(key)
    need(isinstance(result, str) and result != "", f"{label}.{key} must be nonempty text")
    return result


def model_case(case_id: str, expected: str, fixture: dict) -> None:
    case = fixture.get("case")
    need(case == case_id, f"case field is {case!r}, expected {case_id!r}")
    need(fixture.get("expected") == expected, f"expected field mismatch for {case_id}")

    if case_id == "direct_change":
        request = obj(fixture.get("request"), "request")
        catalog = obj(fixture.get("catalog"), "catalog")
        need(text_field(request, "change_id", "request") == text_field(catalog, "change_id", "catalog"),
             "direct request and catalog ChangeId differ")
        need(catalog.get("domain") == "ChangeCatalog", "direct record has wrong domain")
        need(catalog.get("kind") == "ChangeRecord", "direct record has wrong kind")
        need(catalog.get("authenticated") is True, "direct record is not authenticated")
        return

    if case_id == "derived_commit":
        request = obj(fixture.get("request"), "request")
        catalog = obj(fixture.get("catalog"), "catalog")
        need(text_field(request, "commit_id", "request") == text_field(catalog, "commit_id", "catalog"),
             "derived request and CommitCatalog CommitId differ")
        need(catalog.get("domain") == "CommitCatalog", "derived record has wrong domain")
        need(catalog.get("kind") == "CommitRecord", "derived record has wrong kind")
        need(catalog.get("authenticated") is True, "derived record is not authenticated")
        need(isinstance(catalog.get("parents"), list), "derived commit parents are not typed")
        need(text_field(catalog, "change_id", "catalog"), "derived commit lacks canonical ChangeId")
        return

    if case_id == "absent_key":
        request = obj(fixture.get("request"), "request")
        catalog = obj(fixture.get("catalog"), "catalog")
        text_field(request, "change_id", "request")
        need(catalog.get("state") == "authenticated_absence", "absence is not authenticated")
        need("record" not in catalog, "authenticated absence must not contain a record")
        return

    if case_id == "missing_catalog_record":
        catalog = obj(fixture.get("catalog"), "catalog")
        need(catalog.get("enumerated") is True, "missing case did not enumerate the catalog key")
        need(catalog.get("record") is None, "missing case unexpectedly has a record")
        need(fixture.get("failure") == "typed_corruption", "missing case lacks typed corruption")
        return

    if case_id == "malformed_change":
        catalog = obj(fixture.get("catalog"), "catalog")
        need(catalog.get("domain") == "ChangeCatalog", "malformed case changed domain")
        need(catalog.get("kind") == "ChangeRecord", "malformed case changed kind")
        need(catalog.get("encoding") in {"truncated", "invalid_version", "invalid_checksum"},
             "malformed case is not an actual codec failure")
        need(fixture.get("failure") == "typed_corruption", "malformed case lacks typed corruption")
        return

    if case_id == "wrong_kind_or_domain":
        request = obj(fixture.get("request"), "request")
        catalog = obj(fixture.get("catalog"), "catalog")
        requested = text_field(request, "change_id", "request")
        embedded = text_field(catalog, "embedded_change_id", "catalog")
        need(requested == embedded, "wrong-kind control must not also be an identity mismatch")
        need(catalog.get("domain") != "ChangeCatalog" or catalog.get("kind") != "ChangeRecord",
             "wrong-kind control is actually a valid ChangeRecord")
        need(fixture.get("failure") == "typed_corruption", "wrong-kind case lacks typed corruption")
        return

    if case_id == "wrong_embedded_change_id":
        request = obj(fixture.get("request"), "request")
        catalog = obj(fixture.get("catalog"), "catalog")
        need(text_field(request, "change_id", "request") != text_field(catalog, "embedded_change_id", "catalog"),
             "identity control is not substituted")
        need(catalog.get("domain") == "ChangeCatalog", "identity control changed domain")
        need(catalog.get("kind") == "ChangeRecord", "identity control changed kind")
        need(fixture.get("failure") == "typed_identity_error", "identity case lacks typed identity error")
        return

    if case_id == "duplicate_change_id":
        records = fixture.get("records")
        need(isinstance(records, list) and len(records) >= 2, "duplicate control needs two records")
        ids = [text_field(obj(record, "record"), "change_id", "record") for record in records]
        need(len(ids) != len(set(ids)), "duplicate control contains no duplicate logical ID")
        need(all(obj(record, "record").get("authenticated") is True for record in records),
             "duplicate records are not authenticated")
        need(fixture.get("failure") == "typed_corruption_before_output",
             "duplicate case lacks pre-output typed corruption")
        return

    if case_id == "limit_after_merge":
        direct = fixture.get("direct")
        derived = fixture.get("derived")
        need(isinstance(direct, list) and isinstance(derived, list), "merge inputs must be lists")
        records = [obj(record, "direct/derived record") for record in direct + derived]
        ids = [text_field(record, "change_id", "record") for record in records]
        need(all(record.get("authenticated") is True for record in records), "merge has unauthenticated row")
        need(len(ids) == len(set(ids)), "limit control contains an unhandled duplicate")
        limit = fixture.get("limit")
        need(isinstance(limit, int) and limit >= 0, "limit is not a nonnegative integer")
        sorted_ids = sorted(ids)
        expected_ids = fixture.get("expected_ids")
        need(sorted_ids[:limit] == expected_ids, "canonical sort/limit result is wrong")
        need(ids != sorted_ids, "limit control must be non-canonical before merge")
        return

    if case_id == "same_read_no_fallback":
        need(fixture.get("read_acquisitions") == 1, "read acquisition count is not exactly one")
        ids = obj(fixture.get("reader_ids"), "reader_ids")
        need(ids.get("source") == ids.get("scan") == ids.get("exact"),
             "source, scan, and exact readers do not share one identity")
        forbidden = obj(fixture.get("forbidden"), "forbidden")
        need(all(value is False for value in forbidden.values()), "fallback/second-read flag is enabled")
        return

    raise ContractError(f"unimplemented fixture case {case_id}")


def run_fixture_model(package: Path) -> bool:
    cases_path = package / "SQL_CHANGE_READER_CASES.tsv"
    fixtures = package / "fixtures"
    failures = 0
    with cases_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            case_id = row["id"]
            fixture_path = fixtures / row["fixture"]
            try:
                with fixture_path.open(encoding="utf-8") as fixture_handle:
                    fixture = json.load(fixture_handle)
                model_case(case_id, row["expected"], fixture)
            except (OSError, json.JSONDecodeError, ContractError, KeyError) as error:
                print(f"MODEL-RED {case_id} {error}")
                failures += 1
            else:
                print(f"MODEL-PASS {case_id}")
    return failures == 0


@dataclass(frozen=True)
class Token:
    text: str
    start: int
    end: int


IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def skip_quoted(source: str, index: int, quote: str) -> int:
    index += 1
    while index < len(source):
        if source[index] == "\\":
            index += 2
        elif source[index] == quote:
            return index + 1
        else:
            index += 1
    return len(source)


def tokenize(source: str) -> list[Token]:
    tokens: list[Token] = []
    index = 0
    while index < len(source):
        if source[index].isspace():
            index += 1
            continue
        if source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
            continue
        if source.startswith("/*", index):
            depth = 1
            index += 2
            while index < len(source) and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        if source[index] == '"':
            index = skip_quoted(source, index, '"')
            continue
        if source[index] == "'":
            # Rust lifetimes are not quoted literals.  Preserve the apostrophe
            # as punctuation so a lifetime cannot hide later function tokens;
            # only a one-character/escaped character literal is skipped.
            if index + 2 < len(source) and source[index + 2] == "'":
                index = skip_quoted(source, index, "'")
                continue
            tokens.append(Token("'", index, index + 1))
            index += 1
            continue
        raw = re.match(r"r(#+)\"", source[index:])
        if raw:
            hashes = raw.group(1)
            terminator = '"' + hashes
            start = index
            end = source.find(terminator, index + len(raw.group(0)))
            index = len(source) if end < 0 else end + len(terminator)
            continue
        match = IDENT.match(source, index)
        if match:
            tokens.append(Token(match.group(0), match.start(), match.end()))
            index = match.end()
            continue
        # Keeping punctuation as one-character tokens makes delimiter
        # balancing and first-argument extraction deterministic.
        tokens.append(Token(source[index], index, index + 1))
        index += 1
    return tokens


PAIRS = {"{": "}", "(": ")", "[": "]"}


def matching(tokens: list[Token], opening: int) -> int:
    close = PAIRS[tokens[opening].text]
    stack = [close]
    for index in range(opening + 1, len(tokens)):
        text = tokens[index].text
        if text in PAIRS:
            stack.append(PAIRS[text])
        elif stack and text == stack[-1]:
            stack.pop()
            if not stack:
                return index
    raise ContractError(f"unbalanced delimiter at token {opening}")


def function_bodies(tokens: list[Token], name: str) -> list[tuple[int, int]]:
    result = []
    for index, token in enumerate(tokens):
        if token.text != "fn" or index + 1 >= len(tokens) or tokens[index + 1].text != name:
            continue
        brace = index + 2
        while brace < len(tokens) and tokens[brace].text not in {"{", ";"}:
            brace += 1
        need(brace < len(tokens) and tokens[brace].text == "{", f"{name} has no body")
        result.append((brace, matching(tokens, brace)))
    return result


def call_arguments(tokens: list[Token], name: str) -> list[list[Token]]:
    calls: list[list[Token]] = []
    for index, token in enumerate(tokens[:-1]):
        if token.text != name or tokens[index + 1].text != "(":
            continue
        end = matching(tokens, index + 1)
        args: list[list[Token]] = []
        start = index + 2
        depth = 0
        for cursor in range(start, end + 1):
            text = tokens[cursor].text if cursor < end else ","
            if text in PAIRS:
                depth += 1
            elif text in PAIRS.values():
                depth -= 1
            if text == "," and depth == 0:
                args.append(tokens[start:cursor])
                start = cursor + 1
        calls.append(args)
    return calls


def normalized(tokens: list[Token]) -> str:
    return "".join(token.text for token in tokens)


def struct_has_field(tokens: list[Token], name: str, field: str) -> bool:
    for index, token in enumerate(tokens[:-1]):
        if token.text != "struct" or tokens[index + 1].text != name:
            continue
        brace = index + 2
        while brace < len(tokens) and tokens[brace].text != "{":
            brace += 1
        need(brace < len(tokens), f"struct {name} has no body")
        end = matching(tokens, brace)
        body = tokens[brace + 1 : end]
        return any(body[cursor].text == field and body[cursor + 1].text == ":"
                   for cursor in range(len(body) - 1))
    return False


def constructor_proof(path: Path, label: str) -> None:
    source = path.read_text(encoding="utf-8")
    tokens = tokenize(source)
    bodies = function_bodies(tokens, "changelog_query_source")
    need(len(bodies) == 1, f"{label}: expected exactly one changelog_query_source body")
    opening, closing = bodies[0]
    body = tokens[opening + 1 : closing]
    literal_positions = [index for index, token in enumerate(body[:-1])
                         if token.text == "ChangelogQuerySource" and body[index + 1].text == "{"]
    need(len(literal_positions) == 1, f"{label}: expected one ChangelogQuerySource literal")
    literal_open = literal_positions[0] + 1
    literal_end = matching(body, literal_open)
    literal = body[literal_open + 1 : literal_end]
    field_positions = [index for index, token in enumerate(literal[:-1])
                       if token.text == "forktree_reader" and literal[index + 1].text == ":"]
    need(len(field_positions) == 1, f"{label}: expected one forktree_reader initializer")
    value_start = field_positions[0] + 2
    value_end = len(literal)
    depth = 0
    for index in range(value_start, len(literal)):
        text = literal[index].text
        if text in PAIRS:
            depth += 1
        elif text in PAIRS.values():
            depth -= 1
        elif text == "," and depth == 0:
            value_end = index
            break
    value = normalized(literal[value_start:value_end])
    need("read_store" in value and "ForkTreeReadFacade" in value,
         f"{label}: facade is not constructed from self.read_store")
    need("begin_read" not in value and "StorageAdapter" not in value,
         f"{label}: constructor acquires a fresh/raw read")
    need(not any(token.text == "begin_read" for token in body),
         f"{label}: changelog source constructor contains a second read acquisition")


def structural_proof(candidate: Path) -> None:
    root = candidate / "packages/lix/src"
    provider_path = root / "sql2/providers/change.rs"
    context_path = root / "sql2/context.rs"
    provider = tokenize(provider_path.read_text(encoding="utf-8"))
    context = tokenize(context_path.read_text(encoding="utf-8"))
    need(struct_has_field(context, "ChangelogQuerySource", "forktree_reader"),
         "ChangelogQuerySource lacks a forktree_reader field")

    for name in ("scan_changelog_changes", "load_exact_change"):
        calls = call_arguments(provider, name)
        need(calls, f"provider has no call to {name}")
        matches = [args for args in calls if args and normalized(args[0]) == "&query_source.forktree_reader"]
        need(len(matches) == len(calls), f"not every {name} call uses the retained reader")
        bodies = function_bodies(provider, name)
        need(len(bodies) == 1, f"provider has unexpected {name} definition count")
        definition_start = next(index for index, token in enumerate(provider)
                                if token.text == "fn" and index + 1 < len(provider)
                                and provider[index + 1].text == name)
        signature = provider[definition_start : bodies[0][0]]
        need("ForkTreeReadFacade" in normalized(signature),
             f"{name} definition is not typed to the ForkTree facade")

    constructor_proof(root / "session/context.rs", "session/context.rs")
    constructor_proof(root / "transaction/context.rs", "transaction/context.rs")
    print("STRUCTURAL-PASS one facade field, balanced call arguments, and two retained-read constructors")


def main(argv: list[str]) -> int:
    if len(argv) == 3 and argv[1] == "--model-only":
        return 0 if run_fixture_model(Path(argv[2]).resolve()) else 1
    if len(argv) != 2:
        print(f"usage: {argv[0]} <candidate-worktree>", file=sys.stderr)
        return 2
    candidate = Path(argv[1]).resolve()
    package = Path(__file__).resolve().parent
    model_ok = run_fixture_model(package)
    try:
        structural_proof(candidate)
    except (OSError, ContractError, KeyError, IndexError) as error:
        print(f"STRUCTURAL-RED {error}")
        structural_ok = False
    else:
        structural_ok = True
    return 0 if model_ok and structural_ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
