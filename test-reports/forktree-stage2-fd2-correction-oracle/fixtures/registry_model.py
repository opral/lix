"""Executable registry-integrity model for the fd2 source oracle."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable


EXPECTED_KEY = "plugin-registry"


class RegistryError(Exception):
    """Every malformed or substituted registry entry fails closed."""


@dataclass(frozen=True)
class RegistryEntry:
    key: str
    schema_keys: tuple[str, ...]


def parse_authenticated_registry(raw: Any, expected_key: str) -> RegistryEntry:
    if raw is None:
        raise RegistryError("missing")
    if not isinstance(raw, dict):
        raise RegistryError("malformed")
    if raw.get("kind") != "plugin_registry":
        raise RegistryError("wrong-kind")
    if raw.get("id") != expected_key:
        raise RegistryError("substituted")
    schema_keys = raw.get("schema_keys")
    if not isinstance(schema_keys, list) or not all(
        isinstance(value, str) for value in schema_keys
    ):
        raise RegistryError("malformed")
    return RegistryEntry(expected_key, tuple(schema_keys))


def valid_fixture(schema_keys: list[str] | None = None) -> dict[str, Any]:
    return {
        "kind": "plugin_registry",
        "id": EXPECTED_KEY,
        "schema_keys": ["plugin_schema"] if schema_keys is None else schema_keys,
    }


def assert_failure(name: str, mutation: Callable[[dict[str, Any]], Any]) -> None:
    fixture = mutation(deepcopy(valid_fixture()))
    try:
        parse_authenticated_registry(fixture, EXPECTED_KEY)
    except RegistryError as error:
        if str(error) not in {"missing", "wrong-kind", "malformed", "substituted"}:
            raise AssertionError(f"{name} returned an unexpected failure: {error}")
    else:
        raise AssertionError(f"{name} was accepted")


def main() -> int:
    valid = parse_authenticated_registry(valid_fixture(), EXPECTED_KEY)
    assert valid.schema_keys == ("plugin_schema",)

    empty = parse_authenticated_registry(valid_fixture([]), EXPECTED_KEY)
    assert empty.schema_keys == (), "authenticated present-empty registry is valid"

    mutations: dict[str, Callable[[dict[str, Any]], Any]] = {
        "missing": lambda _entry: None,
        "wrong-kind": lambda entry: entry | {"kind": "other"},
        "malformed": lambda entry: entry | {"schema_keys": "not-a-list"},
        "substituted": lambda entry: entry | {"id": "other-registry"},
    }
    for name, mutation in mutations.items():
        assert_failure(name, mutation)

    print("PASS registry-valid")
    print("PASS registry-explicit-empty")
    for name in mutations:
        print(f"PASS registry-fail-closed-{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
