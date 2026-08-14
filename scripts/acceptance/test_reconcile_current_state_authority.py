#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "reconcile_current_state_authority", HERE / "reconcile_current_state_authority.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class Helpers(unittest.TestCase):
    def test_body_extracts_nested_function(self) -> None:
        text = "fn selected() { if true { nested(); } done(); } fn other() {}"
        self.assertEqual(
            MODULE.body(text, "selected"),
            "fn selected() { if true { nested(); } done(); }",
        )

    def test_ordered_requires_presence_and_order(self) -> None:
        self.assertTrue(MODULE.ordered("a b c", "a", "b", "c"))
        self.assertFalse(MODULE.ordered("a c b", "a", "b", "c"))
        self.assertFalse(MODULE.ordered("a b", "a", "b", "c"))


if __name__ == "__main__":
    unittest.main()
