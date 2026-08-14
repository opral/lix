#!/usr/bin/env python3
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("native_row_v2_acceptance.py")
SPEC = importlib.util.spec_from_file_location("native_row_v2_acceptance", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class NativeRowV2ModelTests(unittest.TestCase):
    def test_branch_uuid_is_not_an_identity_input(self):
        first = MODULE.digest("local", "s", b"pk", "f")
        child = MODULE.digest("local", "s", b"pk", "f")
        self.assertEqual(first, child)

    def test_domain_and_key_transplants_are_distinct(self):
        baseline = MODULE.digest("local", "s", b"pk", "f")
        for value in (
            MODULE.digest("global", "s", b"pk", "f"),
            MODULE.digest("local", "other", b"pk", "f"),
            MODULE.digest("local", "s", b"pK", "f"),
            MODULE.digest("local", "s", b"pk", "g"),
        ):
            self.assertNotEqual(baseline, value)

    def test_length_framing_rejects_graft_equivalence(self):
        self.assertNotEqual(
            MODULE.digest("local", "ab", b"c", None),
            MODULE.digest("local", "a", b"bc", None),
        )


if __name__ == "__main__":
    unittest.main()
