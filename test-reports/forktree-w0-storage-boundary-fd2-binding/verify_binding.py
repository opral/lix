#!/usr/bin/env python3
"""Static TEST/REPORT-only binding verifier for W0 v3 and exact fd2."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


W0 = "6a91df3f88177e9b6d53d20d5ba6554df8fd6b9a"
W0_PARENT = "dc4323d56be98237c54099c67d46bfc0e3b2ef63"
W0_TREE = "0d194d75190caca4219779edd87469c57f9db8b8"
FD2 = "fd2be256d763f17e9f127d4c984e36fba191cb82"
FD2_PARENT = "cd91b9b90f7f468158b4df154adbed9551eb5d60"
FD2_TREE = "20110ca5e3c33d34217630fff0a2b784b545317a"
PACKAGE = "e2503fd1d43b95d3ebfd133b9868a4be0647ee3d"
PACKAGE_PARENT = FD2
PACKAGE_TREE = "9223d01c5c38457edbe3048f12d90f2305f84a31"
PACKAGE_DIR = "test-reports/forktree-w0-storage-boundary-fd2-binding"
FD2_PACKAGE_DIR = "test-reports/forktree-stage2-fd2-correction-oracle"

W0_BLOBS = {
    "packages/engine-benchmarks/tests/FORKTREE_W0_STORAGE_BOUNDARY_ORACLE.md": "995174f18a82af861e3b647f85f883bab24f6a96",
    "packages/engine-benchmarks/tests/FORKTREE_W0_STORAGE_BOUNDARY_ORACLE_MANIFEST.txt": "5cc670d8456b82ee7079f28921406574ec6c0b96",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/README.md": "0102577305224e2d58ce47e91134e291ca811feb",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_binary_cas_owner.rs": "f7a892f1a62f37274b44df76c6fdbdfc2eeaff7b",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_columnar_owner.rs": "ad89aa4bb811431f20d1dfaecc245cd2f8f04a16",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_legacy_owner.rs": "57cfbd3a2cf3cef8906fb43cfd12cc50f9b13bb0",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_native_exports.ts": "1aa018a4da64f1dce947af3a426bc61aa790fb6d",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_raw_space.rs": "2462dac63da23c251749721dddda88f29bc3ffc2",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_tracked_changelog.rs": "0c13004f2d88abae5185e02e101a6d6262a2b200",
    "packages/engine-benchmarks/tests/forktree_w0_compile_probes/positive_descriptor.rs": "afcc12a740b84eb5d05e3716dfe98c07d52118f1",
    "packages/engine-benchmarks/tests/forktree_w0_storage_boundary_oracle.rs": "c81e49ba481bb963f902a1c99711efe884f882de",
    "scripts/forktree_w0_compile_probes.sh": "2c077f7e2769da89b3f28ceca345d3ed81be1427",
    "scripts/forktree_w0_storage_boundary_residue_verify.mjs": "8ba5360742818974f2bf94308e94c08504e11b07",
}

FD2_PRODUCTION_PATHS = {
    "packages/lix/src/forktree/state.rs",
    "packages/lix/src/forktree/view.rs",
    "packages/lix/src/session/checkpoint.rs",
    "packages/lix/src/sql2/providers/checkpoint.rs",
    "packages/lix/src/sql2/providers/directory_history.rs",
    "packages/lix/src/sql2/providers/file_history.rs",
    "packages/lix/src/sql2/providers/filesystem_working_diff.rs",
    "packages/lix/src/sql2/providers/mod.rs",
    "packages/lix/src/sql2/providers/working_diff.rs",
    "packages/lix/src/transaction/context.rs",
}


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=root, text=True, capture_output=True, check=True
    )
    return result.stdout


def show(root: Path, revision: str, path: str) -> str:
    return git(root, "show", f"{revision}:{path}")


def check(label: str, condition: bool, detail: str, failures: list[str]) -> None:
    status = "PASS" if condition else "RED"
    print(f"{status}\t{label}\t{detail}")
    if not condition:
        failures.append(label)


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    failures: list[str] = []

    for label, revision, parent, tree in (
        ("w0-provenance", W0, W0_PARENT, W0_TREE),
        ("fd2-provenance", FD2, FD2_PARENT, FD2_TREE),
        ("fd2-report-package", PACKAGE, PACKAGE_PARENT, PACKAGE_TREE),
    ):
        actual_tree = git(root, "rev-parse", f"{revision}^{{tree}}").strip()
        actual_parents = git(root, "show", "-s", "--format=%P", revision).strip()
        check(
            f"{label}-tree",
            actual_tree == tree,
            f"{revision[:12]} tree={actual_tree}",
            failures,
        )
        check(
            f"{label}-parent",
            actual_parents.split()[:1] == [parent],
            f"{revision[:12]} parent={actual_parents}",
            failures,
        )

    tracked_w0 = git(root, "ls-tree", "-r", "--name-only", W0).splitlines()
    expected_w0 = set(W0_BLOBS)
    check(
        "w0-source-file-set",
        expected_w0.issubset(tracked_w0),
        f"expected={len(expected_w0)} present={len(set(tracked_w0) & expected_w0)}",
        failures,
    )
    for path, expected_blob in W0_BLOBS.items():
        rows = git(root, "ls-tree", W0, "--", path).splitlines()
        actual_blob = rows[0].split()[2] if rows else "missing"
        check(
            f"w0-blob:{path}",
            actual_blob == expected_blob,
            f"blob={actual_blob}",
            failures,
        )

    w0_doc = show(root, W0, "packages/engine-benchmarks/tests/FORKTREE_W0_STORAGE_BOUNDARY_ORACLE.md")
    w0_manifest = show(
        root,
        W0,
        "packages/engine-benchmarks/tests/FORKTREE_W0_STORAGE_BOUNDARY_ORACLE_MANIFEST.txt",
    )
    check(
        "w0-count-provenance",
        "598 scanned source files / 607 tracked source files" in w0_doc
        and "residues 955" in w0_doc,
        "607 tracked / 598 scanned / 955 residues",
        failures,
    )
    check(
        "w0-model-evidence",
        "corrected_model_tests=6/6 GREEN" in w0_manifest
        and "d2955ecca3d9f66b9eff72950bf688e9b462581de2205783a17ad0d5e86adfe8" in w0_manifest,
        "6/6 model and binary identity bound",
        failures,
    )
    check(
        "w0-wrong-domain-reopen",
        "reopen(wrong_domain_id,complete_wrong_domain_bytes)=WrongDomain" in w0_manifest
        and "WrongDomain" in show(
            root,
            W0,
            "packages/engine-benchmarks/tests/forktree_w0_storage_boundary_oracle.rs",
        ),
        "reopen wrong-domain is distinct and typed",
        failures,
    )

    probe_script = show(root, W0, "scripts/forktree_w0_compile_probes.sh")
    rust_requirements = (
        "rust-negative-raw-space E0423 SpaceId",
        "rust-negative-columnar-owner E0599 load_columnar_row_group",
        "rust-negative-tracked-changelog E0599 load_commit_state_manifest,load_tracked_state",
        "rust-negative-binary-cas-owner E0599 load_binary_cas_manifest",
        "rust-negative-legacy-owner E0599 load_branch_head_control",
    )
    check(
        "rust-negative-diagnostics",
        all(requirement in probe_script for requirement in rust_requirements)
        and "rust-positive-descriptor" in probe_script
        and "rust-positive-oracle" in probe_script,
        "five compiler-fail probes plus positive controls",
        failures,
    )

    ts_probe = show(
        root,
        W0,
        "packages/engine-benchmarks/tests/forktree_w0_compile_probes/negative_native_exports.ts",
    )
    check(
        "typescript-actual-source-probe",
        "from \"../../../js-sdk/src/open-lix.js\"" in ts_probe
        and "from \"../../../js-sdk/src/binding-types.js\"" in ts_probe
        and not any(line.lstrip().startswith("declare ") for line in ts_probe.splitlines()),
        "probe imports actual JS SDK types without declaring removed members",
        failures,
    )
    check(
        "typescript-native-diagnostic",
        "TS2339" in probe_script
        and all(
            token in probe_script
            for token in (
                "syncAllFiles",
                "lixDir",
                "importFilesystemPaths",
                "syncDiskToLix",
            )
        ),
        "TS2339 and all removed JS/native export tokens required",
        failures,
    )
    check(
        "native-registration-absence",
        "native-rust-exports EXPECT_ABSENT" in probe_script
        and "packages/js-sdk/native/napi.rs" in show(
            root,
            W0,
            "packages/engine-benchmarks/tests/forktree_w0_compile_probes/README.md",
        ),
        "native registration scan is separate from TypeScript failure",
        failures,
    )

    fd2_file = show(root, FD2, "packages/lix/src/sql2/providers/file_history.rs")
    check(
        "fd2-known-fallback-diagnostic",
        "fn file_history_owner_schema_keys" in fd2_file
        and ".unwrap_or_else(|| owner.schema_keys())" in fd2_file,
        "expected fd2 owner.schema_keys fallback remains explicitly diagnosed",
        failures,
    )
    fd2_paths = set(git(root, "diff-tree", "--no-commit-id", "--name-only", "-r", f"{FD2}^", FD2).splitlines())
    check(
        "fd2-source-scope",
        fd2_paths == FD2_PRODUCTION_PATHS,
        f"production paths={len(fd2_paths)} exact={len(FD2_PRODUCTION_PATHS)}",
        failures,
    )

    changed = git(root, "diff", "--name-only", f"{PACKAGE}..HEAD").splitlines()
    check(
        "binding-report-only-scope",
        all(path.startswith(PACKAGE_DIR + "/") for path in changed),
        f"changed={len(changed)} production_changes=0",
        failures,
    )

    if failures:
        print("RED\tbinding-summary\t" + ",".join(failures))
        return 1
    print("PASS\tbinding-summary\tW0 v3 bound to exact fd2; no production/runtime claim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
