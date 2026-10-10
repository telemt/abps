#!/usr/bin/env python3
"""BR regressions must detect broken transitions in isolated source/target trees."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from mutation_support import run_case

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--discard-builds", action="store_true", help="Remove this run\'s build artifacts after preserving logs/source")
    args = p.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    native = ["--features", "local-branches-spike", "--lib"]
    cases = [
        ("missing-local-increment", "src/mosaic/protocol/branches.rs", "self.0.set(next);", "let _ = next;",
         native, "mosaic::raw::branches::tests::br02_checked_cell_transition_and_limit", {}),
        ("nonunique-migration", "src/mosaic/protocol/branches.rs", "self.0.get() == 1", "self.0.get() > 0",
         native, "mosaic::raw::branches::tests::br02_checked_cell_transition_and_limit", {}),
        ("lost-guard-rights", "src/mosaic/raw/branches.rs", "0..self.pending", "0..0",
         native, "mosaic::raw::branches::tests::br01_unwind_at_every_materialization_prefix_releases_all_rights", {}),
        ("lost-staged-family", "src/mosaic/raw/branches.rs", "drop(self.staged.take());", "std::mem::forget(self.staged.take());",
         native, "mosaic::raw::branches::tests::br01_source_reserved_staged_and_installed_boundaries_conserve_rights", {}),
        ("lost-source-right", "src/mosaic/raw/branches.rs", "pending: 1,", "pending: 0,",
         native, "mosaic::raw::branches::tests::br01_source_reserved_staged_and_installed_boundaries_conserve_rights", {}),
        ("missing-final-acquire", "src/mosaic/protocol.rs", "fence(Ordering::Acquire);", "// missing synchronization",
         ["--features", "local-branches-spike,loom-model", "--lib"],
         "mosaic::protocol::tests::local_branches::br03_local_readers_happen_before_slot_and_sidecar_reuse_without_join", {}),
        ("relaxed-final-load", "src/mosaic/protocol.rs", "let _ = self.0.load(Ordering::Acquire);", "let _ = self.0.load(Ordering::Relaxed);",
         ["--features", "local-branches-spike,loom-model", "--lib"],
         "mosaic::protocol::tests::local_branches::br03_local_readers_happen_before_slot_and_sidecar_reuse_without_join",
         {"RUSTFLAGS":"--cfg abps_tsan", "CARGO_ENCODED_RUSTFLAGS":"--cfg\x1fabps_tsan"}),
        ("early-domain-recycle", "src/mosaic/protocol.rs", "self.admitted.load(Ordering::Acquire) == 0", "true",
         ["--features", "local-branches-spike,loom-model", "--lib"],
         "mosaic::protocol::tests::si11_reuse_before_old_refund_keeps_old_charge_pinned", {}),
        ("refund-before-publication", "src/mosaic/protocol.rs",
         [("self.class.publish(self.bit);", "self.domain.refund(self.charge);\n        self.class.publish(self.bit);"),
          ("        self.domain.refund(self.charge);\n\n        // SI-11:", "        // SI-11:")], None,
         ["--features", "local-branches-spike,loom-model", "--lib"],
         "mosaic::protocol::tests::si11_reuse_before_old_refund_keeps_old_charge_pinned", {}),
    ]
    results = []
    for name, file, old, new, flags, test, overrides in cases:
        directory = args.output / name
        source = directory / "source"
        source.mkdir(parents=True)
        for child in ["src", "examples", "tests", "benches", "verification", "build.rs", "Cargo.toml", "Cargo.lock"]:
            if (ROOT / child).is_dir():
                shutil.copytree(ROOT / child, source / child, ignore=shutil.ignore_patterns("artifacts", "target", "__pycache__"))
            else:
                shutil.copy2(ROOT / child, source / child)
        edits = old if isinstance(old, list) else [(old, new)]
        env = dict(os.environ, CARGO_TARGET_DIR=str((directory / "target").resolve()), RUSTFLAGS="", CARGO_ENCODED_RUSTFLAGS="") | overrides
        command = ["cargo", "test", "--locked", *flags, test, "--", "--exact"]
        result = run_case(name,source,file,edits,command,test,env)
        caught = result["caught"]
        results.append(result)
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(name, caught, flush=True)
        if args.discard_builds:
            shutil.rmtree(Path(env["CARGO_TARGET_DIR"]), ignore_errors=True)
        if not caught:
            raise RuntimeError(f"mutation not caught by test: {name}")


if __name__ == "__main__":
    main()
