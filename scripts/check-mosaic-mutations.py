#!/usr/bin/env python3
"""Negative controls in disposable source trees AND independent Cargo targets.

Never mutate the working tree or share its Cargo fingerprints. Each negative
control must compile, run its named regression and fail that test (not time out).
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from mutation_support import run_case

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--discard-builds", action="store_true", help="Remove this run\'s build artifacts after preserving logs/source")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    cases = [
        ("berry-missing-depletion-confirmation", "src/berry/protocol.rs",
         "self.0.fetch_or(0, Ordering::Acquire) & mask", "hinted",
         ["--features", "loom-model", "--lib", "berry::protocol::tests::si01_confirmation_rechecks_a_stale_empty_hint"], {}),
        ("missing-final-acquire", "src/mosaic/protocol.rs", "fence(Ordering::Acquire);", "// intentionally missing acquire fence",
         ["--features", "mosaic-spike,loom-model", "--lib", "mosaic::protocol::tests::final_reader_happens_before_overwrite_without_join_synchronization"], {}),
        ("payload-word-corruption", "src/mosaic/raw.rs", "ptr.cast::<usize>().write_volatile(0);", "ptr.cast::<usize>().write_volatile(usize::MAX);",
         ["--features", "mosaic-spike", "--lib", "mosaic::raw::tests::si17_zero_payload_checked_ranges_and_fresh_lease_state"], {}),
        ("stale-registry-refund", "examples/spike_support/supervisor.rs",
         "fn refund(&self, id: Identity) {\n        let mut s = self.state.lock().unwrap();\n        let r = &mut s.records[id.index];\n        if r.generation != id.generation ||",
         "fn refund(&self, id: Identity) {\n        let mut s = self.state.lock().unwrap();\n        let r = &mut s.records[id.index];\n        if",
         ["--features", "bytes-interop,mosaic-spike", "--example", "spike_probe", "spike_support::supervisor::tests::unconfirmed_charged_until_proof_and_reused_record_rejects_old_identity"], {}),
        ("berry-missing-final-acquire", "src/berry/protocol.rs", "fence(Ordering::Acquire);", "// intentionally missing acquire fence",
         ["--features", "loom-model", "--lib", "berry::protocol::tests::final_reader_happens_before_overwrite_without_join_synchronization"], {}),
    ]
    for backend in ["berry", "mosaic"]:
        cases.append((backend+"-relaxed-final-load", f"src/{backend}/protocol.rs",
                      "let _ = self.0.load(Ordering::Acquire);", "let _ = self.0.load(Ordering::Relaxed);",
                      ["--features", "mosaic-spike,loom-model", "--lib", backend+"::protocol::tests::final_reader_happens_before_overwrite_without_join_synchronization"],
                      {"RUSTFLAGS":"--cfg abps_tsan", "CARGO_ENCODED_RUSTFLAGS":"--cfg\x1fabps_tsan"}))
    results = []
    for name, file, old, new, test, overrides in cases:
        copy = args.output / name / "source"
        shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns("target", ".git", "__pycache__", "artifacts", "mutants.out", "mutants.out.old"))
        env = dict(os.environ, CARGO_TARGET_DIR=str((copy.parent / "target").resolve()), RUSTFLAGS="", CARGO_ENCODED_RUSTFLAGS="") | overrides
        command = ["cargo", "test", "--locked", *test, "--", "--exact"]
        result = run_case(name,copy,file,[(old,new)],command,test[-1],env)
        caught = result["caught"]
        results.append(result)
        (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(name, caught, flush=True)
        if args.discard_builds:
            shutil.rmtree(Path(env["CARGO_TARGET_DIR"]), ignore_errors=True)
        if not caught:
            raise RuntimeError(f"negative control not detected by its regression: {name}")


if __name__ == "__main__":
    main()
