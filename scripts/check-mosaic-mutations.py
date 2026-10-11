#!/usr/bin/env python3
"""Negative controls in disposable source trees AND independent Cargo targets.

Never mutate the working tree or share its Cargo fingerprints. Each negative
control must compile, run its named regression and fail that test (not time out).

On Windows keep the absolute --output path within 155 characters: each control
builds in <output>/target/<hash>, and MSVC link.exe cannot create a build-script
executable beyond MAX_PATH.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
from mutation_support import create_case_target, run_case

ROOT = Path(__file__).resolve().parents[1]
NIGHTLY = "nightly-2026-10-03"  # verify.py's pinned Miri toolchain
MIRI_SEEDS = (1, 2, 3)


def cases():
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
    # OWN-03/LIFE-02: restore a final access through a padded record. Only Miri
    # detects it, in schedules that preempt the returner inside the window. Each
    # seed is deterministic for one source, yet an unrelated edit moves its
    # schedule; every MIRI_SEEDS baseline must pass and one seed must kill.
    miri = {"MIRIFLAGS": "-Zmiri-strict-provenance"}
    full = ["--features", "local-branches-spike,bytes-interop", "--lib"]
    cases.extend([
        ("mosaic-final-refund-padded-ledger", "src/mosaic/protocol.rs",
         [("admitted: &'a AtomicUsize,", "domain: &'a DomainLedger,"),
          ("admitted: &self.domain.admitted,", "domain: self.domain,"),
          ("refund_admitted(self.admitted, self.charge);", "self.domain.refund(self.charge);")], None,
         [*full, "mosaic::raw::tests::si11_final_refund_races_teardown_across_many_schedules"], miri),
        ("berry-final-release-padded-ledger", "src/berry/protocol.rs",
         [("pub(super) struct DomainLedger {", "#[repr(align(64))]\npub(super) struct DomainLedger {"),
          ("    admitted: &'a AtomicUsize,\n    allocations: &'a AtomicUsize,\n", "    domain: &'a DomainLedger,\n"),
          ("            admitted: &self.domain.admitted,\n            allocations: &self.domain.allocations,\n",
           "            domain: self.domain,\n"),
          ("self.admitted.fetch_sub(self.charge, Ordering::Release);\n        self.allocations.fetch_sub(1, Ordering::Release);",
           "self.domain.refund(self.charge);\n        self.domain.allocations.fetch_sub(1, Ordering::Release);")], None,
         [*full, "berry::raw::tests::life02_final_claim_release_races_teardown_across_many_schedules"], miri),
    ])
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--discard-builds", action="store_true", help="Remove this run\'s build artifacts after preserving logs/source")
    parser.add_argument("--only", help="one exact control for diagnosing an incomplete campaign")
    parser.add_argument("--nightly", default=NIGHTLY, help="toolchain for the Miri controls")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    selected = [c for c in cases() if not args.only or c[0] == args.only]
    if not selected:
        raise RuntimeError("empty mutation selection")
    results = []
    for name, file, old, new, test, overrides in selected:
        copy = args.output / name / "source"
        shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns("target", ".git", "__pycache__", "artifacts", "mutants.out", "mutants.out.old"))
        env = dict(os.environ, CARGO_TARGET_DIR=str(create_case_target(args.output, name)), RUSTFLAGS="", CARGO_ENCODED_RUSTFLAGS="") | overrides
        tool = ["+" + args.nightly, "miri"] if "MIRIFLAGS" in overrides else []
        command = ["cargo", *tool, "test", "--locked", *test, "--", "--exact"]
        edits = old if isinstance(old, list) else [(old, new)]
        seeds = [{"MIRIFLAGS": f"{overrides['MIRIFLAGS']} -Zmiri-seed={s}"} for s in MIRI_SEEDS] if tool else None
        result = run_case(name,copy,file,edits,command,test[-1],env,seeds)
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
