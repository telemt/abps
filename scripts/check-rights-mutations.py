#!/usr/bin/env python3
"""Directed conservation defects in disposable sources, never the workspace.

Only a compiled, named failing regression kills a mutant. Survivors, compiler
failures and timeouts retain separate outcomes and require review.

On Windows keep the absolute --output path within 155 characters: each control
builds in <output>/target/<hash>, and MSVC link.exe cannot create a build-script
executable beyond MAX_PATH.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
from mutation_support import classify, create_case_target, run_case

ROOT = Path(__file__).resolve().parents[1]
FULL = "local-branches-spike,bytes-interop"


def cases():
    result = []
    # Each published return refunds through the borrowed admitted word (OWN-03).
    final_refund = {"berry": "self.admitted.fetch_sub(self.charge, Ordering::Release);",
                    "mosaic": "refund_admitted(self.admitted, self.charge);"}
    for backend in ["berry", "mosaic"]:
        protocol = f"src/{backend}/protocol.rs"
        raw = f"src/{backend}/raw.rs"
        regression = backend+"::raw::tests::conservation::"
        native = ["--features", FULL, "--lib"]
        loom = ["--features", "local-branches-spike,loom-model", "--lib"]
        result.extend([
            (backend+"-lost-publication", protocol, "self.0.fetch_or(bit, Ordering::Release);", "self.0.fetch_or(0, Ordering::Release);", native,
             regression+"abps_failure_idempotence_and_exact_budget_boundary"),
            (backend+"-relaxed-publication", protocol, "self.0.fetch_or(bit, Ordering::Release);", "self.0.fetch_or(bit, Ordering::Relaxed);", loom,
             backend+"::protocol::tests::final_reader_happens_before_overwrite_without_join_synchronization"),
            (backend+"-lost-refund", protocol, final_refund[backend], "let _ = self.charge;", native,
             regression+"abps_failure_idempotence_and_exact_budget_boundary"),
            (backend+"-lost-reference-release", protocol, "self.0.fetch_sub(1, Ordering::Release) != 1", "self.0.load(Ordering::Relaxed) != 1", loom,
             backend+"::protocol::tests::si10_batch_rollback_races_with_release_without_returning_original"),
            (backend+"-lost-batch-reservation", protocol, "self.references.0.fetch_sub(self.pending, Ordering::Release)", "self.references.0.load(Ordering::Relaxed)", loom,
             backend+"::protocol::tests::si10_batch_rollback_races_with_release_without_returning_original"),
            (backend+"-relaxed-publication-acquire-load", protocol, "self.0.fetch_or(bit, Ordering::Release);", "self.0.fetch_or(bit, Ordering::Relaxed);", loom,
             backend+"::protocol::tests::final_reader_happens_before_overwrite_without_join_synchronization"),
            (backend+"-repeated-generation", raw, "record.state.get().generation + 1", "record.state.get().generation", native,
             regression+"abps_publish_refund_budget_and_single_record_reincarnation"),
            (backend+"-stale-initialization", raw, "initialized: 0,", "initialized: 1,", native,
             regression+"abps_all_class_faults_preserve_neighbors_and_rights"),
        ])
        generation = backend+"::protocol::tests::generations::batch_release_chain_reinitializes_one_slot_without_join"
        result.extend([
            (backend+"-generation-begin-missing", protocol, "self.0.store(1, Ordering::Relaxed);", "// deliberately omit next-reference initialization", loom, generation),
            (backend+"-batch-handoff-unwind", raw, "self.references.materialize();", "self.references.materialize();\n        panic!(\"injected unwind inside SI-10 transfer\");", native,
             regression+"abps_batch_boundaries_conserve_handles_pending_and_source"),
            (backend+"-generation-relaxed-publication", protocol, "self.0.fetch_or(bit, Ordering::Release);", "self.0.fetch_or(bit, Ordering::Relaxed);", loom, generation),
            (backend+"-generation-relaxed-publication-acquire-load", protocol, "self.0.fetch_or(bit, Ordering::Release);", "self.0.fetch_or(bit, Ordering::Relaxed);", loom, generation),
            (backend+"-generation-missing-final-acquire", protocol, "fence(Ordering::Acquire);", "// deliberately omit final acquire", loom, generation),
            (backend+"-generation-relaxed-final-load-acquire-load", protocol, "let _ = self.0.load(Ordering::Acquire);", "let _ = self.0.load(Ordering::Relaxed);", loom, generation),
        ])
    result.extend([
        ("shared-soft-total-bypass", "examples/spike_support/network/memory/shared.rs",
         ".is_none_or(|v| v > p.total)", ".is_none_or(|_| false)",
         ["--features", FULL, "--example", "spike_probe"],
         "spike_support::network::memory::shared::tests::ap14_global_cap_rejects_when_each_hard_limit_still_has_space"),
        ("shared-cache-undercharge", "examples/spike_support/network/memory/shared.rs",
         "self.streams[stream] = s;", "self.streams[stream] = s - charge;",
         ["--features", FULL, "--example", "spike_probe"],
         "spike_support::network::memory::shared::tests::ap14_independent_rights_model_bounds_cached_admission"),
        ("mosaic-root-skip-last-domain", "src/mosaic/protocol.rs", "ledgers.all(DomainLedger::drained)", "ledgers.take(2).all(DomainLedger::drained)",
         ["--features","local-branches-spike,loom-model","--lib"], "mosaic::protocol::tests::si11_root_drain_covers_last_domain_and_all_refunds_without_join"),
        ("mosaic-lost-free-node-count", "src/mosaic/raw.rs", "root.free_nodes.set(root.free_nodes.get() - 1);", "root.free_nodes.set(root.free_nodes.get());",
         ["--features",FULL,"--lib"], "mosaic::raw::tests::partitions::histories::grant_histories_cover_word_edges_and_record_reuse"),
        ("mosaic-lost-free-row", "src/mosaic/raw.rs", "root.free_rows.set(free + 1);", "root.free_rows.set(free);",
         ["--features",FULL,"--lib"], "mosaic::raw::tests::partitions::histories::grant_histories_cover_word_edges_and_record_reuse"),
        ("mosaic-premature-grant-recycle", "src/mosaic/raw.rs",
         "if record.ledger.drained() {\n            self.recycle_domain(index);\n        } else {\n            self.enqueue_domain(index);",
         "if true {\n            self.recycle_domain(index);\n        } else {\n            self.enqueue_domain(index);",
         ["--features",FULL,"--lib"], "mosaic::raw::tests::partitions::histories::grant_histories_cover_word_edges_and_record_reuse"),
        ("branch-lost-final-family-right", "src/mosaic/protocol/branches.rs", "old == 1", "false",
         ["--features","local-branches-spike,loom-model","--lib"], "mosaic::protocol::tests::local_branches::br03_local_readers_happen_before_slot_and_sidecar_reuse_without_join"),
    ])
    last_claim = "berry::protocol::tests::life02_last_claim_observation_acquires_every_prior_return_before_destroy"
    claim_load = "        self.0.load(Ordering::Acquire)\n    }\n    /// LIFE-02/AP-03: called only by the closed sole owner."
    result.extend([
        ("berry-relaxed-root-claim-release", "src/berry/protocol.rs", "self.0.fetch_sub(1, Ordering::Release);", "self.0.fetch_sub(1, Ordering::Relaxed);",
         ["--features","local-branches-spike,loom-model","--lib"], last_claim),
        ("berry-relaxed-root-claim-load", "src/berry/protocol.rs", claim_load, claim_load.replace("Acquire", "Relaxed"),
         ["--features","local-branches-spike,loom-model","--lib"], last_claim),
        ("reference-open-claims-before-record", "src/reference/mod.rs",
         "        if state.free_domains.is_empty() {\n            return Err(Error::DomainCapacityExhausted);\n        }\n        self.root.check_handles(2)?;",
         "        self.root.check_handles(2)?;\n        if state.free_domains.is_empty() {\n            return Err(Error::DomainCapacityExhausted);\n        }",
         ["--features",FULL,"--test","model"], "si09_open_error_order_matches_reference_in_issuer_backends"),
        ("berry-drain-without-allocation-gate", "src/berry/protocol.rs", "self.allocations.load(Ordering::Acquire) == 0 && self.admitted.load(Ordering::Acquire) == 0", "self.admitted.load(Ordering::Acquire) == 0",
         ["--features","local-branches-spike,loom-model","--lib"], "berry::protocol::tests::closed_two_allocation_drain_gates_record_reuse_before_join"),
        ("mosaic-redundant-search-pass", "src/mosaic/protocol.rs", "for confirm in [false, true] {", "for confirm in [false, false, true] {", ["--features",FULL,"--lib"],
         "mosaic::raw::tests::abps_search_success_and_masked_failure_have_bounded_work"),
        ("shrinker-substituted-defect", "tests/support/rights_trace.rs", ".is_some_and(|failure| failure.key == *key)", ".is_some_and(|_| true)", ["--features",FULL,"--test","adversarial"],
         "abps_shrinking_preserves_defect_identity_not_panic_text"),
        ("berry-lost-root-claim", "src/berry/protocol.rs", "self.root.release();", "let _ = self.root;", ["--features",FULL,"--lib"],
         "berry::raw::tests::conservation::abps_all_class_faults_preserve_neighbors_and_rights"),
        ("berry-duplicate-grant", "src/berry/protocol.rs", "unassigned.set(free);", "unassigned.set(free | selected);", ["--features",FULL,"--lib"],
         "berry::raw::tests::conservation::abps_grant_assignment_detector_has_no_destructor_cleanup"),
        ("mosaic-duplicate-grant", "src/mosaic/raw.rs", "self.unassigned(word).set(free & !mask);", "self.unassigned(word).set(free);", ["--features",FULL,"--lib"],
         "mosaic::raw::tests::partitions::abps_fragmented_multiclass_partition_transaction_and_negative_controls"),
        ("adapter-lost-queue-credit", "examples/spike_support/network/worker_fanout.rs", "drop(credit); // AP-07:", "credit.forget(); // AP-07:", ["--features",FULL,"--example","spike_probe"],
         "spike_support::network::worker_fanout::tests::abps_queue_credit_conservation_at_every_cancellation_stage"),
    ])
    return result



def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--discard-builds",action="store_true")
    p.add_argument("--only",help="one exact control for diagnosing an incomplete campaign")
    args=p.parse_args();args.output.mkdir(parents=True,exist_ok=False)
    results=[]
    selected=[c for c in cases() if not args.only or c[0]==args.only]
    if not selected: raise RuntimeError("empty mutation selection")
    for name,file,old,new,flags,selector in selected:
        directory=args.output/name;source=directory/"source";source.mkdir(parents=True)
        for child in ["src","tests","examples","benches","verification","build.rs","Cargo.toml","Cargo.lock"]:
            if (ROOT/child).is_dir(): shutil.copytree(ROOT/child,source/child,ignore=shutil.ignore_patterns("artifacts","target","__pycache__"))
            else: shutil.copy2(ROOT/child,source/child)
        target=create_case_target(args.output,name)
        env=dict(os.environ,CARGO_TARGET_DIR=str(target.resolve()),RUSTFLAGS="",CARGO_ENCODED_RUSTFLAGS="")
        load_variant=name.endswith("-acquire-load")
        if load_variant: env.update(RUSTFLAGS="--cfg abps_tsan",CARGO_ENCODED_RUSTFLAGS="--cfg\x1fabps_tsan")
        command=["cargo","test","--locked",*flags,selector,"--","--exact"]
        result=run_case(name,source,file,[(old,new)],command,selector,env)
        status=result["status"]
        results.append(result)
        (args.output/"results.json").write_text(json.dumps(results,indent=2)+"\n")
        print(name,status,flush=True)
        if args.discard_builds: shutil.rmtree(target,ignore_errors=True)
    if any(r["status"]!="killed" for r in results): raise RuntimeError("conservation mutations need review; inspect all outcomes")


if __name__=="__main__": main()
