#!/usr/bin/env python3
"""AP-12 interleaved ingress experiments on prebuilt, source-qualified binaries.

No compilation/profiling during timings. The optional old server receives only
its supported arguments; the current load generator validates all deliveries.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

from performance_support import require

spec = importlib.util.spec_from_file_location("review", Path(__file__).with_name("performance-review.py"))
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


def arena_identity(plan):
    """Compare physical reservation and admission without external queue bytes."""
    return dict(classes=plan["class_capacities"], slots=plan["class_slots"],
                roots=[{k: root[k] for k in ("reservation_bytes", "configured_backing_bytes",
                                             "metadata_bytes", "domain_limit_each", "domains")}
                       for root in plan["roots"]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--build-record", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--baseline-build-record", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--seconds", type=float, default=15)
    parser.add_argument("--suite", choices=("shortlist", "final"), default="shortlist")
    parser.add_argument("--server-cpus", type=review.berry.cpus, default=[0, 2])
    parser.add_argument("--reader-cpus", type=review.berry.cpus, default=[4, 6])
    parser.add_argument("--generator-cpus", type=review.berry.cpus, default=[8])
    args = parser.parse_args()
    require(args.repeats > 0 and 0 < args.seconds <= 3600, "duration", "positive bounded duration/repeats required")
    modes = {"baseline": None, "initialized": "initialized", "direct": "read-buf", "staged": "staged-copy"}
    runners = {}
    for name in modes:
        runners[name] = review.berry.Runner(SimpleNamespace(
            target=args.target, build_record=args.build_record, output=args.output/name,
            server_binary=args.baseline if name == "baseline" else None,
            server_build_record=args.baseline_build_record if name == "baseline" else args.build_record,
            server_cpus=args.server_cpus, reader_cpus=args.reader_cpus,
            generator_cpus=args.generator_cpus, micro_cpus=args.server_cpus, server_prefix=""))
    profiles = [("ordinary", review.tcp_profile("mosaic", 8)),
                ("adaptive", review.fixed_arena_profile(local=True, queue_backend="local", freshness="adaptive"))]
    jobs = []
    for name, profile in profiles:
        patterns = ["healthy"] if args.suite == "shortlist" or name == "ordinary" else ["healthy", "pause", "constant", "ramp", "stop-resume"]
        for rate in [1000, 20000]:
            for pattern in patterns:
                options = dict(profile, seconds=args.seconds, rate=rate)
                if pattern != "healthy":
                    options.update(slow="yes", **{"slow-profile": pattern, "send-buffer-bytes": 4096})
                jobs.append((f"{name}-{pattern}-r{rate}", options))
    if args.suite == "final":
        for keyed in [False, True]:
            profile = review.fixed_arena_profile(local=True, queue_backend="local", freshness="adaptive")
            if keyed:
                profile["frame-dependencies"] = "keyframe"
            jobs.append((f"mixed-key{int(keyed)}", dict(profile, seconds=args.seconds, rate=200,
                distribution="mixed-frames", **{"domain-limit-bytes": 136314880, "keyframe-burst": 4})))
    # The same typed plan computes the ceiling and verifies unchanged arena bytes.
    probe = args.target/"release/examples/spike_probe"
    def plan(binary, options):
        output = subprocess.check_output([str(binary), "--case=tcp", "--role=plan",
                    *[f"--{k}={v}" for k, v in options.items()]], text=True)
        return json.loads(output)["result"]["pipeline_plan"]
    limits = {}
    for key, options in jobs:
        original = arena_identity(plan(args.baseline, options))
        current = plan(probe, options)
        limits[key] = current["external_storage_model_bytes"]*105//100
        require(arena_identity(current) == original, key, "baseline arena/admission mismatch")
        for mode in modes.values():
            if mode is not None:
                selected = plan(probe, dict(options, ingress=mode,
                                **{"external-memory-limit-bytes": limits[key]}))
                require(arena_identity(selected) == original, key, "candidate arena/admission mismatch")
    for trial in range(args.repeats):
        order = list(modes)
        order = order[trial % len(order):] + order[:trial % len(order)]
        for key, options in (jobs if trial % 2 == 0 else list(reversed(jobs))):
            for name in order:
                selected = dict(options)
                if modes[name]:
                    selected.update(ingress=modes[name], **{"external-memory-limit-bytes": limits[key]})
                runners[name].tcp(key, selected, trial)


if __name__ == "__main__":
    main()
