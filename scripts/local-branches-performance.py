#!/usr/bin/env python3
"""BR-06 reproducible, sequential probes. Build first; no timing CI thresholds.

Fixed-work saturation probes are distinct from the independently paced TCP tests.
Missing CPUs are recorded as unvalidated, never replaced by oversubscription.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("berry", HERE / "berry-performance.py")
berry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(berry)


from performance_support import physical_cpus, validate_branch_result


def main():
    allowed = sorted(os.sched_getaffinity(0))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", type=Path, default=Path("target"))
    parser.add_argument("--suite", choices=("smoke", "compare", "tcp"), default="smoke")
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--iterations", type=int, default=100000)
    parser.add_argument("--tcp-seconds", type=float, default=3)
    parser.add_argument("--smt", action="store_true", help="separate logical-CPU experiment")
    parser.add_argument("--micro-cpus", type=berry.cpus)
    parser.add_argument("--coordinator-cpu", type=int)
    parser.add_argument("--server-cpus", type=berry.cpus)
    parser.add_argument("--reader-cpus", type=berry.cpus)
    parser.add_argument("--generator-cpus", type=berry.cpus)
    parser.add_argument("--build-record", type=Path, help="actual build command/environment recorded when building")
    parser.add_argument("--min-seconds", type=float, default=0.1)
    parser.add_argument("--sizes", type=str, default="64,1500,65536")
    args = parser.parse_args()
    if args.repeats < 1 or args.iterations < 1 or args.tcp_seconds <= 0:
        parser.error("positive repeats, iterations and duration required")
    args.target = args.target.resolve()
    selected = allowed if args.smt else physical_cpus(allowed)
    args.coordinator_cpu = args.coordinator_cpu if args.coordinator_cpu is not None else selected[-1]
    if args.coordinator_cpu not in allowed:
        parser.error("coordinator CPU outside affinity")
    args.micro_cpus = args.micro_cpus or [c for c in selected if c != args.coordinator_cpu] or selected
    args.server_cpus = args.server_cpus or selected[:min(2, max(0, len(selected)-2))]
    args.reader_cpus = args.reader_cpus or selected[2:-1]
    args.generator_cpus = args.generator_cpus or selected[-1:]
    if args.min_seconds <= 0:
        parser.error("min-seconds must be positive")
    sizes = list(map(int, args.sizes.split(",")))
    if not sizes or any(n < 1 or n > 65536 for n in sizes):
        parser.error("sizes must be 1..65536")
    args.server_prefix = ""
    runner = berry.Runner(args)
    environment_path = args.output / "environment.json"
    environment = json.loads(environment_path.read_text())
    environment["affinity_kind"] = "SMT/logical" if args.smt else "one CPU per physical core"
    environment["coordinator_cpu"] = args.coordinator_cpu
    environment["caller_rustflags"] = os.environ.get("RUSTFLAGS")
    environment_path.write_text(json.dumps(environment, indent=2) + "\n")
    binaries = {name: berry.digest(runner.bin / name) for name in ("branches_probe", "branches_alloc")}
    identity_path = args.output / "branch-binaries.json"
    if identity_path.exists() and json.loads(identity_path.read_text()) != binaries:
        raise RuntimeError("branch binaries changed: use a new output directory")
    identity_path.write_text(json.dumps(binaries, indent=2) + "\n")

    def run(name, binary, options):
        if name in runner.done:
            if runner.done[name]["args"] != options:
                raise RuntimeError("probe options changed: use a new output directory")
            return runner.done[name]["result"]
        command = ["taskset", "-c", str(args.coordinator_cpu), str(runner.bin / binary)]
        command += [f"--{key}={value}" for key, value in options.items()]
        record=dict(args=options, command=command, binary_sha256=binaries[binary],status="incomplete")
        try:
            p = subprocess.run(command, capture_output=True, text=True, timeout=120, cwd=berry.ROOT)
            (args.output / f"{name}.stderr").write_text(p.stderr)
            (args.output / f"{name}.stdout").write_text(p.stdout)
            record["exit_codes"]=[p.returncode]
            p.check_returncode()
            result = json.loads(p.stdout)
            record["result"]=result
            validate_branch_result(result, options.get("mode"))
            record["status"]="passed"
        except Exception as error:
            record.update(status="incomplete" if isinstance(error,subprocess.TimeoutExpired) else "failed",errors=[str(error)])
            if isinstance(error,subprocess.TimeoutExpired):
                for suffix,value in [("stdout",error.stdout),("stderr",error.stderr)]:
                    (args.output / f"{name}.{suffix}").write_bytes(value.encode() if isinstance(value,str) else value or b"")
            raise
        finally:
            runner.save(name,record)
        return result

    run("allocation-control", "branches_alloc", dict(iterations=1000))
    repeats = 1 if args.suite == "smoke" else args.repeats
    calibrated = {}
    if args.suite != "tcp":
        threads = [1, min(2, len(args.micro_cpus))] if args.suite == "smoke" else [1, 2, 4, 8, 16, 32]
        for n in sorted(set(threads)):
            if n > len(args.micro_cpus) or (not args.smt and len(physical_cpus(args.micro_cpus[:n])) < n):
                runner.save(f"threads-{n}-unvalidated", {"status": "unvalidated", "reason": "insufficient distinct CPUs", "threads": n})
                continue
            for trial in range(repeats):
                modes = ["scalar", "batch", "borrowed", "local"]
                if trial % 2:
                    modes.reverse()
                for case in ["churn", "cycle", "prepare", "materialize", "drops", "final-drop"]:
                    if case == "final-drop" and n != 1:
                        continue
                    for size in ([64] if args.suite == "smoke" else sizes):
                        for views in ([1] if case in ("drops", "final-drop", "prepare", "materialize") else [1, 8, 64]):
                            for mode in [*modes, "control"]:
                                options = dict(case=case, mode=mode, threads=n, views=views, size=size,
                                               iterations=1000 if case == "churn" else 10,
                                               cpus=",".join(map(str, args.micro_cpus[:n])))
                                key = f"{case}-{n}-{views}-{size}-{mode}"
                                if args.suite != "smoke":
                                    if key not in calibrated:
                                        initial = dict(options, iterations=args.iterations if case == "churn" else 100)
                                        result = run(key + "-calibration", "branches_probe", initial)
                                        calibrated[key] = min(100_000_000, max(initial["iterations"],
                                            int(initial["iterations"] * args.min_seconds / max(result["wall_seconds"], 1e-9))))
                                    options["iterations"] = calibrated[key]
                                run(f"{key}-{trial}", "branches_probe", options)
                                if trial == 0 and mode != "control":
                                    run(key + "-latency", "branches_probe", dict(options, latency="yes"))
    # Existing runner retains every arrival/loss category and validates flow.
    for trial in range(repeats):
        modes = ["scalar", "batch", "local-branches", "worker-branches"]
        if trial % 2:
            modes.reverse()
        for mode in modes:
            runner.tcp(f"branches-tcp-{mode}", dict(backend="mosaic", threads=1, streams=2,
                fanout=4, queue=8, **{"return":"remote", "write-batch":8,
                "class-slots":"130,130", "domain-limit-bytes":97500,
                "capacity-policy":"reserved", "capacity-check":"require-envelope",
                "fanout-api":mode, "max-frame-age-ms":100}, rate=500,
                seconds=1 if args.suite == "smoke" else args.tcp_seconds, overflow="skip"), trial)


if __name__ == "__main__":
    main()
