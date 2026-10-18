#!/usr/bin/env python3
"""SI-11..15 comparisons. Build all binaries first; never compile while timing.

Shared hosts produce exploratory evidence only. --suite=smoke is functional;
--suite=compare defaults to seven interleaved repeats, without noisy CI gates.
"""
import argparse
import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("berry_performance", HERE / "berry-performance.py")
berry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(berry)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", type=Path, default=Path("target"))
    parser.add_argument("--suite", choices=("smoke", "compare", "alloc", "tcp", "classes", "scaling", "preparation"), default="smoke")
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--seconds", type=float, default=1)
    parser.add_argument("--tcp-seconds", type=float, default=3)
    parser.add_argument("--micro-cpus", type=berry.cpus, default=None)
    parser.add_argument("--server-cpus", type=berry.cpus, default=None)
    parser.add_argument("--generator-cpus", type=berry.cpus, default=None)
    parser.add_argument("--reader-cpus", type=berry.cpus, default=None)
    parser.add_argument("--smt", action="store_true")
    parser.add_argument("--build-record", type=Path)
    args = parser.parse_args()
    order = berry.cpu_selection(args.smt)
    args.micro_cpus = args.micro_cpus or order
    args.server_cpus = args.server_cpus or order[:min(2, max(0, len(order)-2))]
    args.generator_cpus = args.generator_cpus or order[-1:]
    args.reader_cpus = args.reader_cpus or order[2:-1]
    if args.repeats < 1 or args.seconds <= 0 or args.tcp_seconds <= 0:
        parser.error("positive durations/repeats required")
    args.target = args.target.resolve()
    args.server_prefix = ""
    runner = berry.Runner(args)
    if args.suite == "preparation":
        binary = runner.bin / "mosaic_probe"
        for trial in range(args.repeats):
            policies = ["lazy", "zero-payload"] if trial % 2 == 0 else ["zero-payload", "lazy"]
            for policy in policies:
                key = f"preparation-{policy}-{trial}"
                command = ["taskset", "-c", str(args.micro_cpus[0]), str(binary), "--case=preparation",
                           f"--preparation={policy}", "--size=1048576", "--slots=64"]
                if key in runner.done:
                    berry.require(runner.done[key]["command"] == command and runner.done[key]["binary_sha256"] == berry.digest(binary), key, "preparation identity changed; use a new output directory")
                    continue
                record=dict(command=command,binary_sha256=berry.digest(binary),status="incomplete")
                try:
                    with (args.output/f"{key}.stdout").open("w") as out, (args.output/f"{key}.stderr").open("w") as err:
                        result=berry.subprocess.run(command,cwd=berry.ROOT,stdout=out,stderr=err,timeout=60)
                    record["exit_codes"]=[result.returncode]
                    result.check_returncode()
                    record.update(berry.json.loads((args.output/f"{key}.stdout").read_text()))
                    record["status"]="passed"
                except Exception as error:
                    record.update(status="incomplete" if isinstance(error,berry.subprocess.TimeoutExpired) else "failed",errors=[str(error)])
                    raise
                finally:
                    runner.save(key,record)
        for policy in ["lazy", "zero-payload"]:
            runner.run(f"preparation-alloc-{policy}", dict(backend="mosaic", preparation=policy, iterations=1000), alloc=True)
    elif args.suite == "scaling":
        # Distinct layout from the historical 512-drop bursts. Do not extrapolate
        # missing CPUs by oversubscribing or mix per-drop clocks into throughput.
        for workers in [1, 2, 4, 8, 16, 32]:
            if workers > len(set(args.micro_cpus)):
                runner.save(f"scaling-{workers}-unvalidated", {"status":"unvalidated",
                    "reason":"insufficient distinct configured CPUs", "returners":workers})
                continue
            for trial in range(args.repeats):
                backends = ["berry", "mosaic"] if trial % 2 == 0 else ["mosaic", "berry"]
                for topology in ["roots", "domains", "domain"]:
                    for sampled in ["no", "yes"]:
                        for backend in backends:
                            runner.run(f"scaling-{workers}-{topology}-{sampled}-{backend}", dict(case="returns",
                                backend=backend, threads=workers, topology=topology, iterations=300,
                                latency=sampled, **{"return-layout":"scaling"}), trial)
                for backend in backends:
                    runner.run(f"shared-word-{workers}-{backend}", dict(case="returns", backend=backend,
                        threads=workers, topology="domain", iterations=300, **{"return-layout":"shared-word"}), trial)
    elif args.suite == "alloc":
        runner.run("mosaic-allocation-control", dict(backend="mosaic", op="borrowed", iterations=1000), alloc=True)
    elif args.suite == "classes":
        # Equal backing bytes, byte limits, source sequence and overload policy.
        # The extra 128 KiB class trades four 2 MiB slots for 64 medium slots.
        for trial in range(args.repeats):
            profiles = [("original", "65536,2097152", "64,64"),
                        ("medium", "65536,131072,2097152", "64,64,60")]
            if trial % 2:
                profiles.reverse()
            for backend in ["mosaic", "bytes"]:
                for name, capacities, slots in profiles:
                    runner.tcp(f"classes-{name}-{backend}", dict(backend=backend, threads=2, streams=4,
                        fanout=4, queue=8, distribution="mixed-frames", **{"write-batch":8,
                        "class-capacities":capacities, "class-slots":slots,
                        "domain-limit-bytes":8388608, "capacity-policy":"reserved", "fanout-api":"batch",
                        "max-frame-age-ms":100}, rate=200, seconds=args.tcp_seconds, overflow="skip"), trial)
    elif args.suite == "compare":
        for trial in range(args.repeats):
            backends = ["berry", "mosaic"] if trial % 2 == 0 else ["mosaic", "berry"]
            for workers in [1, min(4, len(args.micro_cpus))]:
                for topology in ["roots", "domains", "domain"]:
                    for backend in backends:
                        runner.run(f"returns-{workers}-{topology}-{backend}", dict(case="returns", backend=backend,
                            threads=workers, topology=topology, iterations=300), trial)
            for op in ["cycle", "borrowed", "clone", "slice"]:
                for backend in backends:
                    runner.run(f"{op}-{backend}", dict(backend=backend, op=op, seconds=args.seconds,
                        warmup=.15, latency="yes"), trial)
            for fanout in [0, 1, 2, 4, 8, 20]:
                for backend in backends:
                    runner.run(f"batch-{fanout}-{backend}", dict(backend=backend, op="fanout_batch", fanout=fanout,
                        seconds=args.seconds, warmup=.15, latency="yes"), trial)
            for backend in backends:
                runner.run(f"metadata-pages-{backend}", dict(case="public-api", backend=backend, op="pages",
                    classes=64, records=65536, iterations=4096, **{"capacity-policy":"reserved"}), trial)
    else:
        repeats = 1 if args.suite == "smoke" else args.repeats
        for trial in range(repeats):
            for backend in ["mosaic", "bytes"]:
                runner.tcp(f"mosaic-envelope-{backend}", dict(backend=backend, threads=2, streams=4,
                    fanout=4, queue=8, **{"write-batch":8, "class-slots":"260,260",
                    "domain-limit-bytes":97500, "capacity-policy":"reserved", "capacity-check":"require-envelope",
                    "fanout-api":"batch", "max-frame-age-ms":100}, rate=1000, seconds=args.tcp_seconds,
                    overflow="skip"), trial)
                runner.tcp(f"mixed-frames-{backend}", dict(backend=backend, threads=2, streams=4,
                    fanout=4, queue=8, distribution="mixed-frames", **{"write-batch":8,
                    "class-capacities":"65536,131072,2097152", "class-slots":"64,64,60",
                    "domain-limit-bytes":8388608, "capacity-policy":"reserved", "fanout-api":"batch",
                    "max-frame-age-ms":100}, rate=200, seconds=args.tcp_seconds, overflow="skip"), trial)
            if args.suite == "tcp":
                for backend in ["mosaic", "bytes"]:
                    runner.tcp(f"64-stream-envelope-{backend}", dict(backend=backend, threads=2, streams=64,
                        fanout=20, queue=8, **{"write-batch":8,"class-slots":"20544,20544",
                        "domain-limit-bytes":481500,"capacity-policy":"reserved","capacity-check":"require-envelope",
                        "fanout-api":"batch","max-frame-age-ms":100},rate=2000,seconds=args.tcp_seconds,overflow="skip"),trial)
            runner.tcp("mosaic-read-buf", dict(backend="mosaic", threads=1, streams=1, fanout=4,
                queue=8, ingress="read-buf", **{"class-slots":"65,65","write-batch":8,
                "domain-limit-bytes":97500,"capacity-policy":"reserved","capacity-check":"require-envelope"},
                rate=200,seconds=args.tcp_seconds,overflow="disconnect"), trial)


if __name__ == "__main__":
    main()
