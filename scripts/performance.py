#!/usr/bin/env python3
"""Run sequential, resumable reference audits. No timing pass overlaps another."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
CPU_ORDER = [0, 2, 4, 6, 8, 1, 3, 5]


def validate_flow(result):
    """Conservation of workload counters, independent of allocator ledgers."""
    if "pipeline" in result:
        result = result["pipeline"]
    if "source" not in result:
        return
    source = result["source"]
    assert source["planned"] == (
        source["attempted"] + source["missed_arrivals"] + source["not_attempted_after_stop"])
    assert source["sent"] <= source["attempted"]
    assert result["accepted"] <= source["sent"]
    assert result["accepted"] * result["subscribers"] == (
        result["enqueued"] + result["skipped"] + result["queue_rejected"] +
        result["no_subscriber_deliveries"])
    delivered = sum(c["delivered"] for c in result["consumers"])
    assert delivered == result["total"]["delivered"]
    assert result["total"]["scheduled_arrival_latency"]["samples"] == delivered
    assert result["enqueued"] == delivered + result["undelivered_enqueued"]
    assert result["drained"]["admitted"] == 0
    assert result["drained"]["available"] == result["drained"]["total_slots"]


def environment():
    def command(*args):
        return subprocess.run(args, text=True, capture_output=True, check=False).stdout.strip()
    return {
        "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "platform": platform.platform(), "rust": command("rustc", "-Vv"),
        "cargo": command("cargo", "-V"), "lscpu": command("lscpu"),
        "affinity_available": sorted(os.sched_getaffinity(0)),
        "loadavg": Path("/proc/loadavg").read_text().strip(),
        "perf_event_paranoid": Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip(),
        "build": "release, debug=1, strip=none, default target CPU, no LTO override",
        "sources": {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in [ROOT / "Cargo.toml", ROOT / "Cargo.lock", *sorted((ROOT / "src").rglob("*.rs")),
                      *sorted((ROOT / "examples").rglob("*.rs"))]
        },
    }


class Runner:
    def __init__(self, options):
        self.options = options
        self.output = options.output
        self.output.mkdir(parents=True, exist_ok=True)
        self.raw = self.output / "results.jsonl"
        self.done = {}
        if self.raw.exists():
            self.done = {r["id"]: r for r in map(json.loads, self.raw.read_text().splitlines())}
        env = self.output / "environment.json"
        if not env.exists():
            env.write_text(json.dumps(environment(), indent=2) + "\n")

    def run(self, name, args, trial=0, alloc=False):
        key = f"{name}-{trial}"
        if key in self.done:
            return self.done[key]
        cpus = [c for c in CPU_ORDER if c in os.sched_getaffinity(0)]
        threads = int(args.get("threads", 1))
        count = 4 if args.get("case") == "tcp" else threads
        if "observe-hz" in args or args.get("case") == "handoff":
            count = max(count, 2)
        affinity = ",".join(map(str, cpus[:count]))
        executable = self.options.target / "release/examples" / ("perf_alloc" if alloc else "perf_probe")
        cmd = ["taskset", "-c", affinity, str(executable)]
        cmd += [f"--{k}={v}" for k, v in args.items()]
        stdout = self.output / f"{key}.json"
        stderr = self.output / f"{key}.stderr"
        start = time.monotonic()
        rss = []
        with stdout.open("w") as out, stderr.open("w") as err:
            process = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=err)
            deadline = start + float(args.get("seconds", 2)) + float(args.get("warmup", .5)) + 30
            while process.poll() is None:
                try:
                    status = Path(f"/proc/{process.pid}/status").read_text()
                    current = next(int(s.split()[1]) for s in status.splitlines() if s.startswith("VmRSS:"))
                    rss.append([round(time.monotonic() - start, 3), current])
                except (OSError, StopIteration):
                    pass
                if time.monotonic() > deadline:
                    process.kill()
                    process.wait()
                    raise RuntimeError(f"{key}: timeout, see {stderr}")
                time.sleep(.02)
        if process.returncode:
            raise RuntimeError(f"{key}: exit {process.returncode}, see {stderr}: {stderr.read_text()}")
        result = json.loads(stdout.read_text())
        validate_flow(result["result"])
        record = {
            "id": key, "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "command": cmd, "instrumented_alloc": alloc, "wall_s": time.monotonic() - start,
            "rss_peak_kib": max((r[1] for r in rss), default=0),
            "rss_samples": rss, **result,
        }
        with self.raw.open("a") as out:
            out.write(json.dumps(record) + "\n")
        self.done[key] = record
        detail = result["result"]
        rate = detail.get("metrics", {}).get("accepted_per_s", detail.get("accepted_per_planned_s", ""))
        print(f"{key}: rate={rate} stop={detail.get('ingress_stop')} rss={record['rss_peak_kib']} KiB", flush=True)
        return record

    def repeat(self, name, args):
        for trial in range(self.options.repeats):
            self.run(name, args, trial)

    def core(self):
        timing = {"seconds": self.options.seconds, "warmup": .5}
        for backend in ("abps", "bytes"):
            for size in (512, 1500, 65536, 2097152):
                for op in ("acquire_drop", "cycle", "clone", "slice", "fill", "init_fill"):
                    # Native rates; separate clock/histogram pass below.
                    self.repeat(f"{backend}-{op}-{size}", dict(
                        timing, backend=backend, size=size, op=op, latency="no"))
            for op in ("control", "freeze", "final_drop", "clone", "slice", "cycle"):
                self.repeat(f"{backend}-latency-{op}", dict(
                    timing, backend=backend, op=op, size=1500, latency="yes"))
            for size in (65536, 2097152):
                for op in ("fill", "init_fill"):
                    self.repeat(f"{backend}-latency-{op}-{size}", dict(
                        timing, backend=backend, op=op, size=size, latency="yes"))
            for threads in (1, 2, 4, 8):
                for roots in ("shared", "separate"):
                    if threads == 1 and roots == "separate":
                        continue
                    self.repeat(f"{backend}-{threads}t-{roots}", dict(
                        timing, backend=backend, op="cycle", size=1500,
                        threads=threads, roots=roots, latency="yes"))
            for consumers in (1, 4, 20):
                self.repeat(f"{backend}-fanout-{consumers}", dict(
                    timing, backend=backend, op="fanout", size=1500, fanout=consumers, latency="yes"))
            self.repeat(f"{backend}-handoff", dict(timing, backend=backend, case="handoff", size=1500))
            self.run(f"{backend}-retention", {"case": "retention", "backend": backend})
        for error in ("budget", "depleted", "too_large"):
            self.repeat(f"failure-{error}", dict(timing, op=error, size=512, latency="yes"))
        for count in (1, 8, 64):
            for position in ("first", "last"):
                size = 512 if position == "first" else count * 512
                self.repeat(f"classes-{count}-{position}", dict(
                    timing, op="acquire_drop", classes=count, size=size))
        for count in (64, 4096, 65536):
            self.repeat(f"snapshot-{count}", dict(timing, case="snapshot", records=count))
            for hz in (0, 1, 100):
                self.repeat(f"observer-{count}-{hz}", dict(
                    timing, op="cycle", records=count, size=512, latency="yes",
                    **{"observe-hz": hz}))

    def allocation(self):
        for backend in ("abps", "bytes"):
            for op in ("acquire_drop", "cycle", "clone", "slice", "fanout", "init_fill"):
                self.run(f"alloc-{backend}-{op}", dict(
                    backend=backend, op=op, size=1500, iterations=10000), alloc=True)
            self.run(f"alloc-{backend}-tcp", dict(
                case="tcp", backend=backend, seconds=2, rate=500,
                distribution="packets", overflow="skip"), alloc=True)

    def calibrate(self, distribution):
        rates = (100, 250, 500, 1000, 2000, 4000, 8000) if distribution == "packets" else (10, 25, 50, 100, 200, 400, 800)
        stable = []
        evidence = []
        for rate in rates:
            record = self.run(f"calibrate-{distribution}-{rate}", dict(
                case="tcp", backend="bytes", distribution=distribution, rate=rate, seconds=2))
            r = record["result"]
            offered = r["source"]["planned"]
            valid = (
                r["accepted"] >= offered * .98
                and r["total"]["delivered"] >= offered * 20 * .98
                and r["disconnected"] == 0 and r["drain_cancelled_tasks"] == 0
                and r["ingress_stop"] in (None, "measurement_deadline")
            )
            evidence.append({"rate": rate, "passed": valid, "id": record["id"]})
            if valid:
                stable.append(rate)
            elif len(evidence) >= 3:
                break
        # A coarse workload calibration, not an asserted capacity of the host.
        if not stable:
            raise RuntimeError(f"no sustainable {distribution} calibration; inspect load generator")
        base = max(stable)
        (self.output / f"calibration-{distribution}.json").write_text(json.dumps(
            {"base_rate": base, "criteria": "98% planned ingress/deliveries, no disconnect, 2s trials",
             "evidence": evidence}, indent=2) + "\n")
        return base

    def tcp(self):
        for distribution in ("packets", "frames"):
            base = self.calibrate(distribution)
            for percentage in (50, 100, 150):
                for backend in ("abps", "bytes"):
                    self.repeat(f"tcp-{distribution}-{percentage}-{backend}-normal", dict(
                        case="tcp", backend=backend, distribution=distribution,
                        rate=base * percentage // 100, seconds=self.options.tcp_seconds,
                        slow="no", overflow="disconnect"))
            for policy in ("disconnect", "skip"):
                for backend in ("abps", "bytes"):
                    self.repeat(f"tcp-{distribution}-100-{backend}-slow-{policy}", dict(
                        case="tcp", backend=backend, distribution=distribution, rate=base,
                        seconds=self.options.tcp_seconds, slow="yes", overflow=policy))

    def quick(self):
        for backend in ("abps", "bytes"):
            self.run(f"quick-{backend}-core", dict(
                backend=backend, seconds=.1, warmup=.02, threads=2, op="cycle", latency="yes"))
            self.run(f"quick-{backend}-retention", dict(case="retention", backend=backend))
            self.run(f"quick-{backend}-alloc", dict(backend=backend, iterations=100), alloc=True)
            for distribution in ("packets", "frames"):
                self.run(f"quick-{backend}-{distribution}", dict(
                    case="tcp", backend=backend, distribution=distribution, seconds=.25, rate=40,
                    overflow="skip", slow="yes"))
        self.run("quick-snapshot", dict(case="snapshot", records=64, seconds=.1, warmup=.01))
        self.run("quick-handoff", dict(case="handoff", seconds=.1))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=["quick", "core", "alloc", "tcp", "all"], default="quick")
    parser.add_argument("--output", type=Path, default=Path("/tmp/abps-perf") / datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--target", type=Path, default=Path("/tmp/abps-reference-target"))
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--tcp-seconds", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    options = parser.parse_args()
    runner = Runner(options)
    print(f"raw results: {options.output}", flush=True)
    suites = ["core", "alloc", "tcp"] if options.suite == "all" else [options.suite]
    for suite in suites:
        getattr(runner, "allocation" if suite == "alloc" else suite)()


if __name__ == "__main__":
    main()
