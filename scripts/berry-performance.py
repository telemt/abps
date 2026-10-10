#!/usr/bin/env python3
"""Sequential Berry experiments, with a separate TCP server and arrival generator.

Build first; this script never compiles during measurements. Output directories
are immutable with respect to source/binary hashes. No performance CI thresholds.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
BACKENDS = [("abps", "shared"), ("abps", "separate"),
            ("berry", "separate"), ("bytes", "separate")]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cpus(value):
    result = list(map(int, value.split(",")))
    if not result or len(set(result)) != len(result):
        raise argparse.ArgumentTypeError("distinct comma-separated CPU numbers required")
    if not set(result) <= os.sched_getaffinity(0):
        raise argparse.ArgumentTypeError("requested CPU outside available affinity")
    return result


def command_output(*command):
    return subprocess.run(command, text=True, capture_output=True, check=False).stdout.strip()


def flow(server, client):
    source = client["source"]
    assert source["planned"] == source["attempted"] + source["missed_arrivals"]
    assert source["attempted"] == source["enqueued"] + source["queue_full"] + source["queue_closed"]
    assert source["enqueued"] == source["sent"] + source["failed_writes"]
    streams = [s for w in server["workers"] for s in w["streams"]]
    received = sum(s["received"] for s in streams)
    discarded = sum(s.get("discarded", s["received"] - s["accepted"]) for s in streams)
    accepted = sum(s["accepted"] for s in streams)
    assert received == accepted + discarded
    enqueued = sum(s["enqueued"] for s in streams)
    assert accepted <= source["sent"]
    cancelled_before_forward = sum(s.get("cancelled_before_forward", 0) for s in streams)
    for s in streams:
        assert s.get("dispatched_recipient_rights", 0) == s.get("locally_enqueued", 0) + s.get("cancelled_before_forward", 0)
        if "contention_by_operation" in s:
            assert s["contended"] == sum(s["contention_by_operation"].values())
    assert received * server["fanout"] == sum(
        s["enqueued"] + s.get("cancelled_before_forward", 0) + s["skipped"] + s["queue_rejected"] +
        s["no_subscriber_deliveries"] + s.get("ingress_rejected_deliveries", 0) for s in streams)
    delivered = sum(c["delivered"] for c in client["consumers"])
    assert delivered == client["total"]["delivered"]
    assert delivered == client["total"]["scheduled_arrival_latency"]["samples"]
    timely = sum(c.get("timely_delivered", c["delivered"]) for c in client["consumers"])
    expired = sum(c.get("expired_delivered", 0) for c in client["consumers"])
    assert timely + expired == delivered
    assert timely == client["total"].get("timely_delivered", delivered)
    assert expired == client["total"].get("expired_delivered", 0)
    written = sum(w["writer_completed"] for w in server["workers"])
    assert delivered <= written <= enqueued
    for worker in server["workers"]:
        if worker["drained"]:
            d = worker["drained"]
            assert d["admitted"] == 0 and d["available"] == d["total_slots"]
    planned_deliveries = source["planned"] * server["fanout"]
    return {"received": received, "discarded": discarded, "accepted": accepted, "enqueued": enqueued, "writer_completed": written,
            "delivered": delivered, "undelivered_enqueued": enqueued - delivered,
            "cancelled_before_forward": cancelled_before_forward,
            "write_api_calls": sum(w["write_api_calls"] for w in server["workers"]),
            "frames_per_write": written / max(1, sum(w["write_api_calls"] for w in server["workers"])),
            "queue_credit_attempts": sum(s.get("queue_credit_attempts", 0) for s in streams),
            "planned_deliveries": planned_deliveries,
            "delivery_fraction": delivered / max(1, planned_deliveries),
            "timely_delivered": timely, "expired_delivered": expired,
            "timely_delivery_fraction": timely / max(1, planned_deliveries),
            "network_bytes_per_s": client["total"]["bytes"] / client["measurement_seconds"],
            "useful_bytes_per_s": client["total"].get("timely_bytes", client["total"]["bytes"]) /
                client["measurement_seconds"]}


class Runner:
    def __init__(self, options):
        if not hasattr(options, "reader_cpus"):
            options.reader_cpus = options.generator_cpus
        if not hasattr(options, "server_binary"):
            options.server_binary = None
        self.options = options
        self.output = options.output
        self.output.mkdir(parents=True, exist_ok=True)
        self.bin = options.target / "release/examples"
        sources = [ROOT / "Cargo.toml", ROOT / "Cargo.lock", *sorted((ROOT / "scripts").glob("*.py")),
                   *sorted((ROOT / "src").rglob("*.rs")),
                   *sorted((ROOT / "examples").rglob("*.rs"))]
        identity = {str(p.relative_to(ROOT)): digest(p) for p in sources}
        identity.update({name: digest(self.bin / name) for name in ("spike_probe", "spike_alloc")})
        if options.server_binary:
            identity["server_binary_override"] = digest(options.server_binary)
        path = self.output / "environment.json"
        if path.exists():
            saved = json.loads(path.read_text())
            if saved["identity"] != identity:
                raise RuntimeError("source/binary changed: use a new output directory")
            for field in ("server_cpus", "generator_cpus", "reader_cpus", "micro_cpus", "server_prefix"):
                if saved[field] != getattr(options, field):
                    raise RuntimeError("affinity/profiling changed: use a new output directory")
        else:
            path.write_text(json.dumps({"identity": identity,
                "utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "rust": command_output("rustc", "-Vv"), "lscpu": command_output("lscpu"),
                "server_cpus": options.server_cpus, "generator_cpus": options.generator_cpus,
                "reader_cpus": options.reader_cpus,
                "server_prefix": options.server_prefix,
                "sample_columns": ["elapsed_s", "rss_kib", "user_ticks", "system_ticks"],
                "clock_ticks_per_s": os.sysconf("SC_CLK_TCK"),
                "micro_cpus": options.micro_cpus, "loadavg": Path("/proc/loadavg").read_text(),
                "build": "prebuilt binaries; compiler flags are unknown unless a build record is supplied",
                "caller_rustflags": os.environ.get("RUSTFLAGS"),
                "build_record": json.loads(options.build_record.read_text()) if getattr(options, "build_record", None) else None,
                "server_binary_override": str(options.server_binary) if options.server_binary else None,
                "note": "affinity does not isolate a shared VM or remove SMT interference",
            }, indent=2) + "\n")
        self.raw = self.output / "results.jsonl"
        self.done = {r["id"]: r for r in map(json.loads, self.raw.read_text().splitlines())} if self.raw.exists() else {}

    def save(self, key, record):
        record.update(id=key, utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
        with self.raw.open("a") as output:
            output.write(json.dumps(record) + "\n")
        self.done[key] = record
        brief = record.get("flow", record.get("result", {}).get("metrics", {}))
        print(f"{key}: " + json.dumps({k: v for k, v in brief.items()
            if k in ("accepted_per_s", "delivery_fraction", "delivered", "errors")}), flush=True)
        return record

    @staticmethod
    def kill(process):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    @staticmethod
    def rss(pid):
        try:
            lines = Path(f"/proc/{pid}/status").read_text().splitlines()
            return next(int(line.split()[1]) for line in lines if line.startswith("VmRSS:"))
        except (OSError, StopIteration):
            return 0

    def process_sample(self, pid, elapsed):
        try:
            fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            user, system = int(fields[11]), int(fields[12])
        except (OSError, IndexError, ValueError):
            user, system = 0, 0
        return [round(elapsed, 3), self.rss(pid), user, system]

    def wait(self, processes, timeout, sample_pids=None):
        start = time.monotonic()
        samples = [[] for _ in processes]
        sample_pids = sample_pids or [p.pid for p in processes]
        try:
            while any(p.poll() is None for p in processes):
                for pid, sample in zip(sample_pids, samples):
                    sample.append(self.process_sample(pid, time.monotonic() - start))
                if time.monotonic() - start > timeout:
                    raise TimeoutError("measurement process exceeded deadline")
                time.sleep(.025)
        finally:
            for process in processes:
                if process.poll() is None:
                    self.kill(process)
                process.wait()
        if any(p.returncode for p in processes):
            for process in processes:
                self.kill(process)
            raise RuntimeError(f"measurement exits {[p.returncode for p in processes]}; see stderr files")
        return samples

    def run(self, name, args, trial=0, alloc=False):
        key = f"{name}-{trial}"
        affinity = self.options.micro_cpus[:int(args.get("threads", 1))]
        opts = dict(args, cpus=",".join(map(str, affinity)))
        if key in self.done:
            if self.done[key]["args"] != {k: str(v) for k, v in opts.items()}:
                raise RuntimeError(f"{key}: configuration changed; use a new output directory")
            return self.done[key]
        command = ["taskset", "-c", opts["cpus"], str(self.bin / ("spike_alloc" if alloc else "spike_probe"))]
        command += [f"--{k}={v}" for k, v in opts.items()]
        out = self.output / f"{key}.json"
        with out.open("w") as stdout, (self.output / f"{key}.stderr").open("w") as stderr:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr, cwd=ROOT, start_new_session=True)
            samples = self.wait([process], float(args.get("seconds", 1)) + float(args.get("warmup", .2)) + 30)[0]
        result = json.loads(out.read_text())
        return self.save(key, {**result, "command": command, "instrumented_alloc": alloc,
            "rss_samples": samples, "rss_peak_kib": max((s[1] for s in samples), default=0)})

    def tcp(self, name, args, trial=0):
        key = f"{name}-{trial}"
        if key in self.done:
            if self.done[key]["args"] != args:
                raise RuntimeError(f"{key}: configuration changed; use a new output directory")
            return self.done[key]
        server_args = dict(args, case="tcp", role="server", cpus=",".join(map(str, self.options.server_cpus)))
        server_cmd = ["taskset", "-c", server_args["cpus"], str(self.options.server_binary or (self.bin / "spike_probe"))]
        server_cmd += [f"--{k}={v}" for k, v in server_args.items()]
        prefix = self.options.server_prefix
        if prefix:
            import shlex
            server_cmd = [part.replace("{output}", str(self.output / key)) for part in shlex.split(prefix)] + server_cmd
        server_log = self.output / f"{key}.server.jsonl"
        client_log = self.output / f"{key}.loadgen.json"
        with (self.output / f"{key}.server.stderr").open("w") as se, \
             (self.output / f"{key}.loadgen.stderr").open("w") as ce, client_log.open("w") as co:
            server = subprocess.Popen(server_cmd, stdout=subprocess.PIPE, stderr=se, text=True,
                                      cwd=ROOT, start_new_session=True)
            client = None
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(server.stdout, selectors.EVENT_READ)
                    if not selector.select(30):
                        raise TimeoutError("server readiness timed out")
                first = server.stdout.readline()
                ready = json.loads(first)
                generator_args = dict(args, case="tcp", role="loadgen", ports=",".join(map(str, ready["ports"])),
                    **{"generator-cpus": ",".join(map(str, self.options.generator_cpus)),
                       "reader-cpus": ",".join(map(str, self.options.reader_cpus))})
                client_cpus = sorted(set(self.options.generator_cpus + self.options.reader_cpus))
                client_cmd = ["taskset", "-c", ",".join(map(str, client_cpus)), str(self.bin / "spike_probe")]
                client_cmd += [f"--{k}={v}" for k, v in generator_args.items()]
                client = subprocess.Popen(client_cmd, stdout=co, stderr=ce, cwd=ROOT, start_new_session=True)
                # A full server report can exceed pipe capacity (many streams).
                # Drain stdout concurrently; do not make server completion depend on its size.
                import threading
                tail = []
                reader = threading.Thread(target=lambda: tail.append(server.stdout.read()))
                reader.start()
                samples = self.wait([server, client], float(args["seconds"]) + 40,
                                    [ready["pid"], client.pid])
                reader.join(timeout=5)
                server_log.write_text(first + "".join(tail))
            finally:
                for process in (client, server):
                    if process and process.poll() is None:
                        self.kill(process)
                    if process:
                        process.wait()
        sr = json.loads(server_log.read_text().splitlines()[-1])["result"]
        cr = json.loads(client_log.read_text())["result"]
        cr["measurement_seconds"] = float(args["seconds"])
        conservation = flow(sr, cr)
        return self.save(key, {"args": args, "commands": [server_cmd, client_cmd],
            "profiled": bool(prefix), "server": sr, "loadgen": cr, "flow": conservation,
            "server_rss_samples": samples[0], "loadgen_rss_samples": samples[1],
            "server_rss_peak_kib": max(s[1] for s in samples[0]),
            "loadgen_rss_peak_kib": max(s[1] for s in samples[1])})

    def core(self):
        base = {"seconds": self.options.seconds, "warmup": .2}
        for workers in (1, 2, 4, 8):
            for backend, roots in BACKENDS:
                for latency in ("no", "yes"):
                    for trial in range(self.options.repeats):
                        self.run(f"cycle-{backend}-{roots}-{workers}t-{latency}", dict(base,
                            backend=backend, roots=roots, threads=workers, size=1500, op="cycle", latency=latency), trial)
        for backend in ("abps", "berry", "bytes"):
            for op, size, fanout in [("fanout", 1500, 1), ("fanout", 1500, 20),
                ("copy_cycle", 65536, 1), ("init_fill", 65536, 1),
                ("copy_cycle", 2097152, 1), ("init_fill", 2097152, 1),
                ("clone", 1500, 1), ("slice", 2097152, 1)]:
                for trial in range(self.options.repeats):
                    self.run(f"{op}-{backend}-{size}-{fanout}", dict(base,
                        backend=backend, op=op, size=size, fanout=fanout, latency="yes"), trial)

    def allocations(self):
        for backend in ("abps", "berry", "bytes"):
            for op in ("acquire_drop", "cycle", "copy_cycle", "freeze", "final_drop",
                       "fill", "init_fill", "clone", "slice", "fanout"):
                self.run(f"alloc-{backend}-{op}", dict(backend=backend, op=op,
                    size=1500, iterations=10000, fanout=20), alloc=True)

    def observe(self):
        for records in (64, 4096, 65536):
            for backend, full in [("abps", "yes"), ("berry", "yes"), ("berry", "no")]:
                for hz in (0, 1, 100):
                    self.run(f"observe-{backend}-{full}-{records}-{hz}", dict(case="observe",
                        backend=backend, full=full, records=records, seconds=self.options.seconds,
                        warmup=.2, **{"observe-hz": hz}))

    def network(self, smoke=False):
        scenarios = [
            ("uniform-local", dict(threads=4, streams=4, fanout=20, load="uniform", **{"return": "local"})),
            ("uniform-remote", dict(threads=4, streams=4, fanout=20, load="uniform", **{"return": "remote"})),
            ("skew-remote", dict(threads=4, streams=4, fanout=20, load="skew", **{"return": "remote"})),
            ("one-reader", dict(threads=1, streams=1, fanout=1, **{"return": "remote"})),
            ("slow-disconnect", dict(threads=4, streams=4, fanout=20, slow="yes", overflow="disconnect")),
            ("slow-skip", dict(threads=4, streams=4, fanout=20, slow="yes", overflow="skip")),
            ("frames-remote", dict(threads=4, streams=4, fanout=1, distribution="frames", rate=200, **{"return": "remote"})),
        ]
        if smoke:
            scenarios = [("smoke-local", dict(threads=1, streams=1, fanout=2)),
                         ("smoke-reserved-fresh", dict(threads=2, streams=4, fanout=2, overflow="skip", **{"capacity-policy":"reserved", "max-frame-age-ms":100,"send-buffer-bytes":65536,"write-batch":8})),
                         ("smoke-remote-skip", dict(threads=2, streams=2, fanout=2, slow="yes", overflow="skip", **{"return": "remote"}))]
        for name, options in scenarios:
            for backend, roots in BACKENDS:
                for trial in range(1 if smoke else self.options.repeats):
                    args = dict(seconds=.3 if smoke else self.options.tcp_seconds,
                        backend=backend, roots=roots, distribution="packets", rate=1000,
                        queue=8, overflow="disconnect", slow="no")
                    args.update(options)
                    self.tcp(f"tcp-{name}-{backend}-{roots}", args, trial)
        if smoke:
            for backend in ("berry", "abps", "bytes"):
                self.tcp(f"tcp-batch-envelope-{backend}", dict(backend=backend, roots="separate",
                    threads=1, streams=1, fanout=2, distribution="packets", rate=1000,
                    seconds=.3, queue=1, overflow="skip", slow="no",
                    **{"capacity-policy":"reserved","capacity-check":"require-envelope",
                       "class-slots":"5,5","fanout-api":"batch","max-frame-age-ms":100}))

    def revisions(self):
        """SI-07..10, fixed-seed interleaving; never a shared-host timing gate."""
        import random
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        needed = 4096  # 64 streams * 20 readers, server and client are separate
        if soft < needed:
            if hard < needed:
                raise RuntimeError("revisions requires RLIMIT_NOFILE >= 4096")
            resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))
        (self.output / "descriptor-limit.json").write_text(json.dumps(
            {"previous_soft": soft, "hard": hard, "measurement_soft": max(soft, needed)}) + "\n")
        jobs = []
        for trial in range(self.options.repeats):
            for classes in (1, 4, 16, 64):
                for policy in ("unreserved", "reserved"):
                    jobs.append((False, f"open-{classes}-{policy}", dict(case="public-api", op="churn",
                        classes=classes, records=128, iterations=20000, **{"capacity-policy":policy}), trial))
            for classes in (1, 64):
                for op, iterations in (("snapshot",20), ("metrics",20000), ("pages",20000)):
                    jobs.append((False, f"diagnostic-{classes}-{op}", dict(case="public-api", op=op,
                        classes=classes, records=65536, iterations=iterations,
                        **{"page-size":32,"capacity-policy":"reserved"}), trial))
            for backend in ("berry", "abps", "bytes"):
                for fanout in (1,20):
                    for op in ("fanout", "fanout_batch"):
                        jobs.append((False, f"{op}-{backend}-{fanout}", dict(backend=backend, op=op,
                            fanout=fanout, threads=1, seconds=self.options.seconds, warmup=.2), trial))
            for backend in ("berry", "bytes"):
                for slots in (64,128):
                    for api in ("scalar", "batch"):
                        jobs.append((True, f"capacity-{backend}-{slots}-{api}", dict(backend=backend,
                            threads=2, streams=64, fanout=20, distribution="packets", rate=20000,
                            seconds=self.options.tcp_seconds, queue=8, overflow="skip", slow="no",
                            **{"return":"remote","write-batch":8,"max-frame-age-ms":100,
                               "capacity-policy":"reserved","class-slots":f"{slots},{slots}",
                               "fanout-api":api}), trial))
        random.Random(424242).shuffle(jobs)
        for tcp, name, args, trial in jobs:
            (self.tcp if tcp else self.run)(name,args,trial)

    def pressure(self):
        # Larger packets fill kernel buffers soon enough to exercise both queue
        # policies; tiny-packet slow readers may only accumulate a kernel backlog.
        for policy in ("disconnect", "skip"):
            for backend, roots in BACKENDS:
                for trial in range(self.options.repeats):
                    self.tcp(f"pressure-{policy}-{backend}-{roots}", dict(
                        seconds=self.options.tcp_seconds, backend=backend, roots=roots,
                        threads=4, streams=4, fanout=20, distribution="frames", rate=80,
                        queue=8, overflow=policy, slow="yes", load="skew", **{"return": "remote"}), trial)

    def architecture(self):
        import random
        cases = []
        base = dict(seconds=self.options.seconds, warmup=.15)
        for backend in ("abps", "berry"):
            for records in (64, 4096, 65536):
                cases.append((False, f"churn-{backend}-{records}", dict(case="churn", backend=backend, records=records, iterations=1000)))
            for working in (1, 8, 64):
                for op in ("fill", "init_fill"):
                    cases.append((False, f"working-{backend}-{working}-{op}", dict(base, case="working-set", backend=backend, op=op, size=2097152, **{"working-set":working})))
            for threads in (1, 4, 8):
                for backing in ("same", "distinct"):
                    cases.append((False, f"views-{backend}-{threads}-{backing}", dict(base, case="concurrent-views", backend=backend, threads=threads, backing=backing)))
        tcp_base = dict(seconds=self.options.tcp_seconds, threads=4, streams=4, fanout=20,
            distribution="packets", overflow="skip", **{"return":"remote"})
        for backend, roots in BACKENDS:
            for rate in (1000, 5000, 20000):
                for queue in (8, 32):
                    cases.append((True, f"ramp-{backend}-{roots}-{rate}-q{queue}", dict(tcp_base, backend=backend, roots=roots, rate=rate, queue=queue)))
        for ingress in ("initialized", "read-buf"):
            for batch in (1, 8):
                for queue in (8, 32):
                    cases.append((True, f"io-{ingress}-b{batch}-q{queue}", dict(tcp_base, backend="berry", roots="separate", rate=20000, queue=queue, ingress=ingress, **{"write-batch":batch})))
        for policy in ("disconnect", "skip"):
            cases.append((True, f"burst-skew-{policy}", dict(tcp_base, backend="berry", roots="separate", rate=100, queue=32, overflow=policy, distribution="frames", load="skew", slow="yes", ingress="read-buf", **{"keyframe-burst":4,"write-batch":8})))
        trials = [(tcp, name, args, trial) for trial in range(self.options.repeats) for tcp, name, args in cases]
        random.Random(424242).shuffle(trials)
        for tcp, name, args, trial in trials:
            (self.tcp if tcp else self.run)(name, args, trial)

    def isolation(self):
        import random
        cases = []
        for threads in (4, 8):
            for stride in (1, 2):
                for offset in range(8):
                    cases.append((False, f"spacing-{threads}-{stride}-{offset}", dict(case="concurrent-views", threads=threads, backing="distinct", seconds=self.options.seconds, warmup=.15, **{"slot-stride":stride,"slot-offset":offset})))
        for busy in (0,63,511,4095):
            cases.append((False, f"pending-{busy}", dict(case="pending-domains", iterations=25, **{"pending-records":busy,"maintenance-batch":32})))
        base = dict(threads=2,streams=4,fanout=20,distribution="frames",rate=500,seconds=self.options.tcp_seconds,overflow="skip",slow="yes", **{"return":"remote","write-batch":8})
        for backend in ("berry","abps","bytes"):
            for policy in ("unreserved","reserved"):
                for queue in (8,32):
                    cases.append((True, f"isolation-{backend}-{policy}-q{queue}",dict(base,backend=backend,queue=queue,**{"capacity-policy":policy})))
            for send in (None,65536,262144):
                args = dict(base,backend=backend,queue=8,**{"capacity-policy":"reserved","max-frame-age-ms":100})
                if send is not None: args["send-buffer-bytes"] = send
                cases.append((True, f"fresh-{backend}-{send}",args))
        trials = [(tcp,name,args,trial) for trial in range(self.options.repeats) for tcp,name,args in cases]
        random.Random(424242).shuffle(trials)
        for tcp,name,args,trial in trials:
            (self.tcp if tcp else self.run)(name,args,trial)


def main():
    allowed = sorted(os.sched_getaffinity(0))
    # Prefer physical cores here on the recorded 10-vCPU host. Override on other hosts.
    order = [c for c in (0, 2, 4, 6, 8, 1, 3, 5, 7, 9) if c in allowed] or allowed
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", type=Path, default=Path("/tmp/abps-reference-target"))
    parser.add_argument("--suite", choices=("smoke", "core", "alloc", "observe", "tcp", "pressure", "architecture", "isolation", "revisions", "all"), default="smoke")
    parser.add_argument("--seconds", type=float, default=2)
    parser.add_argument("--tcp-seconds", type=float, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--micro-cpus", type=cpus, default=order[:8])
    parser.add_argument("--server-cpus", type=cpus, default=order[:max(1, min(4, len(order)-1))])
    parser.add_argument("--generator-cpus", type=cpus, default=order[-1:])
    parser.add_argument("--reader-cpus", type=cpus, default=order[4:5] or order[-1:])
    parser.add_argument("--server-binary", type=Path, help="explicit preserved server for before/after runs; its hash is recorded")
    parser.add_argument("--server-prefix", default="", help="optional perf/strace command; {output} is a per-run path")
    args = parser.parse_args()
    if set(args.server_cpus) & set(args.generator_cpus + args.reader_cpus):
        parser.error("server and client CPU sets must be disjoint")
    if min(args.seconds, args.tcp_seconds, args.repeats) <= 0:
        parser.error("durations/repeats must be positive")
    runner = Runner(args)
    if args.suite in ("revisions", "all"):
        runner.revisions()
    if args.suite in ("isolation", "all"):
        runner.isolation()
    if args.suite in ("architecture", "all"):
        runner.architecture()
    if args.suite in ("core", "all"):
        runner.core()
    if args.suite in ("alloc", "all"):
        runner.allocations()
    if args.suite in ("observe", "all"):
        runner.observe()
    if args.suite in ("tcp", "all", "smoke"):
        runner.network(smoke=args.suite == "smoke")
    if args.suite in ("pressure", "all"):
        runner.pressure()


if __name__ == "__main__":
    main()
