#!/usr/bin/env python3
"""Reproducible ABPS verification. No empty/incomplete run is a passing gate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "verification/matrix.json"
NATIVE = ["", "berry-spike", "bytes-interop", "mosaic-spike", "mosaic-spike,bytes-interop", "local-branches-spike", "local-branches-spike,bytes-interop"]
FULL = NATIVE[-1]
SERIAL = FULL + ",serial-adapter"
NATIVE.extend(["serial-adapter", "serial-adapter,bytes-interop", SERIAL])
SERIAL_ADAPTER = (["--test", "serial_adapter"], "ap15_")
MIRI_GROUPS = [(["--lib"],"berry::raw::"),(["--lib"],"mosaic::raw::"),(["--test","model"],"mem_own_life_same_trace"),(["--test","model"],"reservations::"),(["--test","adversarial"],""),(["--test","local_branches"],"four_ledgers"),(["--test","issuer_snapshots"],""),(["--test","configuration"],"")]
LOCAL_QUEUE = (["--example", "spike_probe"], "ap10_")
ADAPTIVE_QUEUE = (["--example", "spike_probe"], "ap11_")
HOTPATH_ADAPTER = (["--example", "spike_probe"], "ap12_")
MEMORY_ADAPTER = (["--example", "spike_probe"], "ap13_")
SHARED_MEMORY = (["--example", "spike_probe"], "ap14_")
MIRI_GROUPS.extend([LOCAL_QUEUE, ADAPTIVE_QUEUE, HOTPATH_ADAPTER, MEMORY_ADAPTER, SHARED_MEMORY])
ADMISSION_DYNAMICS = (["--example", "spike_probe"], "admission::")
QUEUE_RECEIPTS = (["--example", "spike_probe"], "ap19_")
MIRI_GROUPS.extend([SERIAL_ADAPTER, ADMISSION_DYNAMICS, QUEUE_RECEIPTS])
ADAPTER_CREDITS = (["--example", "spike_probe"], "spike_support::network::worker_fanout::tests::")
SANITIZER_GROUPS = [(["--lib"], "raw::"), (["--test", "model"], ""),
                    (["--test", "adversarial"], ""), (["--test", "local_branches"], ""),
                    (["--test", "issuer_snapshots"], ""), (["--test", "configuration"], ""), (["--test", "issuer_stress"], ""), ADAPTER_CREDITS, LOCAL_QUEUE, ADAPTIVE_QUEUE, HOTPATH_ADAPTER, MEMORY_ADAPTER, SHARED_MEMORY,
                    (["--example","spike_probe"],"abps_scripted_"), ADMISSION_DYNAMICS, QUEUE_RECEIPTS]
NIGHTLY = "nightly-2026-10-03"
REFERENCE_CONFIG_TEST = "verification_config::reference_acquire_mode_matches_campaign"


def require(condition, message):
    if not condition:
        raise VerificationError(str(message))


def feature_set(features):
    graph = tomllib.loads((ROOT / "Cargo.toml").read_text())["features"]
    found = set(filter(None, features.split(","))) | {"default"}
    pending = list(found)
    while pending:
        for child in graph.get(pending.pop(), []):
            if child in graph and child not in found:
                found.add(child); pending.append(child)
    return sorted(found)


def classify(code, output, *, timeout=False, tests=False):
    # A real finding remains a failure even if a later command succeeds or hangs.
    if re.search(r"(?:WARNING|ERROR|SUMMARY|FATAL): (?:Address|Leak|Memory|Thread)Sanitizer:|"
                 r"(?:Address|Memory|Thread)Sanitizer: (?:DEADLYSIGNAL|reported|CHECK failed)|"
                 r"^test result: FAILED\.", output, re.M):
        return "failed"
    if timeout or "Model exceeded maximum number of branches" in output:
        return "incomplete"
    if code and ("error: no such command:" in output or "is not installed for the toolchain" in output):
        return "unsupported"
    if code:
        return "incomplete" if "could not compile" in output or "No space left on device" in output else "failed"
    # TSAN_OPTIONS=exitcode=0 must not turn an actual report into passing evidence.
    if re.search(r"(?:WARNING|SUMMARY|FATAL): ThreadSanitizer:|ThreadSanitizer: (?:reported|CHECK failed)", output):
        return "failed"
    counts = [int(n) for n in re.findall(r"test result: ok\. (\d+) passed;", output)]
    if tests and (not counts or sum(counts) == 0):
        return "incomplete"
    return "passed"


def test_names(output):
    return [line[:-6] for line in output.splitlines() if line.endswith(": test")]


def reference_configuration(output):
    samples = re.findall(r"\bABPS_REFERENCE_ACQUIRE=(\S+) ABPS_TARGET_SANITIZERS=(\S+)", output)
    return samples[0] if len(samples) == 1 else None


def mutation_verified(summary):
    return bool(summary.get("end_time") and summary.get("total_mutants") and
                summary.get("caught") == summary.get("total_mutants") and
                not any(summary.get(k, 0) for k in ["missed", "timeout", "unviable", "success"]))


def review_evidence(data, records, source=None):
    """Never merge binaries, revisions, instrumented variants or legacy records.

    Selectors filter discovery, not unrelated execution output. A check can be
    observed in several tools, but every required variant must pass in ONE tool
    and build configuration, with every selected test actually executed.
    """
    sources = {r.get("identity", {}).get("source_sha256") for r in records if r.get("evidence_schema")==1}
    sources.discard(None)
    if source is None and len(sources)==1: source=next(iter(sources))
    observed = set()
    for check in data["checks"]:
        groups = {}
        for record in records:
            identity = record.get("identity", {})
            if (record.get("status") != "passed" or record.get("evidence_schema") != 1
                or identity.get("target") != check.get("target")
                or identity.get("tool") not in check.get("tools", [])
                or source is None or identity.get("source_sha256") != source
                or not all(identity.get(k) for k in ["source_sha256", "toolchain", "platform", "features", "reference_acquire", "build_configuration"])):
                continue
            # Explicit feature closure: a superset is allowed only when the row
            # permits it. Native compilation must never supply a Loom model.
            expected = set(feature_set(check.get("features", "")))
            actual = set(identity["features"])
            if actual != expected and not (check.get("allow_feature_superset", False) and expected <= actual):
                continue
            if ("loom-model" in actual) != (identity["tool"] == "loom"):
                continue
            selected = set(record.get("selected_tests", []))
            executed = set(record.get("executed_tests", []))
            catalog = set(record.get("target_tests", []))
            matched = {n for n in catalog if check["selector"] in n}
            if not matched or selected != executed or not selected <= catalog:
                continue
            key = tuple(identity[k] for k in ["source_sha256", "tool", "toolchain", "platform", "build_configuration"]) + (tuple(identity["features"]),)
            required, completed = groups.setdefault(key, {}).setdefault(identity["reference_acquire"], (set(),set()))
            required.update(matched)
            completed.update(executed & matched)
        if any(all(mode in modes and modes[mode][0] <= modes[mode][1]
                   for mode in check.get("reference_acquire", ["acquire-load" if key[1]=="tsan" else "fence"]))
               for key,modes in groups.items()):
            observed.add(check["id"])
    return [{"id":case["id"],"observed_checks":sorted(set(case["checks"]) & observed),
             "not_executed_in_this_campaign":sorted(set(case["checks"]) - observed),
             "status":"per-campaign-evidence-not-production-approval"}
            for case in data.get("review_cases", [])]


def source_identity():
    h = hashlib.sha256()
    for base in ["src", "tests", "scripts", "verification", "fuzz/fuzz_targets", "benches", "examples", ".github", ".cargo"]:
        for p in sorted((ROOT / base).rglob("*")):
            if p.is_file() and not any(x in p.parts for x in ["target", "__pycache__", "artifacts"]):
                h.update(str(p.relative_to(ROOT)).encode() + b"\0" + p.read_bytes())
    for name in ["build.rs", "Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml", "fuzz/Cargo.toml", "fuzz/Cargo.lock", "CONTRIBUTING.md", "AGENTS.md", "SAFETY_CASE.md"]:
        p = ROOT / name
        if p.exists():
            h.update(name.encode() + b"\0" + p.read_bytes())
    return h.hexdigest()


def validate_registry(data):
    contract = (ROOT / "CONTRIBUTING.md").read_text()
    require(data["checks"], "empty verification registry")
    seen = set()
    for row in data["checks"]:
        require(row["id"] not in seen, row["id"])
        seen.add(row["id"])
        require(row["selector"] and row["tools"] and row["invariants"], row)
        for invariant in row["invariants"]:
            require(re.search(r"\b" + re.escape(invariant) + r"\b", contract), invariant)
        require("loom-model" not in row["features"] or row["tools"] == ["loom"], row)
    require(len(data["intentional_retention"]) == len(set(data["intentional_retention"])), 'invalid verification registry')
    for row in data["checks"]:
        if row["tools"] == ["loom"]:
            require(row.get("reference_acquire") == ["fence", "acquire-load"], row)
    require(all(name.count("::") >= 2 for name in data["intentional_retention"]), 'invalid verification registry')
    if data.get("schema", 1) >= 2:
        cases = data.get("review_cases", [])
        require(len(cases) == 32 and {c["id"] for c in cases} == {f"ABPS-{n:03d}" for n in range(1,33)}, "incomplete review matrix")
        controls = data.get("negative_controls", [])
        control_ids = {c["id"] for c in controls}
        require(len(control_ids) == len(controls), "duplicate negative control")
        for c in controls:
            require(c["kind"] in ["mutation", "oracle-rejection", "compile-fail"], c)
            require(c["checks"] and set(c["checks"]) <= seen, c)
        for case in cases:
            require(case["checks"] and set(case["checks"]) <= seen, case)
            require(case["precondition"] and case["oracle"] and case["priority"] in ["P0", "P1"], case)
            require(case["negative_controls"] and set(case["negative_controls"]) <= control_ids, case)
            require(set(case["backends"]) == {"berry", "mosaic"}, case)
            require(all(v == "applicable" or v.startswith("not-applicable:") for v in case["backends"].values()), case)
    for row in data.get("allocation_probes", []):
        require((ROOT/"examples"/(row["example"]+".rs")).is_file(), row)
        require(row["arguments"] and row["invariants"], row)
        for invariant in row["invariants"]:
            require(re.search(r"\b"+re.escape(invariant)+r"\b", contract), invariant)
    for row in data.get("empty_targets", []):
        require(row["features"] in NATIVE and row["target"] and row["reason"], row)
    for row in data.get("verification_directions", []):
        require(row["checks"] and set(row["checks"]) <= seen, row)


class VerificationError(RuntimeError):
    pass


def kill_process_tree(process):
    """Forcibly end a start_new_session child and its descendants on timeout.

    POSIX kills the child's process group. Windows ignores start_new_session, so
    taskkill /T follows the unreaped child (its open handle keeps the PID from
    being reused) to every descendant still linked to it. Callers still record
    the run as timed out.
    """
    if os.name != "nt":
        os.killpg(process.pid, signal.SIGKILL)
        return
    taskkill = Path(os.environ.get("SystemRoot", r"C:\Windows"), "System32", "taskkill.exe")
    try:
        subprocess.run([str(taskkill), "/T", "/F", "/PID", str(process.pid)], stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
    except (OSError, subprocess.SubprocessError):
        pass
    process.kill()  # covers a missing or failed taskkill; no-op once the child has ended


class Runner:
    def __init__(self, args):
        self.args = args
        self.out = args.output.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.build = args.build_root.resolve() if args.build_root else self.out / "build"
        self.build.mkdir(parents=True, exist_ok=True)
        (self.build / "tmp").mkdir(exist_ok=True)
        self.data = json.loads(MATRIX.read_text())
        validate_registry(self.data)
        self.env = dict(os.environ, TMPDIR=str(self.build / "tmp"), CARGO_TARGET_DIR=str(self.build / "native"))
        self.records = []
        self.meta = dict(evidence_schema=1, required_steps={}, suite=args.suite, profile=args.profile, source_sha256=source_identity(), commands=self.records, status="not_run", tools=self.data["tools"], critical_checks="not_run", limits=vars(args).copy())
        self.meta["limits"] = {k:str(v) if isinstance(v,Path) else v for k,v in self.meta["limits"].items()}
        import shutil
        self.meta["disk_free_bytes"] = shutil.disk_usage(self.build).free
        self.env.update(ABPS_SOURCE_SHA256=self.meta["source_sha256"], ABPS_TRACE_OUTPUT=str(self.out/"traces"))
        self.save()

    def save(self):
        self.meta["review_evidence"] = review_evidence(self.data, self.records, self.meta["source_sha256"])
        (self.out / "results.json").write_text(json.dumps(self.meta, indent=2) + "\n")

    def complete(self):
        self.meta["final_source_sha256"] = source_identity()
        if self.meta["final_source_sha256"] != self.meta["source_sha256"]:
            self.meta["status"] = "incomplete"
            self.meta["source_drift"] = "source changed during campaign; rerun on one revision"
            self.save()
            raise VerificationError(self.meta["source_drift"])
        statuses = [self.meta["status"]] + [r["status"] for r in self.records]
        steps = self.meta.get("required_steps", {})
        if (not self.records or not steps or any(s != "passed" for s in steps.values())
            or any(s in ["failed", "incomplete", "unsupported"] for s in statuses)):
            self.meta["status"] = next((s for s in ["failed", "incomplete", "unsupported"] if s in statuses), "incomplete")
            self.save()
            raise VerificationError("campaign has missing or unsuccessful required steps")
        self.meta["status"] = "passed"
        self.save()

    def run(self, name, command, *, env=None, tests=False, timeout=None, cwd=ROOT, allow_empty=False):
        log = self.out / f"{len(self.records):03d}-{name}.log"
        print(name, flush=True)
        started = time.monotonic()
        pending = dict(name=name, command=command, cwd=str(cwd), status="not_run", log=log.name)
        self.records.append(pending)
        if self.meta["status"] == "passed":
            self.meta["status"] = "not_run"
        self.save()
        timed_out = False
        effective = self.env | (env or {})
        try:
            with log.open("w") as stream:
                p = subprocess.Popen(command, cwd=cwd, env=effective, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = p.wait(timeout=timeout or self.args.timeout)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    kill_process_tree(p)
                    code = p.wait()
                    timed_out = True
            output = log.read_text(errors="replace")
            status = classify(code, output, timeout=timed_out, tests=tests and not allow_empty)
        except FileNotFoundError as e:
            output = str(e)
            log.write_text(output)
            status, code = "unsupported", None
        record = dict(name=name, command=command, cwd=str(cwd), environment={k:effective[k] for k in effective if k.startswith(("ABPS_","MIRI","LOOM_","RUST","CARGO_TARGET","CARGO_ENCODED_","PROPTEST_","ASAN_","MSAN_","TSAN_","CC_","CXX_","CFLAGS_","CXXFLAGS_"))}, exit_code=code, status=status, seconds=time.monotonic()-started, log=log.name)
        if tests:
            record["passed_executions"] = sum(map(int,re.findall(r"test result: ok\. (\d+) passed;", output)))
            record["executed_tests"] = re.findall(r"^test (.+?) \.\.\. (?:ok|FAILED)$", output, re.M)
        if tests and not record.get("executed_tests") and allow_empty:
            record["applicability"] = "not-applicable: declared empty target in this configuration"
        pending.update(record)
        if status != "passed" and self.meta["status"] != "failed":
            self.meta["status"] = status
        self.save()
        if status != "passed":
            raise VerificationError(f"{name}: {status}; see {log}")
        return output

    def cargo_test(self, flags, *, miri=False, sanitizer=None):
        command = ["cargo"] + (["+"+NIGHTLY] if miri or sanitizer else []) + (["miri", "test"] if miri else ["test"])
        if sanitizer:
            command += ["-Zbuild-std", "--target", "x86_64-unknown-linux-gnu"]
        return command + ["--locked", *flags]

    def execute_tests(self, name, features, target, selector="", *, env=None,
                      miri=False, sanitizer=None, extra=(), harness=(), allow_empty=False):
        """One target, one discovery and execution configuration, exact outcomes."""
        flags = (["--features", features] if features else []) + list(extra)
        base = self.cargo_test([*flags, *target, *([selector] if selector else [])],
                               miri=miri, sanitizer=sanitizer)
        # Discover the entire instrumented target, including tests omitted by this
        # command. A narrow run must never pass a broader registered selector.
        catalog_command = self.cargo_test([*flags,*target],miri=miri,sanitizer=sanitizer)
        listing = self.run("discover-"+name, catalog_command+["--", "--list"], env=env)
        catalog = test_names(listing)
        skips=[harness[i+1] for i,x in enumerate(harness) if x=="--skip"]
        selected=[n for n in catalog if (n==selector if "--exact" in harness else selector in n)
                  and not any(skip in n for skip in skips)]
        require(selected or allow_empty, f"empty selection: {target} {selector}")
        output = self.run(name, base+["--", *harness], env=env, tests=True, allow_empty=allow_empty)
        record = self.records[-1]
        executed = record.get("executed_tests", [])
        # rustdoc adds a result annotation absent from --list.
        canonical = lambda n: re.sub(r"(?: \((?:compile fail|should panic)\)| - (?:compile fail|should panic))$", "", n)
        record["executed_tests"] = [canonical(n) for n in executed]
        record["selected_tests"] = [canonical(n) for n in selected]
        record["target_tests"] = [canonical(n) for n in catalog]
        if (not re.search(r"test result: ok\. \d+ passed;",output)
            or len(set(record["selected_tests"])) != len(record["selected_tests"])
            or len(set(record["executed_tests"])) != len(record["executed_tests"])
            or set(record["executed_tests"]) != set(record["selected_tests"])
            or record["passed_executions"] != len(record["selected_tests"])):
            record["status"] = self.meta["status"] = "incomplete"
            self.save()
            raise VerificationError("discovered and completed tests differ; see " + record["log"])
        effective = self.env | (env or {})
        tool = sanitizer or ("miri" if miri else "loom" if "loom-model" in features else "native")
        mode = effective.get("ABPS_EXPECT_REFERENCE_ACQUIRE", "acquire-load" if sanitizer == "tsan" else "fence")
        toolchain = self.meta.get("toolchains", {}).get("nightly" if miri or sanitizer else "stable")
        require(toolchain, "missing compiler identity before test execution")
        platform = "x86_64-unknown-linux-gnu" if sanitizer else toolchain.split("host: ", 1)[1].splitlines()[0]
        platform = platform if sanitizer else effective.get("CARGO_BUILD_TARGET",platform)
        build = {k:effective.get(k,"") for k in ["RUSTFLAGS","RUSTDOCFLAGS","CARGO_ENCODED_RUSTFLAGS","CARGO_ENCODED_RUSTDOCFLAGS","RUSTC","RUSTC_WRAPPER","RUSTC_WORKSPACE_WRAPPER","CARGO_BUILD_TARGET"]}
        build["profile"] = "release" if "--release" in extra else "test"
        # The two explicitly checked Loom implementations differ by this one cfg.
        if tool=="loom":
            for k in ["RUSTFLAGS","CARGO_ENCODED_RUSTFLAGS"]:
                build[k]=build[k].replace("\x1f"," ").replace("--cfg abps_tsan","").strip()
        record.update(evidence_schema=1, identity=dict(source_sha256=self.meta["source_sha256"],
            target=target, features=feature_set(features), tool=tool, toolchain=toolchain,
            platform=platform, reference_acquire=mode,build_configuration=json.dumps(build,sort_keys=True)))
        self.save()
        return output

    def target_plan(self, features):
        metadata = json.loads(self.run("metadata", ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"]))
        package = next(p for p in metadata["packages"] if Path(p["manifest_path"]).parent == ROOT)
        enabled = set(feature_set(features))
        targets = []
        for target in package["targets"]:
            if not set(target.get("required-features", [])) <= enabled:
                continue
            if "lib" in target["kind"]: targets.append(["--lib"])
            elif "test" in target["kind"]: targets.append(["--test", target["name"]])
            elif "example" in target["kind"]: targets.append(["--example", target["name"]])
        return targets

    def execute(self):
        """Publish required work before execution; individual failures are sticky."""
        tasks = [("rustc", lambda: self.compiler("stable"))]
        if self.args.suite in ["miri", "asan", "tsan", "msan", "fuzz"]:
            tasks.append(("nightly", lambda: self.compiler("nightly")))
        if self.args.suite == "native":
            env = {"ABPS_EXPECT_REFERENCE_ACQUIRE":"fence", "ABPS_EXPECT_SANITIZER":"none"}
            tasks += [("fmt", lambda: self.run("fmt", ["cargo","fmt","--all","--check"])),
                      ("reference-config", lambda: self.reference_config("fence", "none", FULL, env)),
                      ("remote-authority", lambda: self.run("remote-authority",
                          [sys.executable,str(ROOT/"scripts/check_remote_authority.py"),
                           "--output",str(self.out/"remote-authority"),
                           "--build-root",str(self.build/"remote-authority")]))]
            for features in NATIVE:
                flags = ["--features",features] if features else []
                tasks.append(("clippy:"+features, lambda f=flags: self.run("clippy", ["cargo","clippy","--locked","--all-targets",*f,"--","-D","warnings"])))
                for target in self.target_plan(features)+[["--doc"]]:
                    # Only source-declared feature gates may make a target empty.
                    empty = self.empty_target_allowed(features, target)
                    tasks.append(("native:"+features+":"+":".join(target),
                        lambda f=features,t=target,e=empty: self.execute_tests("native", f,t,env=env,allow_empty=e)))
        elif self.args.suite == "registry":
            tasks += [("registry:"+row["id"], lambda r=row: self.registry(r)) for row in self.data["checks"]]
        elif self.args.suite == "properties":
            tasks += [("properties:"+target, lambda t=target: self.properties(t)) for target in ["model","adversarial","local_branches","configuration","grant-histories"]]
        elif self.args.suite == "miri":
            tasks += [("miri:"+str(group), lambda g=group: self.miri(g)) for group in MIRI_GROUPS]
        elif self.args.suite in ["asan","tsan","msan"]:
            tasks.append(("instrumentation", lambda: self.sanitizer_setup(self.args.suite)))
            tasks += [("sanitizer:"+str(group), lambda g=group: self.sanitizer_group(self.args.suite,g)) for group in SANITIZER_GROUPS]
        elif self.args.suite == "fuzz":
            tasks += [("fuzz:"+target, lambda t=target: self.fuzz(t)) for target in ["ownership_trace","mosaic_boundaries","local_branches","adversarial_rights","configuration"]]
        elif self.args.suite == "mutations":
            if self.args.mutation_scope != "components":
                tasks += [("mutations:"+script, lambda s=script: self.mutations(s)) for script in ["check-mosaic-mutations.py","check-branch-mutations.py","check-rights-mutations.py"]]
            if self.args.mutation_scope != "directed":
                tasks.append(("mutations:components", lambda: self.mutations("components")))
        elif self.args.suite == "allocations":
            tasks.append(("allocation-build", lambda: self.run("allocation-build",["cargo","build","--locked","--release","--features",FULL,"--examples"])))
            tasks.append(("serial-allocation-build", lambda: self.run("serial-allocation-build",["cargo","build","--locked","--release","--features",SERIAL,"--example","serial_alloc"])))
            tasks += [("allocation:"+probe["id"],lambda p=probe:self.allocation_probe(p)) for probe in self.data["allocation_probes"]]
        elif self.args.suite == "loom":
            for mode in ["fence", "acquire-load"]:
                tasks.append(("loom:"+mode, lambda m=mode: self.loom(m)))
        else:
            action = (lambda: self.sanitizer(self.args.suite)) if self.args.suite in ["asan","tsan","msan"] else getattr(self,self.args.suite)
            tasks.append((self.args.suite, action))
        self.meta["required_steps"] = {name:"not_run" for name,_ in tasks}
        require(len(self.meta["required_steps"]) == len(tasks), "duplicate required step")
        self.save()
        for name,action in tasks:
            before = len(self.records)
            try:
                action()
                require(len(self.records)>before and all(r["status"]=="passed" for r in self.records[before:]),
                        "required step has no successful command execution: "+name)
            except Exception:
                self.meta["required_steps"][name] = "incomplete"
                self.save()
                raise
            self.meta["required_steps"][name] = "passed"
            self.save()
        self.complete()

    def compiler(self, name):
        command = ["rustc"] + (["+"+NIGHTLY] if name=="nightly" else []) + ["-Vv"]
        output = self.run(name,command)
        require("host: " in output and "commit-hash: " in output, "invalid compiler identity")
        self.meta.setdefault("toolchains", {})[name] = output.strip()
        self.save()

    def empty_target_allowed(self, features, target):
        # Declared in the registry, never inferred from an unexpectedly empty list.
        return any(row["target"] == target and row["features"] == features
                   for row in self.data.get("empty_targets", []))

    def list_tests(self, features, target, selector=""):
        flags = ["--features",features] if features else []
        out = self.run("list", self.cargo_test(flags + target + ([selector] if selector else []) + ["--", "--list"]))
        names = test_names(out)
        if not names:
            self.meta["status"] = "incomplete"; self.save()
            raise VerificationError(f"empty selector: {target} {selector}")
        return names

    def registry(self, only_row=None):
        for row in [only_row] if only_row else self.data["checks"]:
            names = self.list_tests(row["features"],row["target"],row["selector"])
            self.records[-1]["registry_id"] = row["id"]
            self.records[-1]["selected_tests"] = names
        self.meta["critical_checks"] = "selectors-resolve; execution recorded by individual suites"
        self.save()

    def reference_config(self, mode, sanitizer, features, env, *, instrumented=False):
        output = self.execute_tests("reference-config-"+mode, features, ["--lib"], REFERENCE_CONFIG_TEST,
                          sanitizer=self.args.suite if instrumented else None, harness=["--exact", "--nocapture"],
                          env=env | {"ABPS_EXPECT_REFERENCE_ACQUIRE":mode,"ABPS_EXPECT_SANITIZER":sanitizer})
        actual = reference_configuration(output)
        if actual != (mode, sanitizer):
            self.records[-1]["status"] = "incomplete"
            self.meta["status"] = "incomplete"; self.save()
            raise VerificationError(f"missing or inconsistent compiled reference configuration: {actual}")
        self.records[-1]["reference_acquire"] = actual[0]
        self.records[-1]["target_sanitizers"] = actual[1]
        self.save()

    def loom(self, only_mode=None):
        profile = "deep" if self.args.profile in ["nightly","weekly"] else "exhaustive" if self.args.profile=="release" else "smoke"
        features = "local-branches-spike,loom-model"
        for mode,flags in [("fence",""),("acquire-load","--cfg abps_tsan")]:
            if only_mode is not None and mode != only_mode: continue
            env = {"ABPS_LOOM_PROFILE":profile,"CARGO_TARGET_DIR":str(self.build/("loom" if mode=="fence" else "loom-acquire-load")),"RUSTFLAGS":flags,"CARGO_ENCODED_RUSTFLAGS":flags.replace(" ","\x1f")}
            self.reference_config(mode,"none",features,env)
            env["ABPS_EXPECT_REFERENCE_ACQUIRE"] = mode
            # Keep bounds output, but let libtest capture it per test. Concurrent
            # eprintln under --nocapture can splice a result line and invalidate
            # exact discovery/execution matching despite completed models.
            self.execute_tests("loom-"+mode,features,["--lib"],"protocol::tests::",env=env,harness=["--show-output"])
            self.records[-1]["reference_acquire"] = mode
            self.save()

    def properties(self, only_target=None):
        cases = 10000 if self.args.profile in ["nightly","weekly","release"] else 128
        cases = getattr(self.args,"cases",None) or cases
        operations = getattr(self.args,"operations",None) or (1000 if cases==10000 else 250)
        env = {"ABPS_PROPTEST_CASES":str(cases),"ABPS_TRACE_OPERATIONS":str(operations),"PROPTEST_RNG_SEED":str(self.args.seed)}
        flags = ["--release"] if cases==10000 else []
        for target in [only_target] if only_target else ["model", "adversarial", "local_branches", "configuration", "grant-histories"]:
            self.execute_tests("properties-"+target,FULL,["--lib"] if target=="grant-histories" else ["--test",target],"grant_histories_match_logical_slot_sets" if target=="grant-histories" else "",extra=flags,env=env)

    def miri(self, only_group=None):
        seeds = list(map(int,self.args.seeds.split(','))) if self.args.seeds else ([1] if self.args.profile=="smoke" else [1,17,42] if self.args.profile=="pr" else sorted(set(range(10))|{17,42}))
        aliases = ["stacked","tree"] if self.args.alias=="both" else [self.args.alias]
        groups = [only_group] if only_group else MIRI_GROUPS
        for target,selector in groups:
            features = SERIAL if (target,selector) == SERIAL_ADAPTER else FULL
            names = self.list_tests(features,target,selector)
            leaks = sorted(set(names)&set(self.data["intentional_retention"]))
            clean = sorted(set(names)-set(leaks))
            for seed in seeds:
                for alias in aliases:
                    flags = f"-Zmiri-strict-provenance -Zmiri-seed={seed}" + (" -Zmiri-tree-borrows" if alias=="tree" else "")
                    env = {"MIRIFLAGS":flags,"CARGO_TARGET_DIR":str(self.build/"miri"),"PROPTEST_RNG_SEED":str(self.args.seed)}
                    if clean:
                        skips = [item for name in leaks for item in ["--skip",name]]
                        self.execute_tests(f"miri-{alias}-{seed}",features,target,selector,miri=True,env=env,harness=skips)
                    for name in leaks:
                        self.execute_tests(f"miri-intentional-retention-{alias}-{seed}",features,target,name,miri=True,env=env|{"MIRIFLAGS":flags+" -Zmiri-ignore-leaks"},harness=["--exact"])

    def sanitizer_environment(self,kind):
        flag = {"asan":"address","tsan":"thread","msan":"memory"}[kind]
        env = {"CARGO_TARGET_DIR":str(self.build/kind),"RUSTFLAGS":f"-Zsanitizer={flag} -Cdebuginfo=1","RUSTDOCFLAGS":f"-Zsanitizer={flag}","ASAN_OPTIONS":"detect_leaks=1","ABPS_STRESS_ROUNDS":"1000"}
        if kind=="msan":
            env["RUSTFLAGS"] += " -Zsanitizer-memory-track-origins"
            # cc-rs target overrides also instrument native dependency helpers;
            # never change host build-script compilation or use leak suppressions.
            for key,value in [("CC","clang"),("CXX","clang++"),("CFLAGS","-fsanitize=memory -fsanitize-memory-track-origins=2"),("CXXFLAGS","-fsanitize=memory -fsanitize-memory-track-origins=2")]:
                env[key+"_x86_64_unknown_linux_gnu"] = value
        # An inherited encoded flag string must not silently disable instrumentation.
        env["CARGO_ENCODED_RUSTFLAGS"] = env["RUSTFLAGS"].replace(" ","\x1f")
        env["CARGO_ENCODED_RUSTDOCFLAGS"] = env["RUSTDOCFLAGS"].replace(" ","\x1f")
        return env

    def sanitizer_setup(self,kind):
        env=self.sanitizer_environment(kind)
        if kind=="msan": self.run("msan-clang",["clang","--version"])
        mode="acquire-load" if kind=="tsan" else "fence"
        flag={"asan":"address","tsan":"thread","msan":"memory"}[kind]
        self.reference_config(mode,flag,FULL,env,instrumented=True)
        self.meta["reference_acquire"]=mode
        self.meta["instrumentation_limit"]="TSan checks the Acquire-load variant; Loom checks both" if kind=="tsan" else "MSan needs instrumented dependencies/std" if kind=="msan" else "ASan does not infer slot boundaries"
        self.save()

    def sanitizer_group(self,kind,group):
        target,selector=group
        names=self.list_tests(FULL,target,selector)
        leaks=sorted(set(names)&set(self.data["intentional_retention"]))
        skips=[x for name in leaks for x in ["--skip",name]]
        self.execute_tests(kind,FULL,target,selector,sanitizer=kind,env=self.sanitizer_environment(kind),harness=skips)

    def tool(self,name):
        expected=self.data["tools"][name]
        out=self.run(name+"-version",["cargo",name,"--version"])
        if not re.search(r"(?<![0-9.])"+re.escape(expected)+r"(?![0-9.])", out):
            self.meta["status"]="unsupported";self.save()
            raise VerificationError(f"expected {name} {expected}, got {out.strip()}")

    def coverage(self):
        self.tool("llvm-cov")
        env={"CARGO_TARGET_DIR":str(self.build/"coverage")}
        self.run("coverage-clean",["cargo","llvm-cov","clean","--workspace"],env=env)
        for features in NATIVE:
            flags=["--features",features] if features else []
            self.run("coverage-tests",["cargo","llvm-cov","--locked","--no-report","--lib","--tests",*flags],env=env,tests=True)
        for fmt in ["json","lcov","html"]:
            self.run("coverage-"+fmt,["cargo","llvm-cov","report","--"+fmt,"--output-dir" if fmt=="html" else "--output-path",str(self.out/("coverage" if fmt=="html" else "coverage."+fmt))],env=env)
        self.run("coverage-missing",["cargo","llvm-cov","report","--show-missing-lines"],env=env)

    def fuzz(self, only_target=None):
        import shutil
        self.tool("fuzz")
        lock = ROOT / "fuzz/Cargo.lock"
        locked = lock.read_bytes()
        self.run("fuzz-lockfile", ["cargo", "+"+NIGHTLY, "metadata", "--locked", "--manifest-path", "fuzz/Cargo.toml", "--format-version", "1"])
        seconds=self.args.seconds or (20 if self.args.profile=="smoke" else 3600)
        for name,corpus in [("ownership_trace","ownership"),("mosaic_boundaries","ownership"),("local_branches","branches"),("adversarial_rights","rights"),("configuration","configuration")]:
            if only_target is not None and name != only_target: continue
            directory=self.out/("corpus-"+name)
            shutil.copytree(ROOT/"verification/corpus"/corpus,directory)
            artifacts=self.out/("crashes-"+name);artifacts.mkdir()
            output = self.run("fuzz-"+name,["cargo","+"+NIGHTLY,"fuzz","run",name,str(directory),"--","-max_total_time="+str(seconds),"-max_len=5123","-rss_limit_mb=2048","-timeout=30","-seed="+str(self.args.seed),"-artifact_prefix="+str(artifacts)+"/"],env={"CARGO_TARGET_DIR":str(self.build/"fuzz")},timeout=seconds+600)
            # cargo-fuzz has no --locked flag; check both sides of the build.
            if lock.read_bytes() != locked or not re.search(r"Done [1-9][0-9]* runs in", output):
                self.meta["status"] = "incomplete"; self.save()
                raise VerificationError("fuzz lockfile changed or no completed fuzz executions")

    def mutations(self, only_script=None):
        import shutil
        self.tool("mutants")
        scratch = self.build/("mutations-"+hashlib.sha256((str(self.out)+str(only_script)).encode()).hexdigest()[:12])
        scratch.mkdir()
        if only_script != "components" and self.args.mutation_scope != "components":
            for script in ["check-mosaic-mutations.py","check-branch-mutations.py","check-rights-mutations.py"]:
                if only_script is None or only_script == script:
                    self.directed_mutation(script, scratch/script)
        if self.args.mutation_scope == "directed" or only_script not in [None,"components"]: return
        source=scratch/"source"
        shutil.copytree(ROOT,source,ignore=shutil.ignore_patterns("target",".git","__pycache__","artifacts"))
        # Mutated storage can hang unrelated concurrent tests after a unit test
        # has already detected the bug. Use bounded component checks here; the
        # unmutated lifecycle/native campaigns run independently and in full.
        for name,files,selector in [
            ("grants",["src/berry/grants.rs","src/mosaic/grants.rs"],"grants::tests::"),
            ("local-references",["src/mosaic/protocol/branches.rs"],"mosaic::raw::branches::tests::br02_checked_cell_transition_and_limit"),
        ]:
            self.list_tests(FULL,["--lib"],selector)
            output = self.out/("mutants-"+name)
            try:
                self.run("mutants-"+name,["cargo","mutants","--in-place","--dir",str(source),"--features",FULL,*[arg for path in files for arg in ["--file",path]],"--cargo-arg=--locked","--cargo-arg=--lib","--timeout","60","--no-shuffle","--output",str(output),"--",selector],env={"CARGO_TARGET_DIR":str(scratch/"target")},timeout=self.args.timeout)
            finally:
                result = output/"mutants.out/outcomes.json"
                if result.exists():
                    outcomes = json.loads(result.read_text())
                    self.meta.setdefault("mutation_outcomes", {})[name] = {k:v for k,v in outcomes.items() if k != "outcomes"}
                    self.save()
            summary = self.meta.get("mutation_outcomes", {}).get(name, {})
            if not mutation_verified(summary):
                self.meta["status"] = "incomplete"; self.save()
                raise VerificationError("mutation campaign contains unverified outcomes; inspect survivors/build failures/timeouts")

    def directed_mutation(self, script, scratch):
        import shutil
        try:
            self.run(script,[sys.executable,str(ROOT/"scripts"/script),"--output",str(scratch),"--discard-builds"],timeout=3600)
            result = json.loads((scratch/"results.json").read_text())
            require(result and all(r.get("status") == "killed" and r.get("selector") and r.get("baseline_passed") for r in result),
                    "directed campaign lacks named detector evidence")
        finally:
            if scratch.exists():
                shutil.copytree(scratch,self.out/script,ignore=shutil.ignore_patterns("source","target"))

    def stress(self):
        seconds=self.args.seconds or (10 if self.args.profile in ["smoke","pr"] else 3600 if self.args.profile in ["weekly","release"] else 600)
        started=time.monotonic()
        while time.monotonic()-started < seconds:
            self.execute_tests("stress",FULL,["--test","issuer_stress"],extra=["--release"],env={"ABPS_STRESS_ROUNDS":"10000"},harness=["--nocapture"])

    def allocation_probe(self,probe):
        self.run("allocation-"+probe["id"],[str(self.build/"native/release/examples"/probe["example"]),*probe["arguments"]])

    def performance(self):
        self.run("criterion",["cargo","bench","--locked","--features",FULL,"--bench","allocator_contracts","--","--test"] if not self.args.dedicated_host else ["cargo","bench","--locked","--features",FULL,"--bench","allocator_contracts"])
        if self.args.dedicated_host:
            self.run("performance-build",["cargo","build","--locked","--release","--examples","--features",FULL])
            for script,suite in [("berry-performance.py","core"),("berry-performance.py","tcp"),("mosaic-performance.py","compare"),("mosaic-performance.py","tcp"),("local-branches-performance.py","compare")]:
                self.run("performance-"+suite,[sys.executable,str(ROOT/"scripts"/script),"--target",str(self.build/"native"),"--suite",suite,"--repeats","7","--output",str(self.out/(script+"-"+suite))],timeout=self.args.timeout)
                from performance_support import read_records
                qualified=read_records(self.out/(script+"-"+suite)/"results.jsonl")
                require(qualified,"empty performance suite")
        self.meta["performance_evidence"]="primitive measurements only; compare existing offered-load and tail probes on dedicated hardware" if self.args.dedicated_host else "functional benchmark smoke only; no performance gate"
        self.save()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--suite",required=True,choices=["registry","native","loom","properties","miri","asan","tsan","msan","coverage","fuzz","mutations","stress","allocations","performance"])
    p.add_argument("--profile",choices=["smoke","pr","nightly","weekly","release"],default="smoke")
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--build-root",type=Path,help="Cargo build directory (default: OUTPUT/build). On Windows keep its absolute path within 106 characters: MSVC link.exe cannot create a build-script executable beyond MAX_PATH, and directed mutation targets add 69 characters to it.")
    p.add_argument("--timeout",type=int,default=1800)
    p.add_argument("--seed",type=int,default=424242)
    p.add_argument("--seeds")
    p.add_argument("--cases",type=int,help="explicit property history count")
    p.add_argument("--operations",type=int,help="explicit trace bound (1..1024)")
    p.add_argument("--alias",choices=["stacked","tree","both"],default="both")
    p.add_argument("--seconds",type=int)
    p.add_argument("--dedicated-host",action="store_true")
    p.add_argument("--mutation-scope",choices=["all","directed","components"],default="all")
    args=p.parse_args()
    if args.timeout<=0 or (args.seconds is not None and args.seconds<=0): p.error("durations must be positive")
    if args.cases is not None and args.cases<=0: p.error("cases must be positive")
    if args.operations is not None and not 1<=args.operations<=1024: p.error("operations must be 1..1024")
    runner=Runner(args)
    try:
        runner.execute()
    except VerificationError as e:
        if runner.meta["status"] == "not_run": runner.meta["status"]="incomplete"
        runner.meta["verification_error"]=str(e);runner.save()
        print(e,file=sys.stderr);return 1
    except Exception as e:
        if runner.meta["status"] != "failed": runner.meta["status"]="incomplete"
        runner.meta["harness_error"]=repr(e)
        runner.save()
        raise
    return 0

if __name__=="__main__":
    sys.exit(main())
