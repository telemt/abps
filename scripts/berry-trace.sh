#!/usr/bin/env bash
# Diagnostic runs only. Do not run beside native timing/allocation measurements.
set -euo pipefail
trace_output="${1:?usage: berry-trace.sh OUTPUT_DIRECTORY}"
trace_target="${ABPS_PERF_TARGET:-/tmp/abps-reference-target}"
trace_tools="${ABPS_PERF_TOOLS:-/tmp/abps-perf/tools}"
trace_perf="$trace_tools/usr/bin/perf"
trace_strace="$trace_tools/usr/bin/strace"
trace_binary="$trace_target/release/examples/spike_probe"
export LD_LIBRARY_PATH="$trace_tools/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mkdir -p "$trace_output"
"$trace_perf" version > "$trace_output/tool-versions.txt"
"$trace_strace" -V >> "$trace_output/tool-versions.txt"

for backend in abps berry; do
    roots=separate
    if [[ "$backend" == abps ]]; then roots=shared; fi
    "$trace_perf" record -e cpu-clock:u -F 199 --call-graph dwarf,8192 \
        -o "$trace_output/core-$backend.data" -- taskset -c 0,2,4,6,8,1,3,5 \
        "$trace_binary" --threads=8 --cpus=0,2,4,6,8,1,3,5 --roots="$roots" \
        --backend="$backend" --op=cycle --seconds=4 --warmup=.2 \
        > "$trace_output/core-$backend.json" 2> "$trace_output/core-$backend.record.log"
    "$trace_perf" report -i "$trace_output/core-$backend.data" --stdio --no-children \
        --sort dso,symbol --percent-limit 1 > "$trace_output/core-$backend.report.txt" 2>&1
    "$trace_strace" -f -c -o "$trace_output/syscalls-$backend.txt" \
        taskset -c 0,2,4,6,8,1,3,5 "$trace_binary" --threads=8 \
        --cpus=0,2,4,6,8,1,3,5 --roots="$roots" --backend="$backend" \
        --op=cycle --seconds=2 --warmup=.2 > "$trace_output/syscalls-$backend.json"
done

# Reuse the exact server/loadgen orchestrator and flow checks. perf wraps only
# the server; generator clock/pacing samples cannot enter this profile.
python3 - "$trace_output" "$trace_target" "$trace_perf" <<'PY'
import importlib.util
from pathlib import Path
import shlex
import sys
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("berry_measure", "scripts/berry-performance.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
output, target, perf = map(Path, sys.argv[1:])
options = SimpleNamespace(output=output / "servers", target=target,
    server_cpus=[0, 2, 4, 6], generator_cpus=[9], reader_cpus=[8],
    micro_cpus=[0, 2, 4, 6, 8, 1, 3, 5],
    server_prefix=shlex.join([str(perf), "record", "-e", "cpu-clock:u", "-F", "199",
        "--call-graph", "dwarf,8192", "-o", "{output}.data", "--"]))
runner = module.Runner(options)
for backend in ("abps", "berry"):
    runner.tcp(f"server-{backend}", dict(backend=backend, roots="separate",
        threads=4, streams=4, fanout=20, distribution="packets", rate=1000,
        seconds=10, queue=8, overflow="skip", slow="no", **{"return": "remote"}))
PY
for backend in abps berry; do
    "$trace_perf" report -i "$trace_output/servers/server-$backend-0.data" \
        --stdio --no-children --sort dso,symbol --percent-limit .5 \
        > "$trace_output/server-$backend.report.txt" 2>&1
done
