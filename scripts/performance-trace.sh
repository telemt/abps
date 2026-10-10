#!/usr/bin/env bash
# Separate diagnostic runs. Never mix these timings with native measurements.
set -euo pipefail
trace_dir="${1:?usage: performance-trace.sh OUTPUT_DIRECTORY}"
trace_target="${ABPS_PERF_TARGET:-/tmp/abps-reference-target}"
trace_tools="${ABPS_PERF_TOOLS:-/tmp/abps-perf/tools}"
trace_binary="$trace_target/release/examples/perf_probe"
mkdir -p "$trace_dir"
export LD_LIBRARY_PATH="$trace_tools/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
trace_perf="$trace_tools/usr/bin/perf"
trace_strace="$trace_tools/usr/bin/strace"
"$trace_perf" version > "$trace_dir/tool-versions.txt"
"$trace_strace" -V >> "$trace_dir/tool-versions.txt"

record() {
    local name="$1"
    shift
    "$trace_perf" record -e cpu-clock:u -F 199 --call-graph dwarf,8192 \
        -o "$trace_dir/$name.data" -- taskset -c 0,2,4,6,8,1,3,5 \
        "$trace_binary" "$@" > "$trace_dir/$name.json" 2> "$trace_dir/$name.record.log"
    "$trace_perf" report -i "$trace_dir/$name.data" --stdio --no-children \
        --sort dso,symbol --percent-limit 1 > "$trace_dir/$name.report.txt" 2>&1
}
record shared --threads=8 --op=cycle --size=1500 --seconds=4 --warmup=.5
record separate --threads=8 --roots=separate --op=cycle --size=1500 --seconds=4 --warmup=.5
record observer --op=cycle --records=65536 --observe-hz=100 --seconds=4 --warmup=.5
record init-fill --op=init_fill --size=2097152 --seconds=4 --warmup=.5
record tcp --case=tcp --backend=abps --distribution=packets --rate=1000 --seconds=4 --overflow=skip
for threads in 1 8; do
    "$trace_strace" -f -c -o "$trace_dir/syscalls-$threads.txt" \
        taskset -c 0,2,4,6,8,1,3,5 "$trace_binary" --threads="$threads" \
        --op=cycle --size=1500 --seconds=2 --warmup=.5 \
        > "$trace_dir/syscalls-$threads.json"
done
"$trace_strace" -f -c -o "$trace_dir/tcp-syscalls.txt" \
    taskset -c 0,2,4,6 "$trace_binary" --case=tcp --backend=bytes \
    --distribution=packets --rate=1000 --seconds=2 --overflow=skip \
    > "$trace_dir/tcp-syscalls.json"
"$trace_perf" stat -o "$trace_dir/counters.txt" \
    -e cpu-clock,task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,cache-misses,branch-misses \
    -- taskset -c 0,2,4,6,8,1,3,5 "$trace_binary" --threads=8 \
    --op=cycle --size=1500 --seconds=4 --warmup=.5 > "$trace_dir/counters.json"
