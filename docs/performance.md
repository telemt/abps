# Reference-model performance audit

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

This measures the safe reference backend, not a proposed production Shared
algorithm. The production contract in [CONTRIBUTING](../CONTRIBUTING.md) is
unchanged. No library algorithm was optimized in this audit.

This is the retained reference audit. The subsequent architecture experiment and
new measurements are in [Berry spike](berry-spike.md). Its TCP
generator runs in another process; the older harness below remains reproducible.

For current validation, provenance, CPU selection and payload checks, use the
[measurement harness guide](performance-harness.md). Historical results below
retain their original workload and evidence.

## Reproduce

Rust 1.96, Linux, Python 3 and taskset are used by the runner. Development
dependencies are locked. Build outside the checkout if disk space is limited:

```sh
CARGO_TARGET_DIR=/tmp/abps-reference-target \
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
  cargo build --release --locked --examples

python3 scripts/performance.py --suite=quick
python3 scripts/performance.py --suite=all \
  --output=/tmp/abps-perf/20261003-reference-audit
```

The full run is deliberately sequential: timing, allocator instrumentation and
profiling must not compete for CPUs. The current default is seven trials, each with
0.5 seconds warm-up and two measured seconds for microbenchmarks; TCP trials last
ten scheduled seconds. Individual suites are `core`, `alloc`, and `tcp`.
The output directory contains environment/source hashes, exact commands,
individual JSON outputs, stderr, sampled RSS, calibration decisions and
`results.jsonl`. Reusing a directory resumes completed trial IDs; use a new
directory after changing the executable, configuration or source.

Run an individual operation without instrumentation:

```sh
taskset -c 0 /tmp/abps-reference-target/release/examples/perf_probe \
  --op=cycle --size=1500 --seconds=2 --warmup=.5

taskset -c 0,2,4,6 /tmp/abps-reference-target/release/examples/perf_probe \
  --case=tcp --backend=abps --distribution=frames --rate=100 \
  --seconds=10 --slow=yes --overflow=disconnect

/tmp/abps-reference-target/release/examples/perf_alloc \
  --backend=abps --op=cycle --size=1500 --iterations=10000
```

`perf_probe` accepts `--key=value` arguments. The runner is the canonical matrix:
512/1500-byte packets, 64-KiB/2-MiB frames; 1/2/4/8 threads; shared versus separate
roots with equal total class capacity; 1/4/20-view fan-out; first/last lookup among
1/8/64 classes; 64/4096/65536 domain and slot records; no observer versus 1/100-Hz
requested snapshots. Separate scenarios cover failure reasons, small/empty
retained views, input/output headroom, and cross-thread final return.

## What is measured

Native microbenchmarks report complete iterations per second. For `freeze`,
`final_drop`, `fill`, and `init_fill`, an optional latency span measures only that
operation; their throughput still includes setup and reclamation. Clone/slice
iterations include nonfinal drop. Fan-out creates then drops all requested views.
Handoff measures the channel, cross-thread final drop and acknowledgement together,
not isolated Drop. A timer/control case exposes measurement overhead; timings are
not automatically adjusted by subtracting it.

Per-operation clocks/histograms are enabled only in distinct latency trials.
Histograms have fixed capacity (1 ns through 120 seconds, three significant
digits), never resize during recording, and count out-of-range samples.
The report omits p99.99 below one million samples. All percentiles describe
observed samples, not bounds. Rejections have separate counts and latency
histograms; a fast failure is never counted as useful throughput.

Microbenchmarks repeatedly reuse a small active working set, usually the same
LIFO slot. Prepared capacity can be much larger than the touched working set.
Their copy bandwidth is a cache-dependent single-producer measurement, not an
estimate of sustained multi-stream ingress bandwidth.

TCP uses real loopback ingress, four Tokio workers, twenty queues with eight
entries each, vectored writers and validating readers. The old
[24-frame correctness example](reference-spike.md) remains unchanged.
The new generator has no per-frame ACK. A deterministic arrival schedule and
class sequence are shared between backends: 50/50 512/1500 bytes or 95/5
64 KiB/2 MiB. A late generator records missed arrival slots without a catch-up
burst. The last millisecond uses cooperative yielding to avoid making Tokio's
timer resolution the requested inter-arrival interval.

The baseline calibration tries increasing rates for two seconds and requires
at least 98% of planned ingress and twenty-way deliveries, with no disconnect.
Normal trials use 50%, 100% and 150% of the highest passing candidate; slowed-reader
trials use that candidate with both disconnect and explicit skip policies.
Subscriber zero pauses after each received message for the greater of 5 ms
and four arrival intervals, so it is slower at both packet and frame rates.
This is a coarse, workload-specific reference rate, not a measured universal
capacity or a production sizing recommendation.

Both backends have the same classes, 64 slots per class, domain limits,
queues, arrival schedule and overload handling. Ingress gets at most three
application attempts on `Contended`; view failure is distinct from acquire
failure and stops ingress before enqueue. Accepted, enqueued, delivered, skipped,
disconnected, overdue (>100 ms), generator-missed and never-attempted-after-stop
work are separate. Delivered latency starts at scheduled arrival, not successful
acquisition. Early termination divides useful work by the full planned duration;
its short successful prefix is not presented as sustainable throughput.

All queues and tasks must terminate or be cancelled and joined, with a two-second
drain deadline, before owner-side destruction. Every scenario asserts zero charge
and fully returned class capacity. Reader cancellation returns its accumulated
delivery counters and histogram, including slow samples; it never discards them.
Each cancelled reader is reported, and unread/partial frames remain undelivered.

## Baseline and allocation instrumentation

The comparison is `Bytes + byte budget + class capacity + bounded queues`.
The baseline is measurement support, not another ABPS implementation:

- Admission reserves class capacity and full backing charge under one mutex.
  It uses the same smallest-fitting-class selection and error distinctions.
- A charged owner contains the class-capacity Vec and an RAII permit. The payload
  is destroyed before the permit refunds capacity and budget.
- `Bytes::from_owner` makes that permit live until the final owning view
  disappears. It does allocate ownership metadata. Native nonempty slices and
  clones share it; empty benchmark views use clone/truncate to retain backing,
  matching ABPS rather than Bytes' empty-slice shortcut.
- Payload allocation occurs per baseline acquisition. ABPS prepares its payload
  storage. Equal admission/capacity bounds do not imply equal idle reservation or
  identical RSS. The baseline does not imitate dynamic domain retirement or
  production progress guarantees; those are outside these comparisons.

The separate `perf_alloc` executable instruments the Rust global allocator using
stats_alloc. An intentional 1234-byte allocation/free must first be detected.
Region boundaries exclude formatting and source preparation from isolated core
measurements. A new-thread return is measured after thread startup. TCP phase
totals distinguish runtime setup, adapter preparation, active work, drain and
teardown/snapshot. Histograms are prepared before the active phase, and JSON
reporting follows measurement. Active TCP totals combine core, channels and runtime.
Global counters alone cannot attribute an allocation to a particular component;
only the isolated core regions justify per-operation allocation counts.
They do not measure kernel socket memory or arbitrary libc allocations.

## Tracing

`scripts/performance-trace.sh OUTPUT_DIRECTORY` records separate CPU samples,
symbol reports, syscall summaries and counter availability. It expects perf and
strace under `/tmp/abps-perf/tools`; override `ABPS_PERF_TOOLS` and
`ABPS_PERF_TARGET` for other local paths. It uses cpu-clock sampling and DWARF
call stacks. Profiled elapsed times are diagnostic, not native benchmark results.

The tools can be downloaded/extracted without installing system packages:

```sh
mkdir -p /tmp/abps-perf/apt-lists/partial /tmp/abps-perf/tool-downloads
apt-get -o Dir::State::Lists=/tmp/abps-perf/apt-lists update
cd /tmp/abps-perf/tool-downloads
apt-get -o Dir::State::Lists=/tmp/abps-perf/apt-lists download \
  linux-perf strace libopencsd1 libdebuginfod1t64 libdw1t64 \
  libpython3.13 libnuma1 libbabeltrace1 libtraceevent1
for package in *.deb; do
  dpkg-deb -x "$package" /tmp/abps-perf/tools
done
```

Dependency package names depend on the distribution. Check missing libraries
before running. No kernel tracing settings are changed.

## Evidence and limitations

The host is a shared Hyper-V VM, not a performance gate host.
RSS sampling every 20 ms can miss peaks, especially in short correctness probes.
Prepared backing, modeled metadata and RSS are different measurements. This audit
is not a long soak, weak-memory proof, nonblocking Drop proof, or production
performance guarantee.

### Environment and data

Measured on 2026-10-03, Rust 1.96.0, Linux 6.12.90+deb13.1-amd64,
x86_64 GNU, System allocator. The VM exposes an AMD Ryzen 9 5900X model,
ten logical CPUs / five cores, and one NUMA node. No CPU governor is exposed.
Release builds include line information, with no native-CPU or LTO override.
The timing and allocation executables are separate.

Microbenchmarks use CPU 0; multi-thread trials expand the allowed set in order
0,2,4,6,8,1,3,5. Observer/handoff trials allow CPUs 0,2; TCP allows 0,2,4,6.
These are process affinity sets, not individual worker pinning. The VM and scheduler
are uncontrolled; the results are diagnostic and do not establish hard latency
bounds or a regression threshold.

Raw data in this workspace:

- `/tmp/abps-perf/20261003-reference-audit`: native microbenchmarks, snapshots,
  retention and isolated allocation counters. Ignore its preliminary TCP rows.
- `/tmp/abps-perf/20261003-tcp-audit`: TCP measurements with cancellation preserving
  completed reader samples. This supersedes the preliminary TCP collector.
- `/tmp/abps-perf/20261003-allocation-audit`: final isolated counters and revised
  TCP phase accounting.
- `/tmp/abps-perf/20261003-traces`: CPU samples, symbol reports, syscall summaries
  and counter availability.
- `/tmp/abps-perf/quick-cancellation-fixed`: short instrumentation/lifecycle probes.

Environment files include source hashes and exact commands. Numbers below are
medians of three trials, unless a count is explicitly from the isolated allocation
pass. Medians of per-trial percentiles are not a merged histogram. The microbench
binary was unchanged throughout its complete comparison matrix.

### Shared-root synchronization dominates concurrent work

1500-byte acquire/freeze/final-drop cycles, with clocks and histograms enabled
equally in all rows; total storage capacity stays at 64 slots. Each thread has
an independent domain.

| Threads | One root: accepted M cycles/s | Contended attempts | Success p99 | Separate roots: M cycles/s | Separate roots: p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.783 | 0% | 161 ns | 4.783 | 161 ns |
| 2 | 2.168 | 58.67% | 1.693 us | 9.314 | 171 ns |
| 4 | 1.897 | 60.12% | 15.975 us | 18.639 | 181 ns |
| 8 | 1.476 | 66.51% | 41.087 us | 29.786 | 211 ns |

All separate-root trials have zero acquisition failures. The eight-thread control
is about 20 times faster in useful cycles, despite equal total class capacity.
This isolates root-wide synchronization and shared state as the leading scaling
problem; it is not a recommendation to silently shard quotas or change ownership.
The reference's acquire, freeze, every view, and final return meet at the same
state mutex in [the reference core](../src/reference/mod.rs). Long wall-clock outliers also include VM
scheduling and cannot be attributed entirely to that mutex.

The budgeted Bytes baseline also loses scalability with one budget mutex, but
reaches 5.410 / 7.492 / 3.692 / 2.784 M accepted cycles/s at 1/2/4/8 threads.
Its eight-thread contention rate is 60.87%. Thus merely having a byte budget
does not explain all ABPS reference overhead.

### Wrapper allocation remains, without a small-operation speed win

Native single-thread rates, with no per-operation clocks/histograms, 1500 bytes:

| Complete iteration | ABPS M iterations/s | Budgeted Bytes M iterations/s |
| --- | ---: | ---: |
| Acquire and mutable drop | 10.723 | 14.616 |
| Acquire, freeze, final drop | 7.556 | 9.894 |
| Clone and nonfinal drop | 29.149 | 33.367 |

The instrumented empty/control span has p50 30 ns and p99 41 ns. Adding clocks
and histograms reduces the ABPS full-cycle rate to roughly 4.5 M/s. Do not compare
that rate to the native 7.556 M/s as a library regression, or interpret measured
50-ns freeze/drop spans as timer-free instruction costs.

The allocator positive control detected the intentional 1234-byte allocation and
free. On 10,000 warmed iterations, at 1500 bytes:

| Path | ABPS allocations / frees | Bytes allocations / frees | Requested allocation bytes per iteration |
| --- | ---: | ---: | --- |
| Acquire + mutable drop | 0 / 0 | 10,000 / 10,000 | ABPS 0; Bytes 1500 |
| Acquire + freeze + final drop | 10,000 / 10,000 | 20,000 / 20,000 | ABPS 64; Bytes 1564 |
| Clone, slice, or 20-view fan-out + nonfinal drops | 0 / 0 | 0 / 0 | 0 |
| Initialize + fill + mutable drop | 0 / 0 | 10,000 / 10,000 | ABPS 0; Bytes 1500 |

No reallocations were observed in these regions. First final return on a new
thread performed no allocation: it freed one 64-byte wrapper for ABPS, versus
payload plus wrapper for Bytes. The wrapper results validate the source inventory,
not the production no-allocation contract. Preparation, teardown, runtime and
kernel storage have different accounting.

### Observability participates in contention

Full snapshot, equal numbers of domain records and slots, 8-byte payload slots:

| Records and slots | Snapshot p50 | Snapshot p99 |
| --- | ---: | ---: |
| 64 | 0.341 us | 0.401 us |
| 4096 | 19.343 us | 58.879 us |
| 65536 | 327.167 us | 797.695 us |

A snapshot with 64 domain records and 64 slots allocated/freed three vectors,
requesting 8240 bytes. [Snapshot construction](../src/reference/state.rs) scans and collects
these vectors while holding the same mutex used by the datapath.

With 65,536 domain records and 512-byte slots, a single acquire/freeze/drop worker
plus an observer produced:

| Requested observation rate | Accepted M cycles/s | Contended attempts |
| --- | ---: | ---: |
| None | 4.689 | 0% |
| 1 Hz | 4.743 | 0.70% |
| 100 Hz | 4.421 | 7.30% |

The small throughput difference between none and 1 Hz is noise, not an
improvement. There were two observations per 1-Hz trial and 188–189 per 100-Hz
trial; snapshot work delays the next observation. These include first-use/cold
snapshot costs, unlike the separately warmed snapshot loop. Success p99 stays
around 161–191 ns and misses much of this interference. Failure counts and
observer durations expose work that a success-only percentile would hide.

### Initialization and class search are secondary costs

Explicit zero initialization followed by copying, versus copying directly:

| Payload | Direct fill iterations/s | Initialize + fill iterations/s | Reduction |
| --- | ---: | ---: | ---: |
| 64 KiB | 602,681 | 403,088 | 33.1% |
| 2 MiB | 15,608 | 10,884 | 30.3% |

These are ABPS native complete iterations, including acquire and mutable release,
with an actively reused slot. They establish a cost for the extra pass, not
permission to expose stale/uninitialized memory. The generic safe ingress contract
still requires correctly initialized writable bytes.

Selecting the first versus last class among 64 classes gives 10.611 versus
8.453 M acquire/drop iterations/s, a 20.3% reduction; with eight classes it is
10.410 versus 10.138 M/s. Selection is linear in the reference. It deserves a
bounded lookup decision, but ranks below synchronization and snapshot interference.

### Retention and integration progress

Both backends retained a 2-MiB charge for a 128-byte view, and then for an empty
view alone. Another allocation in the same domain returned `BudgetExceeded`;
a second domain below its own limit returned `ClassDepleted` for that occupied
class. Dropping the final empty view restored capacity and admission exactly once.
The input/output-headroom scenario fails immediately instead of waiting for
an output buffer that cannot become available.

Every completed scenario drained its ledgers and destroyed storage on the owner
side. These experiments found performance limitations and integration-policy
risks, not a lost slot, budget overshoot or double return in the exercised model.

### TCP: offered load and useful delivery tell different stories

The coarse baseline calibration selected 1000 packet arrivals/s and 200 frame
arrivals/s. Normal ten-second trials below use default disconnect on queue
overflow. Delivery percentage has a denominator of
`planned arrivals * 20 subscribers`, including work never generated or never
delivered after disconnect. p99 is conditional on delivered samples.

| Workload / offered rate | Backend | Accepted / planned | Delivered / planned deliveries | Delivery p99 |
| --- | --- | ---: | ---: | ---: |
| Packets / 500 per s | ABPS | 99.74% | 99.74% | 3.326 ms |
| Packets / 500 per s | Bytes | 99.72% | 99.72% | 3.006 ms |
| Packets / 1000 per s | ABPS | 99.23% | 99.23% | 3.512 ms |
| Packets / 1000 per s | Bytes | 99.41% | 99.41% | 3.840 ms |
| Packets / 1500 per s | ABPS | 98.91% | 42.50% | 3.879 ms |
| Packets / 1500 per s | Bytes | 99.09% | 71.16% | 3.736 ms |
| Frames / 100 per s | ABPS | 100% | 100% | 7.913 ms |
| Frames / 100 per s | Bytes | 100% | 100% | 7.074 ms |
| Frames / 200 per s | ABPS | 99.95% | 99.95% | 8.036 ms |
| Frames / 200 per s | Bytes | 100% | 100% | 7.234 ms |
| Frames / 300 per s | ABPS | 99.23% | 99.23% | 7.516 ms |
| Frames / 300 per s | Bytes | 98.60% | 98.60% | 6.865 ms |

At 1500 packets/s, ABPS deliveries ranged from 33.56% to 94.03%, with 6–16
subscribers disconnected; Bytes ranged from 54.20% to 84.23%, with 8–13
disconnected. These overlapping, noisy outcomes do not establish a reliable
throughput winner. A queue of eight messages with immediate disconnect is
sensitive to transient service gaps and bursts from TCP buffering. Accepting
almost all ingress and retaining a low success p99 would conceal substantial
lost useful work.

All 60 final ten-second trials and 11 calibration trials completed without an
allocator-triggered ingress stop. There was one transient Contended attempt
across those 71 runs, handled within the existing three-attempt policy. Thus the
severe contention in the eight-producer microbenchmark did **not** reproduce in
this single-producer multicast workload. Multiple independent ingress streams,
sustained overload and different scheduling arrangements remain separate workloads.

The generator records missed arrivals even when no allocator failed. No latency
sample is invented for undelivered work; losses stay visible in the denominator.
There were too few delivery samples per trial to publish p99.99.

With subscriber zero explicitly slower:

| Workload / policy | ABPS slow-reader p99 | Bytes slow-reader p99 | Operational outcome |
| --- | ---: | ---: | --- |
| Packets / disconnect | 9.932 s | 9.932 s | One subscriber disconnected; reader still has TCP backlog at the drain deadline. |
| Packets / skip | 9.924 s | 9.924 s | No disconnect; thousands of new deliveries skipped; old queued data remains late. |
| Frames / disconnect | 200.7 ms | 195.7 ms | One subscriber disconnected; 13 completed deliveries for that subscriber. |
| Frames / skip | 1.584 s | 1.662 s | Roughly 1464–1468 new deliveries skipped; about 531–536 completed for that subscriber. |

Slots and byte limits cannot by themselves enforce a freshness deadline.
With ordinary TCP writes, ABPS ownership can finish while copied data remains in
socket buffers. In the packet trials, cancellation preserved roughly 1960–1973
completed slow-reader samples rather than silently losing their worst delays.
The report records remaining undelivered work after bounded drain.
This supports retaining explicit queue, overload and delivery-deadline policies;
it does not justify expanding ABPS into a network scheduler.

At 200 frames/s, sampled peak process RSS was about 183 MiB for ABPS and 53 MiB
for Bytes. ABPS prepares 132 MiB of payload capacity for these two classes,
whereas the baseline allocates on admission. Reader buffers, runtime, allocator
caches and measurement state are also present; kernel socket memory is not in
process RSS. This is the reservation tradeoff, not evidence of a leak or a bound
on all streaming-system memory. A ten-second run is not a soak result.

### CPU and syscall traces corroborate the bottleneck

Five separate cpu-clock user-space profiles used 199-Hz sampling with DWARF
stacks: shared roots, separate roots, observation, large initialization/copy, and
TCP. They contained 2134 / 7145 / 919 / 897 / 595 samples respectively, with zero
reported lost samples. Their percentages are CPU samples, not wall-clock waiting
fractions.

- Eight-thread shared-root cycling spent 28.73% of sampled user CPU in
  `std::sys::sync::mutex::futex::Mutex::lock_contended`, with stacks through both
  `BufMut::freeze` and `Allocation::drop`. Another 12.84% was in the libc syscall
  wrapper. This confirms that final return really enters a contended blocking
  path in the reference.
- The separate-root profile moved activity to acquire (20.28%), freeze (12.83%)
  and allocation Drop (12.50%). Explicit malloc/free symbols still accounted
  for about 5.5% of samples; avoiding contention does not remove wrapper churn.
- The observer profile contains both `Root::snapshot` (2.94% self samples) and
  Vec collection (2.18%). Its small total CPU share can still affect other
  operations because the scan holds their shared mutex.
- More than 97% of large initialize/copy samples landed in libc memory loops.
  Private libc symbols were unresolved; disassembly at offsets 0x162d60 and
  0x1635c0 shows vector load/store-copy and repeated-store loops. This supports
  the measured extra-memory-pass cost without attributing unresolved addresses
  to guessed function names.
- The TCP profile spent 35.63% in `__vdso_clock_gettime`, with generator pacing
  on the stacks, plus scheduler activity. This is a limitation of the load
  harness itself: its submillisecond pacing consumes runtime CPU. It is another
  reason these rates cannot size a production server. A future throughput
  campaign needs a separate load-generator resource budget or external producer.

Whole-process strace summaries for one versus eight shared-root workers recorded
4 versus 177,423 futex calls. In the latter, futex represented 99.80% of the
summarized syscall time. Startup, barriers and joining are included; strace changes
scheduling, so these call counts are diagnostic rather than native rates.
The TCP syscall trace also shows actual `writev`, `recvfrom` and `epoll_wait`.

Software CPU/time, context-switch, migration and page-fault counters were
available. The four-second, eight-thread counter probe reported 676,597 context
switches, but hardware cycles, instructions, cache misses and branch misses were
all unsupported. No cycles/op, cache-miss or atomic-count claim is made.
Kernel policy was not changed.

The revised global-allocation pass at 500 scheduled packets/s for two seconds
recorded 1027 active-phase allocations for 1000 accepted ABPS frames and 2023
for 998 accepted baseline frames. These are combined core/runtime/channel totals,
not exact allocations per channel or a native timing comparison. Preparation,
drain, cold teardown and reporting are separate; all isolated core counts above
were reproduced with this final executable.

## What to change next

1. **Settle the Shared synchronization protocol before implementing it.** The
   largest measured cost is coupling independent domain admission, immutable
   reference creation, final return and diagnostics through one root mutex.
   Define the minimum shared state and refine all four ledgers, close/drain,
   finite reference counts and completed-drop requirements. A mutex replacement
   or counter tree is not accepted merely because it improves this benchmark.
2. **Prepare ownership metadata with storage.** One 64-byte allocation/free per
   frozen allocation demonstrably remains. Removing it needs ownership/reuse
   proofs and allocation-hook regression coverage, not just a faster allocator.
3. **Separate cheap operational counters from full snapshots.** Keep the
   consistency contract explicit; monitoring should not become an admission
   competitor through full-table allocations and scans.
4. **Keep byte quotas, queue limits and delivery deadlines independent.** The
   packet experiment accepted about 99% of ingress while sometimes delivering
   less than half the planned fan-out. Test multiple real ingress streams and
   isolate the load generator before interpreting overload thresholds.
5. **Then optimize explicit byte work and lookup.** The extra initialization
   pass and 64-class scan are measurable, but initialization safety cannot be
   traded away. Validate an actual ingress API before proposing unsafe writes.

No library algorithm, accounting rule, failure meaning or shutdown semantics was
changed to obtain these results.

## Validation

The audit adds two safe example executables, shared measurement support, a
sequential runner, a trace script and development-only dependencies. Both examples
test baseline charge retention/failure rollback and cancellation preserving slow
consumer samples. The latter catches a measurement defect that otherwise erased
the worst reader's completed samples; preliminary TCP results are excluded.

The runner checks workload conservation independently of allocator assertions:
planned arrivals split into attempted/missed/unattempted, accepted fan-out splits
into enqueue/skip/reject/disconnected-target work, and enqueued work splits into
delivered and undelivered. Delivered histogram sample counts match completed
deliveries. Every successful scenario verifies capacity/admission drain and
owner-side teardown.

The workflow configures allocation positive-control and retention sanity checks,
without speed thresholds. Local fmt, Clippy with warnings denied, all 28 tests,
three doctests, both correctness-example policies, both new CI sanity commands
and the quick harness passed. Python/shell syntax, Markdown links/anchors/fences
and whitespace were checked.

The completed evidence includes 335 native core/snapshot/retention trials,
60 ten-second TCP trials plus 11 calibration trials, separate allocation passes,
five CPU profiles and syscall/counter probes. Library sources and existing tests
remained byte-identical by SHA-256. Remote CI, production Loom/Miri, sanitizers,
long soak and cross-target performance are not claimed.

## Later adapter experiments

The [AP-12 report](reports/2026-10-19/hot-path-experiments.md) examines the current
Mosaic TCP adapter at fixed arena size. It does not update this page's historical
reference-backend measurements. The [harness guide](performance-harness.md#adapter-hot-path-experiments-ap-12)
describes opt-in staging, external storage ceilings and writer loss accounting.

The opt-in [adaptive memory experiment](adaptive-memory.md) compares fixed and
adaptive soft admission with explicitly charged small-root topology. Retention
replay, trace and TCP timing are separate evidence; defaults remain unchanged.
