# Architecture follow-up: admission, domain reuse, and I/O

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

This change implements SI-01 through SI-04 in
[CONTRIBUTING](../CONTRIBUTING.md#berry-single-issuer-experiment). The default safe
reference remains the oracle. The Berry backend remains experimental;
there is no new multi-issuer algorithm, counter tree, quota migration, magazine,
participant registration, or hard real-time guarantee.

The later [reservation and freshness follow-up](reservations-performance.md)
adds opt-in SI-05/06 policies and records their costs and validation. Measurements
on this page retain their original configuration and date.

## What changed

- Domain opening no longer scans all records. The last control handle immediately
  recycles its own drained record or links it into a prepared issuer-only FIFO.
  `poll_domains(n)` visits at most the entry-time pending count and the supplied
  limit; busy records rotate. `DomainMaintenanceRequired` asks the caller to run
  explicit maintenance, without promising capacity. Snapshots are read-only.
  Only record bookkeeping waits for maintenance: buffer Drop still returns the
  slot and full charge synchronously. FIFO links/counts are in the reservation.
- The TCP adapter reserves queue permits before acquiring payload and creates
  views only for actual recipients. It prepares every view before publication.
  Default Disconnect still closes an overflowing subscriber; an unrecoverable
  allocation failure stops ingress. Explicit Skip drains rejected payload through
  prepared scratch and continues at the next header. Closed and ReferenceLimit
  remain fatal; partial frames and cancelled operations release through RAII.
- `bytes-interop` is off by default. It implements `bytes::BufMut` for the exclusive
  Berry lease, using a bounded uninitialized tail and checked advancement.
  `--ingress=read-buf` uses Tokio's safe length-limited adapter; initialized reads
  remain the default and are still required for generic `std::io::Read`.
  There is no owning conversion to `Bytes` and no Tokio dependency in the core.
- `--write-batch=1` remains default; up to eight already-ready messages can share
  sixteen stack descriptors. The writer never waits to assemble a batch. Completed
  frames are released during partial writes, before an unfinished tail completes.
- Each server report includes a cold `PipelinePlan`: per-root class counts, domain
  limits, ingress and output headroom, independent queue/writer retention bounds,
  scratch, and modeled external item/descriptor storage. Those bounds do not grant
  dedicated capacity or bound Tokio/kernel/allocator overhead.

## Measurement changes

A pacing thread feeds bounded per-stream source queues. Independent socket writers
and validating readers run on the client I/O runtime; the server is a separate
process. `--generator-cpus`, `--reader-cpus`, and `--server-cpus` record their CPU
sets. Disjoint logical CPUs do not eliminate SMT or VM interference.

Every offered frame remains in the denominator. Reports distinguish pacing
misses, source queue refusals, source write failures, ingress discards, subscriber
queue drops, write completion, and validated delivery. The runner checks:

```text
planned = attempted + pacing misses
attempted = source enqueued + source queue refusals
source enqueued = sent + failed writes
received = accepted + discarded
received × subscribers = enqueued + skipped + rejected + absent + ingress-rejected
validated delivery <= writer completed <= enqueued
```

Each stream has its own seeded payload sequence. Routing/skew cannot select all
large frames into the same shard. `--keyframe-burst` preserves the long-run 5%
large-frame distribution while grouping keyframes. `--source-queue` bounds queued
source descriptors. A blocked or failed source cannot stop another stream's
schedule. Late pacing slots become misses, never a catch-up burst.

Permanent probes cover `--case=churn`, `--case=working-set` with 1/8/64 slots,
and `--case=concurrent-views --backing=same|distinct`. The `architecture` suite
runs those with load ramps, queue sizes 8/32, remote return, ingress and batch
variants, and skewed keyframe pressure. Trial order is seeded and randomized.
Commands, source/binary SHA-256, CPU topology, process RSS and CPU ticks accompany
results. Allocation instrumentation and perf/strace run separately from timings.

```sh
CARGO_TARGET_DIR=/tmp/abps-reference-target cargo build --release --locked \
  --features bytes-interop --examples \
  --config profile.release.debug=1 --config 'profile.release.strip="none"'
python3 scripts/berry-performance.py --suite=architecture \
  --output=/tmp/abps-architecture --seconds=.5 --tcp-seconds=2 --repeats=3 \
  --generator-cpus=9 --reader-cpus=8 --server-cpus=0,2,4,6
```

CPU arguments above describe the measured ten-vCPU host, not portable defaults.
Use a fresh output directory after any source, binary, or CPU-set change. The
explicit `--server-binary` option can compare a preserved old server with the
current client; the old server hash is recorded independently.

## Validation and evidence

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The local evidence file (`evidence/architecture-performance-2026-10-03.json`) contains
configuration, SHA-256 identities, trial medians/ranges, errors, allocation counts and
profiler records. Raw outputs are in `/tmp/abps-architecture-2026-10-03/`. The before
checkout and binaries were preserved before editing; the library comparison compiles
exactly the same probe source against the old and new libraries. TCP comparisons use the
same current client with the preserved old or current server. No build, test or profiler
ran alongside native measurements.

Runs completed on the Linux 6.12.90 Hyper-V VM, Rust 1.96.0, ten logical CPUs/five
exposed physical cores: 192 randomized native trials; 54 matched domain/working-set
trials; 12 matched core trials; 12 matched TCP trials plus 12 CPU-redistribution
trials; nine allocation configurations; and four separate perf/strace runs. The
native suite used 0.15 s warm-up/0.5 s micro measurements and 2 s TCP measurements;
matched TCP used 3 s. Each native configuration has three repetitions. This is
short experimental evidence on a shared VM, not a performance gate or soak test.

### Domain reuse and core work

A nearly full domain table now has constant opening/release work. Values below
are medians of three trials, each containing 1,000 timed close/open operations
using the same probe source and compiler settings.

| Prepared records | Before p50 | After p50 | Before p99 | After p99 |
| --- | ---: | ---: | ---: | ---: |
| 64 | 170 ns | 81 ns | 181 ns | 91 ns |
| 4,096 | 6,632 ns | 80 ns | 20,549 ns | 91 ns |
| 65,536 | 105,479 ns | 80 ns | 258,367 ns | 101 ns |

These immediate-reuse trials are distinct from delayed-record maintenance. The
latter has deterministic fairness, bounded-work and independent model tests;
final buffer Drop still performs all payload return and refund work.

The matched acquire/freeze/drop probe with per-operation latency instrumentation
remained approximately unchanged: 7.18 -> 7.15 M operations/s on one worker and
28.64 -> 28.74 M/s on four, with median successful p99 of 91 ns in each case. This
is not the native throughput of an uninstrumented loop.

The 2 MiB working-set comparison exposes the bandwidth limit instead of presenting
one repeatedly reused slot as sustained streaming throughput. At 64 rotating
slots, copy throughput was 15.94 -> 15.37 GB/s; zero-plus-copy was 10.83 -> 10.54
GB/s. At one slot, copy was 36.86 -> 36.72 GB/s. There is no demonstrated general
copy-throughput improvement; `read-buf` provides an explicit way to omit the
initialization pass when the actual I/O API supports it.

### Overload behavior and latency

The matched workload offered 20,000 packets/s, four streams/workers, twenty
subscribers per stream, remote return and explicit Skip. Server CPUs were
`0,2,4,6`, pacer `9`, client I/O `8` (the client CPUs are SMT siblings).

| Queue | Before delivery, median | After delivery, median | Before stopped streams | After stopped streams |
| --- | ---: | ---: | ---: | ---: |
| 8 | 70.41% | 73.20% | 1–2 per trial | 0 |
| 32 | 26.78% | 85.03% | 4 per trial | 0 |

The previous Skip path terminated ingress on BudgetExceeded/ClassDepleted. The new
path records the refusal, drains that frame, and resumes; every matched run drains
all actual ownership and charge at shutdown. The improvement is overload survival,
not a claim that all offered frames are delivered.

At queue 32, after-change p99 was **943 ms** with a single client I/O CPU. The old
server's p99 of about 5.7 ms describes only its small surviving prefix before all
streams stopped; it is not evidence that its full offered workload was faster.
A separate three-trial CPU redistribution used server `0,1,2,3`, client I/O `4,6`,
and pacer `8`: new p99 was 10.45 ms, delivery 66.16%, with no ingress stops. This
also reduces physical server cores, so it is a resource-allocation tradeoff, not
an isolated causal measurement of reader count. The old server still stopped all
four streams at queue 32 and delivered a median 0.68% in that layout.

The native lower-load ramp reached median 99.70% delivery/p99 0.94 ms at 1,000
packets/s and 98.28%/1.36 ms at 5,000 (queue 32). Kernel queues, client capacity,
overload policy and duration dominate the loaded tails. ABPS memory bounds do not
supply an end-to-end latency bound; application deadlines and queue policy remain
necessary integration decisions.

### Optional I/O optimizations and remaining limits

At 20,000 packets/s and queue 32 in the 2 s native suite:

| Ingress / batch | Median delivered | Async write calls per complete frame |
| --- | ---: | ---: |
| Initialized / 1 | 93.32% | 1.000 |
| Initialized / 8 | 94.48% | 0.221 |
| Read-buf / 1 | 86.58% | 1.000 |
| Read-buf / 8 | 95.03% | 0.228 |

Packet-sized initialized reads remain a reasonable default in this workload;
read-buf and batching remain explicit options. The separate strace pass observed
200,640 writev calls for 200,640 completed frames at batch 1, versus 133,067 for
937,500 frames at batch 8. Tracing changes load acceptance substantially: these
are syscall-count observations, not native timing comparisons. Software perf
collected 872/363 samples with zero lost samples; kernel.kallsyms accounted for
82.22%/65.84%, plus separately named kernel modules. Its sample count is too small
for detailed cache-line attribution. No new hardware-counter claim is made.

The concurrent-view probe still records median Contended fractions of 34.89% on
four threads and 68.62% on eight threads cloning one backing. Distinct backings
had no contention failures. This change does not redesign shared refcount traffic
or silently add retries. Root partitioning still strands capacity under skew;
the keyframe/slow-reader test deliberately records class depletion and continued
Skip processing, not guaranteed availability.

### Checks actually run

- Formatting; Clippy with default, Berry and all features; default tests
  and bytes-interop all-target tests; seven doctests. The feature suite includes
  33 library tests, two independent ledger property tests, eight existing TCP
  tests and fifteen tests in each probe executable (shared modules are duplicated).
- Six Loom models exercise the production atomic protocol. The new model checks
  domain completion before reuse without relying on a thread join.
- Eighteen focused storage tests pass Miri seeds 1, 17 and 42. Only intentional
  forget/abandonment leak reports are ignored; UB and race checks remain enabled.
- Maintenance fairness, snapshots without cleanup, last-control immediate reuse,
  remote completion, generation identity, queue preflight, default failure,
  framed skipping, all-or-nothing fan-out, partial/cancelled read-buf, short and
  zero writes, and release before a stalled batch tail have regressions.
- The allocation harness detects its allocating positive control. The prepared
  Berry core, fresh-thread final return, failure paths, record maintenance
  and bytes cursor operations all report zero allocations/frees/reallocations.
- Two temporary mutations fail the expected regression: ignoring the maintenance
  visit bound and marking the unfilled tail initialized. Mutation builds initially
  reused the working target cache; a subsequent test invoked that intentional
  mutant. The package's debug artifacts were cleared, and the final full suite
  passed in a separate target directory with RNG seed 424242. Benchmark release
  binary hashes remained unchanged.
- The default library's normal dependency tree is empty. CI now includes the
  optional interop tests and Miri coverage; no remote CI execution is claimed.

Sanitizers, fuzzing, long soak, other targets and an isolated performance host were
not exercised here. Miri and the bounded Loom state spaces are evidence, not a
proof of soundness. Production Shared helping and the deferred extensions remain
outside this experiment.
