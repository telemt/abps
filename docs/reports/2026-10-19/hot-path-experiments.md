# Adapter hot-path experiments

This report concerns the private TCP examples, not a new allocator backend.
The existing dirty checkout was snapshotted before work. Library implementation
under `src/`, manifests, atomic ordering, arena layout, slots and domain limits
remain unchanged. AP-12 extends the adapter contract; AP-10/11 queue policy and
SI-06 per-socket-poll deadlines remain applicable.

## Method and scope

Candidate binaries and source manifests were saved separately. Seven interleaved
trials compare each individual representation/timekeeping experiment at 20,000
planned arrivals/s, two seconds per window. Separate longer paired runs use the
original AP-11 executable and the retained adapter. Server CPUs 0/2, validating
readers 4/6 and generator 8 are distinct physical cores. This is a shared VM,
not the dedicated performance host required for promotion.

Timing excludes compilation, sanitizer execution and profiling. Delivery fractions
use planned arrivals times fan-out, including pacer misses and every reported
loss. CPU samples are process ticks, not cycles or instructions. Hardware PMU
counters were unavailable; extracted perf and strace tools required no system
package change or kernel-policy change. Profiled timings are not native timings.

The default packet scratch remains 1500 bytes. With the same selected external
storage model, 2048 bytes fits the +5% envelope in both tested packet profiles;
4096 and 16384 do not. Arena reservation remains 286,352 bytes for the ordinary remote profile and
287,648 bytes across the two local roots. The model
is deliberately separate from RSS, total runtime memory and per-domain admission.

## Implemented and retained

- Explicit `staged-copy` ingress, using the existing scratch allocation. Direct
  initialized and ReadBuf modes remain available; the default is unchanged.
- Typed preflight ceiling on modeled external storage, checked before resources.
- Batch abort outcomes by deadline, policy, I/O, cancellation and unwind, with a
  strict full-drain equation for local queues. Remote queue residuals stay visible.
- Complete adapter-cycle probe, sampled tails, object and writer-future layouts,
  plus staged-fill allocation checks with an independent positive control.
- AP-12 tests and registry/safety-case mappings. No unsafe code entered the adapter.

## Candidate representations

Measured ordinary structures on this target: Header 48 bytes, Message 104 bytes,
backend Frame enum 40 bytes, Mosaic Buf 32 bytes, adaptive Controller 336 bytes.
The retained writer futures occupy 2416 bytes with Tokio queues and 2496 with
local queues, excluding referenced allocations and task headers.

Each candidate was isolated before combining anything:

| Candidate | Mechanism | Scope |
| --- | --- | --- |
| Clock checks | Remove duplicate maintenance around receive/timer ticks | Safe writer experiment |
| Local counters | Worker-local Cell/Rc in place of writer atomic counters | Safe writer experiment |
| Persistent timer | One pinned Sleep per writer, rearmed per deadline | Safe writer experiment |
| Controller sidecar | Prepare a Box only for enabled adaptive queues | Safe layout experiment |
| Typed projection | Project Mosaic frames at enqueue, remove nested backend dispatch | Disposable Mosaic-only adapter; not a general cold-dispatch API |
| Compact header | Store wire payload length as u32; Header 40, Message 96 bytes | Disposable safe representation |
| Single queue deadline | Queue entry owns deadline; reconstruct writer Message on transfer | Disposable safe representation; Q32 storage 4880 → 4368 bytes per queue |
| Prepared ring | MaybeUninit cells with explicit initialized interval and local ownership | Disposable unsafe representation only |

The ring keeps one initialized cyclic interval, transfers each cell exactly once,
and ends all front borrows before payload Drop. It preserves the existing credit
protocol and has no cross-thread queue API. Its native tests and actual adapter
Miri scenarios are separate from current-checkout evidence. Removing initialization
is a sensitive negative control; a passing ordinary stress test would not suffice.

## Measured outcome and promotion decision

The four short comparison campaigns completed 224 TCP trials. The final comparison
completed 84 trials: 14 profiles, two versions, three interleaved repetitions,
15 seconds per offered-load window. Six additional five-second runs were profiled
separately. All 314 records passed payload, accounting and confirmed-drain checks,
including revalidation after hardening the report schema. This is functional
validation of these runs, not evidence that they were lossless.

The final comparison is original AP-11 versus the retained adapter with explicit
staged-copy. It measures the complete change, including new loss instrumentation;
it does not isolate copy cost. The earlier five-way ingress shortlist compares
initialized, ReadBuf and staged-copy in the same instrumented implementation.
No variant earns promotion to the default on these shared-host results.

All values below are medians of three runs; arrows are original → staged-copy.
Timely delivery divides by **planned** deliveries. Latencies are measured from
scheduled arrival, among received frames; missing frames are not latency samples.
CPU is sampled server user+system time per delivered frame, excluding the separate
client/pacer processes. Three runs are not a confidence interval.

| Profile | Planned frames/s | Timely delivery % | p99 ms | Server CPU µs/delivery |
| --- | ---: | ---: | ---: | ---: |
| ordinary | 1000 | 99.79 → 99.76 | 2.488 → 2.691 | 15.20 → 14.70 |
| ordinary | 20000 | 97.01 → 96.29 | 1.540 → 1.776 | 9.19 → 9.15 |
| adaptive-healthy | 1000 | 99.69 → 99.75 | 1.734 → 1.550 | 14.42 → 13.87 |
| adaptive-healthy | 20000 | 98.74 → 98.28 | 1.353 → 2.513 | 8.65 → 8.50 |
| adaptive-pause | 1000 | 99.73 → 99.90 | 1.023 → 1.665 | 13.87 → 13.68 |
| adaptive-pause | 20000 | 97.33 → 96.81 | 2.165 → 2.273 | 8.62 → 8.47 |
| adaptive-constant | 1000 | 87.42 → 87.35 | 1.686 → 1.326 | 15.03 → 14.66 |
| adaptive-constant | 20000 | 85.52 → 85.11 | 1.756 → 1.764 | 9.18 → 9.01 |
| adaptive-ramp | 1000 | 88.16 → 88.52 | 3.658 → 3.647 | 15.23 → 14.98 |
| adaptive-ramp | 20000 | 84.28 → 84.71 | 2.570 → 2.529 | 9.11 → 8.91 |
| adaptive-stop-resume | 1000 | 89.05 → 89.02 | 1.500 → 2.245 | 14.93 → 14.75 |
| adaptive-stop-resume | 20000 | 96.63 → 95.48 | 2.075 → 2.458 | 8.84 → 8.58 |
| mixed-key0 | 200 | 100.00 → 100.00 | 3.844 → 4.014 | 40.83 → 41.67 |
| mixed-key1 | 200 | 100.00 → 100.00 | 4.014 → 3.860 | 41.67 → 41.67 |

Packet p50 remains roughly 50–55 µs at 20k/s and 80–92 µs at 1k/s; mixed-frame
p50 stays around 250 µs. The modest sampled CPU improvement does not establish a
delivery or tail-latency win. The sender itself misses scheduled arrivals: median
ordinary 20k/s misses are 3666 → 4288 out of 300,000 offered frames. These misses
remain in the denominator. Mixed keyed and unkeyed profiles deliver 100% in these
trials, with no late frames; their arena is 281,175,456 bytes, identical per pair.

Slow-reader tails must not be hidden by p99. For the 1k/s ramp, p99.9 is
417.071 → 421.790 ms; for stop/resume it is 893.387 → 891.290 ms. Across the full
final matrix, 9839 original and 7691 staged deliveries were late. Bytes already
accepted by the socket can arrive after their deadline; freshness policy cannot
revoke them. These aggregate counts are observations across different loads,
not a claim of a statistically established improvement.

The original records contain 15 unclassified unfinished enqueues. The retained
adapter reports 15 writer aborts across its distinct executions and zero
unclassified residuals in this campaign. Those are not matched individual frames;
the instrumentation now distinguishes batch loss from queue cancellation. All
current local queues reconcile exactly. Remote Tokio residuals remain a supported
explicit outcome rather than being silently counted as successful delivery.

## What the profiles show

For the same current binary and 5-second adaptive profile at 1k/s:

| Ingress | recvfrom calls | recvfrom errors | writev calls |
| --- | ---: | ---: | ---: |
| initialized | 14795 | 4843 | 19384 |
| read-buf | 14838 | 4854 | 19424 |
| staged-copy | 7283 | 0 | 19132 |

Staging removes about half the read syscalls in this instrumented workload.
It does not materially reduce writes. These are strace counts, not native latency
estimates; the tracer perturbs scheduling. The deterministic ready-source test
independently observes six reads becoming one for three coalesced frames.

At 20k/s, the three separate perf CPU-clock user profiles contain only
397/379/440 samples. The vDSO clock symbol accounts for 10.58%/15.57%/14.77% of
user samples (initialized/ReadBuf/staged). That identifies a candidate cost, not
its contribution to total CPU or a reason to weaken deadlines. Native final
campaign samples spend about three quarters of server CPU in system time.
Hardware cycles, instructions and cache events were unavailable; no cache-miss
or cache-line-contention conclusion is claimed.

The healthy 20k/s native profiles still average only about 1.04–1.05 frames per
writer API call despite a maximum batch of eight. There is no coalescing wait.
Thus reducing queue indexing or allocator dispatch alone targets a small part of
the observed workload. Ready work and I/O scheduling deserve measurement before
adding new allocator synchronization or an unsafe queue.

## Isolated candidates

Each row compares against its own interleaved, instrumented baseline, seven trials
per case. Complete-cycle times include sampled latency observation. They must not
be mixed across campaigns or interpreted as an isolated acquire benchmark.

| Candidate | FIFO cycle ns | Adaptive cycle ns | Decision |
| --- | ---: | ---: | --- |
| Fewer duplicate maintenance calls | 174.6 → 174.4 | 312.9 → 315.6 | No consistent improvement |
| Worker-local counters | 174.6 → 181.5 | 312.9 → 316.9 | Reject this change |
| Persistent writer timer | 178.1 → 175.8 | 322.5 → 311.2 | No stable TCP win; keep prototype |
| Boxed adaptive controller | 178.1 → 181.9 | 322.5 → 335.9 | Reject this representation |
| Unsafe prepared ring | 174.5 → 172.1 | 292.4 → 300.8 | Below 10% experiment gate; reject |
| Mosaic-specific frame projection | 174.5 → 173.5 | 292.4 → 291.9 | No convincing TCP gain |
| Compact wire-length header | 174.5 → 177.6 | 292.4 → 296.2 | Memory reduction only |
| Single queue deadline | 174.5 → 170.9 | 292.4 → 299.9 | 10.5% less queue storage; speed inconclusive |

The compact queue figures use the corrected probe that enqueues its actual
compact entry. The earlier probe still used the generic entry; its output is
retained but excluded from this comparison. Native tests cover the corrected
header/compact prototypes; only the ring has the separate Miri evidence described
above. None of these disposable variants has the retained implementation's full
verification matrix.

Keep staged-copy opt-in, preserve the direct path and retain the loss diagnostics.
No unsafe queue, timer representation, counter replacement or compact message
representation is promoted by this campaign. The next experiment should compare
bounded batches of already-ready writes and a compact safe queue on a dedicated
host, with independent CPU budgets for sender/readers. Preserve fresh deadline
checks before every socket poll, queue-credit transfer, no coalescing wait, payload
validation and every loss category. The current samples justify this direction;
they do not yet establish that it is faster.

## Evidence integrity

Several attempts were invalidated. A native campaign executed a mutation executable
from a shared Cargo target; a ring Miri attempt did not discover the ring detector.
A later cached release executable also failed the expected-capability check.
These records were retained, not counted as passing verification. Separate fresh
build roots and immutable copied executables are used for replacement evidence.
No mutation was applied to the main checkout.

Two later process-harness invocations failed setup: the first used a preserved
binary outside the Cargo target layout assumed by its resume test; the second
target lacked `mosaic_probe`. The complete rerun uses the verified target and
builds that additional example. Both failed logs remain in the evidence.

Raw logs, source snapshots, binaries, profiles and failed attempts live locally at
`verification/artifacts/hotpath-20261019` (an ignored link to the scratch campaign).
A persistent copy is stored under the ignored path
`docs/evidence/2026-10-19/hot-path-experiments.tar.gz` (139.8 MiB), SHA-256
`64e07f524e3c3c2016ad88417158e16f109d0a17a85fd187ab04ef347b34ad6d`.
Every archived file digest was verified. Build caches are excluded; source
snapshots, per-candidate patches and failed attempts are included. The first
packaging attempt captured its changing progress log and failed its own digest
check; that invalid archive is retained separately and is not this artifact.
These are not public download links. Reproduction uses the checked-in tests,
`verify.py`, `spike_alloc`, and `hotpath-review.py`; candidate-only source manifests
and patches are retained with the local experiment artifacts.

## Verification of the retained source

Source identity (verification harness):
`3b2afbba520d4749d61388af1097113253b325bba6a4ab38653e719bc1e6062d`.
Stable Rust 1.96.0, x86_64 Linux; safety tooling uses nightly-2026-10-03.
All Rust files and manifests remain byte-identical to this tested source. The
final Python-only schema and arena-comparison hardening has its own source
manifest and validation below; documentation was completed after Rust validation.

- 1484 native test executions and 77 doctests across the seven supported feature
  configurations; fmt, Clippy with warnings denied and remote-authority checks pass.
- Seven focused AP-12 tests pass in each of four Miri configurations: strict
  provenance, seeds 1/17, default configuration and `-Zmiri-tree-borrows` (28
  executions). This covers the selected adapter paths, not the full storage layer.
- Seven AP-12 tests pass under ASan and seven under TSan. Instrumentation setup
  verifies the production fence / accepted TSan acquire-load selection; sanitizer
  reports are failures even with exit code zero.
- The final Python harness passes 72 tests in both normal and optimized runs,
  including the six real-process tests. The earlier 71-test runs skipped those
  process checks and are not substitutes for this final run.
- A mixed writer-abort schema fails its regression against the former validator.
  All 314 measurement/profile records revalidate with the corrected validator.
  The final comparison runner passes 16 short functional TCP trials; a deliberately
  changed baseline reservation fails before any timing trial. All 42 measured
  original/candidate pairs have equal arena, capacity and domain/grant plans.
- The actual staged fill/reuse path completes 10,000 cycles with zero measured
  allocations, frees and reallocations. The positive control detects one allocation
  and one free. Local/adaptive prepared queues have separate zero-allocation probes.
- An isolated missing-byte mutation fails the named framing/payload detector after
  the unmodified detector passes. Failed artifacts are retained.

The disposable ring has 17 passing actual adapter Miri scenarios at seed 17,
including its independent deque/drop oracle; this is separate source evidence.
A standalone copy of the same ring source passes that named Miri detector, and
omitting initialization produces an explicit UB diagnostic. No ring code is
incorporated into the current allocator or safe adapter.
