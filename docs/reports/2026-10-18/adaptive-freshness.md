# Adaptive freshness at fixed arena capacity

This report covers the opt-in AP-11 policy over the existing AP-10 local queue.
It preserves allocator sources, synchronization, slots, grants and domain byte
limits. The experiment is in the TCP adapter, not the published library API.
English contract definitions are in
[CONTRIBUTING](../../../CONTRIBUTING.md#ap-11-adaptive-freshness-and-dependency-recovery);
the [guide](../../fixed-arena-delivery.md#adaptive-freshness) describes configuration.

## Policy and ownership

Each subscriber has a prepared queue of physical capacity Q and a logical window
W. Queue pressure contracts W and the next writer batch; calm periods restore
them. A 100 ms fast average and accumulated bad milliseconds distinguish a burst
from persistent lag. This is a deterministic controller, with no learned model.
Defaults are a 20 ms soft freshness target, 2000 ms pressure grace, 500 ms active
write-stall timeout and a 10 ms sampling tick. Existing hard frame deadlines
remain in force and can close a connection sooner.

At most eight whole queued entries are pruned per pass. Outstanding reservations
remain charged, even when W shrinks. Payload ownership is dropped before its queue
credit returns; partial writes and writer batches are never evicted. Per-subscriber
state cannot alter another subscriber's window, rights or dependency generation.
Arena capacity is unchanged; external queue/controller/timer metadata is reported.

For two streams and four subscribers each at Q32, both revisions reserve 287,648
arena bytes with 261,560 bytes of configured backing. The local queue storage
model grows from 26,112 to 39,040 bytes, including the enlarged entry/controller
representation even with adaptive mode disabled. Adaptive mode adds a 256-byte
model for eight Interval values. Allocator rounding, runtime tasks and internal
timer/socket storage remain outside these models. This is an external metadata
tradeoff, not free memory or an RSS bound.

Independent frames retain the original wire format. Explicit keyframe mode uses
version 2, with per-stream sequence numbers and source-supplied recovery markers.
A gap invalidates old reservations and drains dependent queued work before a new
keyframe can commit. Already-owned writer batches may finish. A stale reservation
can release itself but cannot invalidate a newer recovered generation. The source
must supply actual independently decodable recovery points; the adapter does not
parse codecs or infer dependencies from frame sizes.

## Source and tools

The checkout is based on `92284eac2588119a7b9b06f27e3b0b7bd57c9bd4`, retaining the
previous local harness and AP-10 work. The ordinary-path baseline is the preserved
AP-10 binary, not an unmodified upstream checkout.

- Stable Rust 1.96.0, Linux x86-64; safety tools use nightly-2026-10-03.
- Features: `local-branches-spike,bytes-interop` for the measured binaries.
- Release overrides: `profile.release.debug=1`, `profile.release.strip="none"`.
- Baseline probe SHA-256: `fb45f1dc87948b8d9225259c8e494046791a22911857fd4347d85019663f25f5`.
- Candidate probe SHA-256: `2cafd2956264316b474d904d410b10ede2b943e981dc6484881179e421817080`.
- Candidate build-record source SHA-256: `9b316c8c001a5090e0ef3f6c1a442ac639092b87b473ce2fec63349fa508a764`.
- Verification source SHA-256 (including contract, registry and harness): `087d9e7743cd499c14a2661921978fb243aab5a4b83cf40bcc70b105161b81d4`.

Local evidence is under `verification/artifacts/adaptive-window/`: qualified test
records, build identities, mutation controls, performance commands and per-consumer
outcomes. These ignored artifacts are not public download links. The mutation tree
is separate from the checkout and retains the deliberately broken source.

## Verification and measurement scope

The AP-11 tests cover bounded pruning, credit conservation through unwind, logical
window contraction with outstanding permits, source gaps, stale commits, recovery
timeouts, hard deadline precedence, partial writes, idle versus stalled output,
wire validation and option compatibility. An independent live-rights oracle runs
1024 steps for capacities 1, 2 and 32 natively, and 64 steps under Miri.

The seven native feature configurations passed 1506 executions, including
doctests and the reference-configuration check, with fmt, clippy and remote-authority
checks. ASan and TSan each passed the 14 AP-11 scenarios plus their instrumentation
configuration test. TSan uses the existing accepted final Acquire-load variant;
production atomics are unchanged. Python harness tests passed all 70 tests in
ordinary and optimized modes, including process checks against the candidate.
Miri passed 56 executions: all 14 scenarios with strict provenance under the
default borrow model and explicit Tree Borrows, each with seeds 1 and 17. Exact
MIRIFLAGS are retained in the qualified command records; see the
[Miri options](https://github.com/rust-lang/miri#available-flags) for their scope.
These focused adapter/storage checks are not new Loom models or a proof of
exhaustive coverage. The allocator protocol did not change; core Loom was not
rerun for this adapter-only change.

The four final TCP smoke cases (independent/keyframe and scalar/batch fan-out)
passed payload, ownership-flow and teardown validation. An earlier keyframe smoke
failed because the common CLI whitelist omitted the new option. That failure is
retained under `smoke/`; the whitelist and full CLI regression were fixed before
the qualified checks and final `smoke-final/` campaign.

A directed negative control removes the captured-epoch check from reservation
rollback in a disposable tree. The named stale-permit detector passes unmodified
and fails the mutation: an old reservation otherwise increments the recovery
generation after a newer keyframe has committed. This control checks that specific
postcondition, not the completeness of the whole suite.

The prepared allocation probe uses actual Mosaic buffers and a noop waker. Its
10,000 cycles enqueue 30,000 frames, evict 10,000 and transfer 20,000 to bounded
batch ownership, with zero allocations, reallocations or frees in the measured
region. Its positive control sees an allocation/free of 1234 bytes. This excludes
runtime/socket setup and does not prove allocation-free Tokio timer execution.

The host is a shared Hyper-V VM with five visible physical cores. Timed runs use
server CPUs 0/2, readers 4/6 and generator 8, after compilation and safety checks
finish. This is exploratory evidence, not the dedicated-host promotion gate.
Every policy keeps the same arena profile, queue capacity, deadline and overload
mode. Comparisons retain planned-delivery denominators, late deliveries, evictions,
rejections, post-disconnect gaps and failed runs. Socket completion is not receipt.
Keyframe mode additionally carries twelve metadata bytes per frame and an
eight-byte handshake prefix; its dependency contract and wire cost differ from
independent-frame modes. Results must be compared with that distinction retained.

## Ordinary-path comparison

The preserved AP-10 baseline and candidate ran ordinary remote Tokio Q8 profiles
with the same candidate load generator, alternating order across seven three-second
runs per load. This comparison does not enable the adaptive policy.

| Offered arrivals/s | Baseline / candidate delivered/planned | Baseline / candidate healthy p99, ms |
| --- | ---: | ---: |
| 1000 | 99.70% / 99.80% | 2.476 / 1.766 |
| 20000 | 95.84% / 95.61% | 2.157 / 2.439 |

Each latency value is the median of the run's worst subscriber p99, not a merged
histogram. At saturation this increases by approximately 13.1%, exceeding the 5%
ordinary-path threshold. Delivery changes by -0.24 percentage points. The shared
VM and wide run-to-run variation prevent attribution to a particular instruction,
but the regression signal remains an open gate; these results do not justify
promotion. The ordinary queue-item model also grows from 5120 to 6656 bytes due
to the extended header representation, even though version 1 sends the old header.

## Adaptive-policy comparison

The campaign finished on October 19: 168 policy runs, 28 ordinary-path comparisons
and four final smoke runs passed the harness's flow, payload and teardown checks.
Passing these checks allows policy losses and disconnects; it does not mean that
every offered frame was delivered. All issuer roots confirmed drain and destruction.

Each policy/profile pair has seven interleaved repetitions, reversing execution
order on alternate repetitions. Packet profiles offer 20,000 frames/s across two
streams with four subscribers each, local Q32, Reserved/Skip, batches bounded by
eight and a 100 ms hard deadline. The profiles are healthy (3 s), constant slow
reader (6 s), 100 ms pause (6 s), one-second stop/resume (6 s), and ramping slowdown
(8 s). Only stream 0/subscriber 0 is slowed; slow profiles request a 4096-byte send
buffer, with actual socket sizes retained in each record. Pause/stop begin at
second 2; ramp reaches full slowdown by second 5.

The mixed profile instead offers 200 frames/s for 3 s with the existing mixed-frame
distribution, a 136,314,880-byte domain limit and payload burst parameter 4. Its
larger resource profile is identical across policies, not identical to the packet
arena. No mixed-profile reader is deliberately slowed. Keyframe mode explicitly
marks every twentieth source frame as a recovery point in both distributions.

`FIFO` means freshness off; `Oldest` replaces whole queued frames with minimum age
zero; `Adaptive` uses independent frames; `Keyframe` adds dependency recovery.
Delivery percentages include all planned recipient frames, even after disconnect.
Healthy p99 is the median of each run's worst healthy subscriber p99. Evictions are
median recipient-frame counts per run, already included in undelivered work.

| Profile | Policy | Delivered/planned | Timely/planned | Healthy delivered/planned | Healthy p99, ms | Evictions/run |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Healthy | FIFO | 99.03% | 99.03% | 99.03% | 1.779 | 0 |
| Healthy | Oldest | 98.73% | 98.73% | 98.73% | 2.122 | 388 |
| Healthy | Adaptive | 98.69% | 98.69% | 98.69% | 1.470 | 68 |
| Healthy | Keyframe | 94.75% | 94.75% | 94.75% | 1.254 | 780 |
| Constant | FIFO | 85.50% | 85.49% | 97.70% | 1.676 | 0 |
| Constant | Oldest | 85.19% | 85.16% | 97.32% | 2.105 | 12049 |
| Constant | Adaptive | 85.65% | 85.62% | 97.85% | 1.888 | 7662 |
| Constant | Keyframe | 82.71% | 82.69% | 94.49% | 1.002 | 5436 |
| Pause | FIFO | 98.04% | 98.04% | 98.10% | 1.518 | 0 |
| Pause | Oldest | 98.09% | 98.09% | 98.21% | 1.505 | 2086 |
| Pause | Adaptive | 97.41% | 97.41% | 97.52% | 1.157 | 6515 |
| Pause | Keyframe | 94.72% | 94.72% | 94.76% | 1.180 | 3384 |
| Stop/resume | FIFO | 90.13% | 89.88% | 97.99% | 1.781 | 0 |
| Stop/resume | Oldest | 95.77% | 95.44% | 97.52% | 2.204 | 13218 |
| Stop/resume | Adaptive | 96.06% | 95.55% | 97.67% | 1.610 | 12647 |
| Stop/resume | Keyframe | 92.81% | 92.53% | 94.59% | 1.010 | 7612 |
| Ramp | FIFO | 86.07% | 86.05% | 98.18% | 1.152 | 0 |
| Ramp | Oldest | 86.07% | 86.05% | 98.21% | 1.320 | 22499 |
| Ramp | Adaptive | 86.32% | 86.16% | 98.39% | 1.418 | 19518 |
| Ramp | Keyframe | 82.55% | 82.39% | 94.05% | 0.887 | 7434 |
| Mixed | FIFO | 100.00% | 100.00% | 100.00% | 4.665 | 0 |
| Mixed | Oldest | 100.00% | 100.00% | 100.00% | 4.694 | 0 |
| Mixed | Adaptive | 100.00% | 100.00% | 100.00% | 4.547 | 0 |
| Mixed | Keyframe | 100.00% | 100.00% | 100.00% | 4.489 | 0 |

Stop/resume is the clearest candidate improvement: Adaptive gains 5.93 percentage
points in median delivery and 5.67 points in timely delivery over FIFO at unchanged
arena capacity. It also exceeds Oldest's delivery by 0.30 points in this sample.
It does not win every profile: FIFO delivers more in healthy and short-pause runs,
and the small constant/ramp differences are insufficient evidence of a general
throughput improvement. Mixed profiles all deliver every planned frame.

The following stop/resume ranges retain variation rather than treating the median
as a guarantee. Delivery gaps include startup and the remaining measurement window
after disconnect; latency histograms alone exclude work never delivered.

| Policy | Delivered/planned range | Lowest healthy delivery | Largest healthy p99, ms | Median worst-recipient delivery gap, ms |
| --- | ---: | ---: | ---: | ---: |
| FIFO | 88.03%..90.79% | 96.12% | 2.505 | 2997.0 |
| Oldest | 83.98%..97.39% | 87.33% | 2.781 | 1000.2 |
| Adaptive | 95.18%..97.29% | 96.96% | 2.071 | 1000.1 |
| Keyframe | 90.78%..94.38% | 92.67% | 1.711 | 1000.1 |

Every observed healthy-subscriber p99 in the 168 profiles is below 5 ms. This does
not imply uninterrupted service: worst healthy delivery gaps reach about 48 ms
in slow-reader profiles. Healthy delivery can also fall substantially below its
median. Generator missed arrivals remain in planned denominators; packet-profile
median missed-arrival counts range from 573 to 2059 per run. Consequently these
numbers measure the full offered-load pipeline, not isolated allocator speed.

## Losses, disconnects and remaining gates

Adaptive and Keyframe each close the deliberately slow subscriber in all seven
constant and all seven ramp runs. Of these 28 closes, 24 are existing hard frame
deadlines and four are sustained-pressure decisions (one Adaptive, three Keyframe).
No healthy subscriber has an adaptive disconnect reason. Neither policy closes
during the 100 ms pause or one-second stop/resume cases. The TCP campaign therefore
does not independently validate the write-stall or keyframe-wait timeout; focused
deterministic tests cover those transitions.

Keyframe mode lowers latency partly by delivering fewer frames. Source gaps and
recipient losses require dropping dependent work until recovery, including when
the generator misses a scheduled arrival. Across the seven healthy Keyframe runs,
61,804 recipient frames are dependency skips. Across stop/resume runs the total
is 156,883. This is a different delivery contract, not an independent-frame speedup.

Eviction is irreversible even if the replacement fails. Summed across seven
stop/resume runs, Adaptive records 27 failed replacement attempts, Keyframe 8216;
these attempts are subsets of eviction events, not additional lost old frames.
Late deliveries also remain counted: FIFO, Oldest, Adaptive and Keyframe record
7512, 13905, 11301 and 9539 respectively across those runs. Successful local pruning
cannot retract bytes already accepted by TCP. All received complete frames pass
payload/metadata checks, and no completed writer frame is unobserved by consumers
at campaign drain.

Terminal transport outcomes remain visible: ten Oldest slow-reader connections and
one Adaptive ramp connection end with a truncated payload; all are the deliberately
slow subscriber. Closing mid-frame is permitted, but that frame is never reported
as delivered. The flow report also retains `unclassified_uncompleted_enqueued`:
in slow profiles its per-run residual is 0..8 for FIFO/Oldest and 0..1 for adaptive
modes. The queue conservation equation places this residual in transferred work
that did not complete its write; the aggregate does not identify the individual
writer-abort cause. Queue cancellation/eviction counters do not absorb it. Exact
per-cause writer-batch loss attribution remains an observability improvement.

This implementation remains opt-in. Before considering default adoption, isolate
the enlarged header/controller metadata from the ordinary path, repeat its open
13.1% p99 regression on a dedicated host, and investigate short-pause over-pruning.
Keep the same offered load, socket limits, arena and policy semantics when comparing
changes. The passing safety checks, fixed arena and improved stop/resume result do
not close the ordinary-path gate or establish production latency guarantees.
