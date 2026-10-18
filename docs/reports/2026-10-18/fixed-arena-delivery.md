# Fixed-arena delivery experiment

This report covers the AP-10 adapter changes over the existing performance-harness
correction. It is exploratory Linux x86-64 evidence from a shared Hyper-V VM with
five visible physical cores. It is not the dedicated-host AP-09 promotion gate.
The allocator implementation, Cargo manifests and synchronization are unchanged.

## Reproducible identity

The checkout starts at `92284eac2588119a7b9b06f27e3b0b7bd57c9bd4` with the existing
local harness corrections retained. The preserved baseline is that corrected
probe before the local-queue changes, not an unmodified upstream binary.

- Compiler: stable Rust 1.96.0; safety instrumentation: nightly-2026-10-03.
- Features: `local-branches-spike,bytes-interop`.
- Release build: `profile.release.debug=1`, `profile.release.strip="none"`.
- Baseline probe SHA-256: `4afb344bc389cb149de3a1df90193b840e46b1192a3c83ece04406c4f163ff8d`.
- Candidate probe SHA-256: `fb45f1dc87948b8d9225259c8e494046791a22911857fd4347d85019663f25f5`.
- Candidate build-record source SHA-256: `3adaed61e68d13782eacb1ba2d4366fe8be336c25b180843f3874f104cde3f7d`.

Local records are under
`verification/artifacts/fixed-arena-implementation-20261018/`, including build
records, preserved baseline, instrumented logs, command lines, resource plans,
per-consumer outcomes and failed-run status. They are ignored artifacts, not public
downloads. The [guide](../../fixed-arena-delivery.md) documents reproduction.

## Implementation and invariants

The fixed-arena packet profile preserves 130 slots in each of the 512/1500-byte
classes and a 97,500-byte per-domain limit as Q changes from 8 to 32 or 64.
It explicitly uses `capacity-check=report`; its envelope deficits remain visible.
The local-routing alternative uses two issuer roots with the same total backing,
adding only 1296 bytes of root metadata in this build.

The optional local queue is prepared, worker-local and safe Rust. AP-10 conserves
free, reserved, queued, transferring and releasing credits. Oldest eviction drops
the previous queued right before replacement admission; it cannot revoke a writer
batch or partial TCP frame. A failed replacement leaves the eviction counted.
FIFO survivors, framing, initialization and SI-06 deadlines remain enforced.

## Verification

The native feature matrix passed 1401 test/doctest executions, plus its reference
configuration test, fmt, clippy and remote-authority checks. ASan and TSan each
passed the eight AP-10 tests and their synchronization configuration test. TSan
uses the already accepted final Acquire-load variant; no production ordering was
changed. Miri passed all 32 executions: eight scenarios under Stacked Borrows and
Tree Borrows, each with strict provenance and seeds 1 and 17. These are focused
adapter/storage checks, not an exhaustive soundness proof or a rerun of core Loom.

Python harness tests passed in ordinary and optimized modes: 63 tests per mode,
with six process checks initially skipped; all six were then run successfully per
mode against the prebuilt candidate. The four TCP smoke combinations (scalar/batch
and FIFO/oldest) passed payload, flow and teardown checks.

The prepared allocation probe ran 10,000 cycles, publishing 30,000 queue entries,
evicting 10,000 and transferring 20,000 to bounded batch ownership. It measured
zero allocations, reallocations and frees in that region. Its external positive
control observed one allocation/free of 1234 bytes. The region uses a noop waker;
it excludes runtime and socket setup and does not claim allocation-free Tokio
task scheduling or TCP execution.

## Measurement protocol

Timed runs start only after the safety and build processes finish. Server CPUs are
0/2, validating readers 4/6 and pacer 8. Each configuration has seven interleaved
three-second offered-load windows; drain is measured separately. Packet workloads
use two streams, fan-out four, writer batch eight, Skip and a 100 ms deadline.
Slow-reader cases request a 4096-byte send buffer for every candidate and retain
the actual kernel value in each record. Only stream 0/subscriber 0 is delayed.

The preserved baseline and candidate additionally run equivalent ordinary Q8
profiles using the same candidate load generator. A separate mixed-frame campaign
uses a four-frame keyframe burst, 1000 arrivals/s, the same slot counts and a fixed
136,314,880-byte domain limit in every candidate. Those larger frame workloads are
compared only within their own capacity profile.

Results include planned and sent denominators, timely delivery, per-consumer tail
latency, evictions, failed replacements, budget refusals and confirmed drain.
Queue evictions remain included in uncompleted enqueued work. No result is called
lossless solely because a surviving subset has a low p99.

## Results

All 203 timed records passed payload, flow and teardown validation (147 profile,
28 before/after and 28 mixed-frame runs). No failed record was discarded. Every
row below summarizes seven runs. Delivery is validated complete frames divided by
planned recipient deliveries; sent-normalized delivery is shown separately.
The p99 column is the median of each run's worst healthy subscriber p99; its maximum
is the worst of those seven values, not a p100 or a merged latency histogram.

### Packet saturation, 20,000 arrivals/s

| Profile | Delivered/planned | Delivered/sent | Healthy p99 median / max, ms |
| --- | ---: | ---: | ---: |
| Remote Tokio Q8 | 91.35% | 94.78% | 3.262 / 3.871 |
| Remote Tokio Q32 | 96.05% | 99.49% | 3.150 / 4.215 |
| Remote Tokio Q64 | 96.67% | 100.00% | 3.133 / 4.317 |
| Local Tokio Q32 | 95.76% | 99.85% | 2.707 / 3.781 |
| Local FIFO Q32 | 95.42% | 99.78% | 2.939 / 3.729 |
| Local oldest 0 ms Q32 | 95.86% | 99.75% | 3.215 / 3.574 |
| Local oldest 2 ms Q32 | 96.52% | 99.86% | 2.556 / 3.932 |

Remote Q32 gains 4.70 percentage points of planned delivery over Q8 at the same
286,352-byte arena and 261,560-byte backing capacity. Its queue item model grows
from 5120 to 20,480 bytes. Local roots total 287,648 bytes; the prepared local
queue model totals 26,112 bytes at Q32 (excluding allocator rounding and runtime
objects). Local FIFO does not beat the comparable local Tokio queue here.

Local oldest 2 ms has the best Q32 median in this healthy workload, but only twelve
queue evictions occurred across its seven runs. Its small advantage over local
Tokio cannot be attributed to eviction: generator missed-arrival counts also vary.
At 1000 arrivals/s every profile delivers all source-sent frames, with no evictions;
planned delivery differs because the pacer misses some arrivals.

### Packet saturation with one slow reader

The healthy fraction excludes only stream 0/subscriber 0. Its planned denominator
comes from the actual per-stream planned class counts. Slow-reader losses and late
frames remain in overall delivery; they are not removed from the run.

| Profile | Overall delivered/planned | Healthy delivered/planned median / min | Healthy p99 median / max, ms | Median evictions/run |
| --- | ---: | ---: | ---: | ---: |
| Remote Tokio Q8 | 80.12% | 91.50% / 86.70% | 3.000 / 3.928 | 0 |
| Remote Tokio Q32 | 80.11% | 91.51% / 89.29% | 7.807 / 38.994 | 0 |
| Remote Tokio Q64 | 74.21% | 84.78% / 74.39% | 46.825 / 84.804 | 0 |
| Local Tokio Q32 | 82.09% | 93.78% / 91.99% | 3.785 / 6.091 | 0 |
| Local FIFO Q32 | 81.62% | 93.24% / 91.35% | 4.227 / 5.575 | 0 |
| Local oldest 0 ms Q32 | 81.73% | 93.32% / 89.41% | 3.176 / 3.791 | 9807 |
| Local oldest 2 ms Q32 | 81.68% | 93.26% / 75.55% | 3.185 / 4.309 | 9069 |

Remote Q64 incurs 6804 budget refusals across seven runs and is unsuitable for this
fixed-arena slow-reader workload. Q32 reduces that to eight, yet its worst healthy
p99 reaches 38.994 ms. Local placement improves isolation, but local Tokio and local
FIFO still exceed 5 ms in their worst runs. Both oldest policies stay below 5 ms
for every healthy subscriber p99 in these seven packet runs.

That latency result is not free delivery: oldest 0 ms evicts 67,157 recipient rights
and oldest 2 ms evicts 89,343 across the slow-reader series. The 2 ms variant has a
particularly bad run with 43,802 evictions and only 75.55% healthy planned delivery.
Its low p99 must not hide that loss. No failed replacement or budget refusal was
observed in the local variants; deterministic tests separately cover replacement
failure and cancellation. Slow-reader expired deliveries remain recorded: totals
are 1150 for oldest 0 ms and 1121 for oldest 2 ms. Do not promote either policy as
uniformly improving delivery or as an end-to-end deadline guarantee.

### Ordinary-path before/after

| Rate | Before planned delivery | After planned delivery | Before / after healthy p99 median, ms |
| --- | ---: | ---: | ---: |
| 1000/s | 99.10% | 99.37% | 2.126 / 2.357 |
| 20000/s | 94.11% | 94.56% | 2.103 / 1.968 |

The low-load p99 median increases by 10.9%, exceeding the ordinary-path 5%
non-regression threshold. The high-load median improves; low-load worst-run p99
also falls from 3.422 to 2.736 ms. This mixed VM evidence does not establish a
causal implementation regression, but it does not pass the promotion gate either.
Keep the unsuccessful threshold result and repeat on dedicated hardware before
promoting the change. Do not compare these medians with the earlier profile series
as though they had identical background scheduling.

### Mixed frames and keyframe bursts

| Profile | Delivered/planned | Healthy p99 median / max, ms |
| --- | ---: | ---: |
| Remote Q8 | 99.57% | 7.373 / 7.676 |
| Remote Q32 | 99.70% | 7.594 / 8.520 |
| Local FIFO Q32 | 99.50% | 6.676 / 7.086 |
| Local oldest 0 ms Q32 | 99.60% | 6.992 / 9.888 |

All source-sent frames are delivered in this workload; planned losses originate
at the generator. There are no evictions, budget refusals or expired deliveries.
Every candidate exceeds the 5 ms target. The remote arena is 281,174,224 bytes and
the local total 281,175,456 bytes, with the same payload capacity within this
separate large-frame profile. The packet result is not evidence of improved
large-frame delivery.

## Decision and remaining gate

Retain all new policies as explicit experiments. Q32 descriptor overbooking is a
useful fixed-arena candidate when consumers keep up. With slow consumers, evaluate
local placement and a whole-frame freshness policy against both loss and tail
requirements. The local FIFO queue has value as a prepared eviction mechanism;
these runs do not establish it as a faster replacement for Tokio.

Allocator defaults, ordinary queue defaults and strict envelope validation remain
unchanged. The dedicated-host non-regression gate is still open, including the
observed low-load p99 increase. Sustained slow-reader loss, codec dependencies and
large-frame latency also prevent a universal recommendation.
