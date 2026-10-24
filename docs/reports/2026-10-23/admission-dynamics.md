# Admission dynamics at fixed capacity

The AP-16 experiment adds explicit issuer-side admission pacing and a reproducible
comparison with greedy Mosaic and Bytes. It changes examples and verification;
the allocator protocol and ordinary adapter defaults remain unchanged.

## Implementation and invariants

The [research guide](../../admission-dynamics.md) describes the CLI and measurement
boundaries. [AP-16](../../../CONTRIBUTING.md#ap-16-experimental-admission-pacing)
is the normative contract.

| Boundary | Executable requirement |
| --- | --- |
| Rate credit | Charge rounded class capacity once per successful backing acquisition; every interval obeys `successful_bytes <= burst + rate * elapsed`. Retain fractions and cap idle credit. |
| Fixed-window control | `tick-cap` limits each window; adjacent windows can produce two bursts. It does not claim the bucket's arbitrary-interval bound. |
| Pending input | One header per stream, at most 64; no payload or queue permit is retained while waiting. Generation checks prevent stale cancellation. |
| Attempt | Queue permits precede one synchronous policy/core attempt. Error or unwind refunds provisional rate credit; successful acquisition consumes it even if subsequent I/O fails. |
| Scheduling | One root-local reusable timer, FIFO, at most eight signals and at most 64 records examined per pass. A delayed commit cannot bank a second burst. |
| Deadline | Header receipt establishes the absolute deadline for wait, input and output. A ready I/O future cannot bypass expiry. Scheduled-arrival latency also exposes upstream waiting. |
| Delivery accounting | Only complete consumed/drained payloads commit received-frame and recipient-loss counters. Incomplete input and unread expiry remain separate observations. |
| Reclamation | Controller credits cannot authorize ownership or reclaim storage. Core publication, charge refund, generation and teardown remain authoritative. |

The implementation is safe Rust under `forbid(unsafe_code)`. The controller is
issuer-local; it introduces no new atomic or remote-Drop operation. Prepared typed
clone scratch supports scalar/batch fan-out to 100 consumers without changing
branch limits. The slow research reader meters headers and payload at an upper
rate of 1024 bytes/s with a 64-byte burst; timer scheduling can make it slower.

The allocation probe measured 100,000 prepared Engine cycles: 50,000 commits and
50,000 rollbacks, with zero allocations, reallocations or frees. A deliberate
allocation/free positive control passed. This result excludes Tokio timers,
sockets, runtime tasks and measurement histograms.

## Evidence identities and method

| Item | Value |
| --- | --- |
| Starting Git HEAD | `92284eac2588119a7b9b06f27e3b0b7bd57c9bd4` plus the preserved working tree |
| Calibration source | `d1d0df7f136eaecab4cd3953f036a0726082e2ceb5f4a727d27b59fc278d002b` |
| Final source | `90753c9dfa6818b8efd904f0b7b95905aea386645159e04038c6bb3e8a6664c1` |
| Final `spike_probe` SHA-256 | `f7ea0fd1caa4123306e2f2d3fe3e67c7468c6750501e57c187e9228e3a2b3426` |
| Release compiler | `rustc 1.96.0 (ac68faa20 2026-05-25)`, x86-64 Linux |
| Release features | `local-branches-spike,bytes-interop` |
| Miri/sanitizers | `nightly-2026-10-03`, rustc `1.101.0-nightly (0abfedbc7 2026-10-02)` |
| Host | Shared host, five distinct reported physical cores; no dedicated-host qualification |
| TCP affinity | Issuer/returner `0,2`; readers `4,6`; generator `8` |
| Scratch/builds/raw evidence | `/tmp/abps-admission-20261023`, tmpfs, swap disabled |

Source hashing includes the actual dirty source tree, scripts, verification corpus,
manifest, lockfile, build script and canonical contracts. Calibration and final
results are separate revisions: the latter fixes partial-input diagnostic
accounting. Parameters were not retuned after that fix. Raw files remain local;
the adjacent [compact metrics record](admission-dynamics-metrics.json) carries
identities, outcomes and artifact
hashes. No build, sanitizer or profiling process ran during confirmation timing.

TCP uses one issuer/root, one remote writer worker, one producer, 100 consumers,
Q=8, writer batch=8, Skip, a 100 ms header deadline and a requested 4096-byte send
buffer. The main Mosaic profile has 32 slots of 1500 bytes and a 48,000-byte domain
limit: 51,344 bytes including arena metadata. Pacing adds 168 modeled external
bytes (253,804 to 253,972; approximately 0.066%), with zero arena growth.
Histograms, runtime/socket storage and allocator rounding are outside that model;
RSS is a separate observation. Bytes has equivalent logical quotas, not the same
physical reservation.

Calibration used 3-second healthy synchronized runs at 500/1000/2000/4000 fps.
1000 fps was the highest sampled rate with complete timely delivery for both
greedy baselines. A nine-point screen tested 0.9/1.0/1.1 times its 1.5 MB/s rounded
charge rate and bursts of 1/4/8 slots. The least disruptive screened setting,
1.65 MB/s with an eight-slot burst, was frozen before confirmation. This is a
conservative calibration choice, not a measured maximum sustainable rate.

## What the experiments show

### Correlated retention matters; fewer core errors need not mean improvement

Logical replay and native returns use 3000 offered events, 3100 fps, eight domains,
32 shared slots and matched offered-byte/service-time hashes. `spread` permutes
service requirements within 31-event cohorts. Actual release dates depend on
successful admission; these are not externally forced periodic release barriers.

The deterministic replay produced:

| Service pattern / admission | Accepted | Core refusals | Pending-header loss |
| --- | ---: | ---: | ---: |
| Synchronized / off | 1560 | 1440 | 0 |
| Spread / off | 2001 | 999 | 0 |
| Synchronized / bucket | 1560 | 1056 | 384 |
| Spread / bucket | 1940 | 498 | 562 |
| Synchronized / tick-cap | 1560 | 960 | 480 |
| Spread / tick-cap | 1927 | 697 | 376 |

Bucket/tick settings here are 3 MB/s and four slots; they are independent of the
TCP selection. Spreading retention improved unpaced acceptance by 28.27%. Pacing
the synchronized workload did not improve acceptance: apparent refusal reduction
became pending-header loss. After a one-cohort impulse, off accepted 2970 frames,
bucket 1938 and tick-cap 1934. This does not support a general stability or goodput
benefit from a fixed cap, nor prove self-sustained oscillation in ABPS.

A further nine-point logical screen used 2.16/2.40/2.64 MB/s and bursts of
1/4/8 slots. Acceptance ranged from 1400 to 1560, never above off's 1560.
The lowest-rate one-slot setting removed every core refusal while losing 1600
offered headers: another direct negative control against optimizing refusal count
alone. Three native off/bucket pairs for the selected tied maximum gave
1561/1565, 1559/1559 and 1559/1560 accepted frames. This exploratory selection
reuses the trace and does not constitute an independent improvement result.

Native campaigns use actual final Drops on one and four remote workers and retain
grant identities, Drop intervals and observed occupancy. Eight-returner cases are
explicitly unsupported on this five-core host. All completed cases reconcile
offered outcomes, final charge and physical slots. Their per-event snapshots and
timestamps are invasive; their timings are mechanism evidence, not allocator
throughput claims. Observed occupancy ranges are not coherent transition counts.

### One-slot pacing bursts are unsuitable at this timer scale

At 1000 fps, the three one-slot-burst screens delivered only about 3.1–3.3% of
offered recipient frames within the source deadline. The observed millisecond
waits and the one-slot accumulation ceiling are consistent with a timer-scale
catch-up limit. Raising rate
by 10% did not cure it. Four/eight-slot bursts at the nominal rate restored timely
delivery, with additional header-to-grant tails. This is not a reason to insert
spinning or hidden retries into the core.

The header deadline bounds adapter custody, not time already spent in an upstream
socket/queue before that header is read. Pacing can move the backlog upstream and
admit an already old frame under a fresh header deadline. Source-to-delivery latency
and late-delivery counts expose this effect; bounded pending-header storage alone
does not ensure freshness from the producer's original arrival.

A separate five-second slow-consumer screen at 8/16/32 slots retained the same
physical capacity within each pair. At eight slots, off/bucket produced 96/95
`BudgetExceeded` refusals in one long sequence. At 16 and 32 slots there were no
core capacity errors. The slow consumer ultimately leaves under the existing
deadline policy; pacing neither revokes its held ownership nor accelerates Drop.

### Repeated TCP confirmation

Seven matched 60-second triples completed, rotating Bytes/off/bucket order.
All 21 runs passed payload, offered-loss and teardown checks. The table contains
medians of run summaries, not pooled latency samples:

| Variant | Timely delivery / offered | Header→grant p50 | Header→grant p99.9 | Delivery p99.9 | Sampled server CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Bytes | 98.9868% | 1.563 µs | 16.463 µs | 5.476 ms | 12.14 s |
| Mosaic off | 98.9821% | 1.483 µs | 26.159 µs | 5.427 ms | 12.58 s |
| Mosaic bucket | 98.9868% | 1.613 µs | 15.727 µs | 4.927 ms | 12.18 s |

Use paired changes for the decision. The median bucket-minus-off timely change
was **0 percentage points**. The median paired header→grant p99.9 change was
**+4.05%**, delivery p99.9 **−3.64%**, and sampled CPU **−3.87%**. Taking the ratio
of the two p99.9 medians above would misleadingly suggest a large grant-tail win:
the per-pair changes range from −52.47% to +35.20%. A descriptive paired bootstrap
(seed 42, 10,000 resamples) spans −43.91% to +19.63% for that tail and −0.0264 to
+0.0315 percentage points for timely delivery. Seven shared-host pairs do not
provide dedicated-host statistical qualification.

Both Mosaic variants had zero core refusals in every main run, so there is no
resource-error streak improvement to establish. The timely fraction's approximate
99% ceiling includes the slow recipient, rather than excluding it from offered
work. Healthy controls completed three 5-second triples per arrival pattern:
median timely delivery off/bucket was 99.54%/99.66% for steady arrivals and
100%/100% for synchronized arrivals. These short controls do not establish a
healthy-load CPU or tail guarantee. The ≥20% tail-improvement requirement and
shorter resource-error streak requirement are not met; no default promotion follows.

Admission p99.99 is not established: each 60-second run offers only 60,000 backing
acquisitions. Aggregate recipient samples are correlated by fan-out and cannot
establish a per-stream or per-consumer p99.99. Slow-reader late delivery, writer
expiry, queued cancellation and unread completed writes remain losses in the
offered denominator. A residual unclassified queued-cancellation count is retained,
not relabeled as an allocator error.

### Profiling and compatibility

`perf 6.12.113` and its dependencies were unpacked into tmpfs. Hardware cycles,
instructions and cache-misses report **not supported**; this run establishes no
hardware RMW or cache-to-cache result. Separate five-second software-event runs
measured 982.45/984.15 ms task-clock for off/bucket, 4033/4316 context switches,
953/948 minor faults and zero major faults. These are single profiled runs including
setup/teardown, not a significant CPU improvement or a hot-path fault guarantee.

A single-slot native trace brackets publication-to-next-successful-grant observation
between the measured final-Drop endpoints. The interval includes the next offered
arrival and scheduler delay; it is not an exact publication timestamp or a tight
reusability-latency guarantee. Both AP-14 `adaptive-shared` integration runs drained
and passed the existing memory-bank oracle at unchanged settings. Their three-second
delivery variation is compatibility evidence, not a combined-policy win.

## Verification and corrections

| Final-revision check | Result / scope |
| --- | --- |
| Native library, integration tests and examples | 510 executions, full local-branches/bytes/serial feature configuration |
| AP-16 with Berry-only features | 18 executions |
| Doctests | 23 executions, including compile-fail contracts |
| Miri | 18 AP-16 executions, strict provenance + Tree Borrows, frozen final source |
| ASan / TSan | 18 AP-16 executions each plus instrumentation selection; TSan uses the accepted Acquire-load variant |
| Production Mosaic Loom | 22 models with fence and 22 with Acquire load; smoke compositions have preemption bound 2, small models no preemption bound, 10,000 branches, no permutation/duration cap |
| Clippy / formatting | Passed with warnings denied / format and whitespace checks |
| Python harness | 80 selected tests in each ordinary and optimized final run, including real-process controls |

Executions across targets/features are not counts of independent logical scenarios.
Miri adapter storage fixtures use Berry; native TCP and native/sanitized returns
exercise Mosaic. Loom covers the production atomic protocol, not the unsafe storage
layer or Tokio scheduling. These checks are not a full production verification matrix.

Three directed Engine mutations were detected in disposable trees: omitted debit,
omitted refund and FIFO bypass. Each named detector passed on its unmodified source
first. They cover the unchanged controller revision, separately from the final TCP
accounting fix. Python negative controls reject rate-bound violations, missing or
duplicate final Drops, lost offered outcomes and sequential spurious contention.

Failures were retained rather than replaced by later successes:

- The first 100-recipient test exposed a pre-existing 20-entry common clone scratch
  limit. The research adapter now prepares backend-typed scratch; batch cloning still
  makes one backend reservation rather than silently becoming scalar.
- A scripted Miri test exceeded a real 100 ms deadline. Its deterministic I/O test
  now uses paused virtual time; the runtime deadline and assertions were preserved.
- Complete-frame accounting regressions failed for unread expiry, partial payload
  and partial discard. Deferring preflight delivery counters to the completed-frame
  boundary fixed all three. The first confirmation campaign was stopped and kept
  separate when this defect was found.
- Initial sanitizer qualification lacked compiler metadata; one process-harness
  check lacked its companion binary. Both were corrected and rerun. An empty Loom
  feature selection was rejected as evidence; the required feature-complete models
  were then executed. None of those setup attempts is counted as a passing check.

## Reproduction and remaining limits

Use the frozen-source/build-record procedure in the research guide. The main run is:

```sh
python3 scripts/admission-dynamics.py --suite=tcp --seconds=60 --repeats=7 \
  --patterns synchronized --clients slow --rate=1000 \
  --pacing-rate=1650000 --burst=12000 \
  --build-record=/tmp/lab/build.json --output=/tmp/lab/confirmation
```

Healthy controls use `--seconds=5 --repeats=3 --patterns steady synchronized
--clients healthy`. The full two-pattern/two-client 7×60-second cross-product is
not claimed. Eight or more native returners, dedicated-host tails, cgroup-isolated
RSS, hardware PMU, and Telemt integration remain outside this evidence.

Defaults remain off. The relevant next hypothesis is correlation-aware scheduling
that preserves useful work, not a generic smaller rate. Any follow-up must retain
the same arena, preserve ownership/deadlines, include every loss and demonstrate a
matched end-to-end benefit before promotion.
