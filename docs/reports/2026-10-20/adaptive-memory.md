# Adaptive memory, soft budgets and root generations

This report records the AP-13 adapter experiment. The [contract](../../../CONTRIBUTING.md#ap-13-experimental-adaptive-memory-and-root-generations)
and [usage guide](../../adaptive-memory.md) define its scope. The experiment
adds no unsafe code, atomics or remote-return work to the allocator. Core source,
manifests and dependencies are unchanged from the pre-experiment checkout.

## Decisions

Keep `memory-policy=off` and `elasticity=off` as defaults. Adaptive budgets and
multiple roots remain explicit research options. A smaller arena reservation
does not establish equal useful capacity, and a larger budget does not recover
ownership retained by a slow recipient.

The useful result so far is an executable policy boundary: logical hard limits
span every generation, soft contraction preserves existing debt, and reservation
refund requires confirmed physical teardown. Tests and a separate rights oracle
exercise these properties. Neither the model nor the native measurements establish
production readiness or a universal delivery improvement.

## Implemented experiment

- Fixed and adaptive soft budgets, explicit groups, weighted 8:3:1 or strict
  priority, and separate individual/group demand estimates.
- Offered full-slot bytes include policy and queue rejection. Existing admitted
  bytes remain charged through final return.
- A 100 ms controller update; 400 ms EWMA/deviation time constant; default
  20 ms demand horizon. Activation after idle can use only unassigned soft
  opportunity after deducting contraction debt.
- Original single-root topology or prepared 5%/10% small-root catalogs, with cold
  issuer preparation or paid standby. Full layout cost determines the size.
- At most 125% aggregate arena reservation, 105% selected external model,
  32 retained records, 64 runtime changes, and one change per second.
- Frame-boundary routing without reconnect, replay, payload migration or grant
  borrowing. Logical stream hard limits cover all generations.
- Bounded traces, explicit policy/core refusal counters, reservation-time,
  preparation/destruction cost, and complete end-to-end loss reconciliation.

The initial router chooses the next active root and makes one core attempt. It
does not search for a successful allocation across roots. A selected-root refusal
must not be described as proof of aggregate exhaustion. This deliberately simple
router is a candidate for further experiments.

## Measurement design

The native baseline has one Mosaic issuer, eight streams, eight subscribers per
stream, 64 queue credits, local queues/returns, scalar fan-out, Skip, adaptive
freshness, an eight-frame writer batch and a 100 ms deadline. The packet classes
are 512 and 1500 bytes with 128 slots each; logical hard limits are 128768 bytes.
The main offered rate is 8000 frames/s. A static equal-peak control uses 160 slots
per class.

Native screening interleaves seven repetitions of 15 seconds. Longer comparisons
use seven repetitions of 60 seconds. Source pacing, validating readers and server
run on disjoint physical-core selections: server 0, generator 8, readers 2/4/6.
This is a shared virtual machine, not a dedicated performance host. Timings are
exploratory; medians of run percentiles are not pooled distribution percentiles.

Synchronized/shifted arrivals preserve the per-stream frame counts and payload
sequence over complete four-frame cycles. Bursts also change the number of pacer
wakeups. Comparisons between arrival patterns therefore cannot attribute every
latency difference to memory correlation.

The independent retention replay uses actual Mosaic buffers and a collection
model of unique allocations and owners. It writes an eight-byte identity marker,
not the full native payload, and checks admitted charge after every event. Its
synthetic slow recipient is present in every stream; TCP's slow recipient is
stream 0, subscriber 0. These are different experiments. Replay results are not
socket-delivery measurements.

Raw records are kept locally under ignored evidence/artifact directories. They
include immutable source snapshots, compiler/build commands, binary hashes,
affinity, complete losses and failed attempts. The dated report is the public
summary; generated build caches are not evidence.

## Findings

### Idle activation was a real policy defect

The first adaptive version reduced a stream's target to zero after idle and then
waited for its next 100 ms update. Initial offers were rejected despite unused
soft opportunity. Admission now permits a bounded target increase from unassigned
opportunity only. It subtracts all contraction debt and cannot borrow another
group's assigned target or exceed a hard limit.

Seven interleaved three-second native pairs compared this change with the same
32-record representation:

| Metric, median of seven runs | Before | After |
| --- | ---: | ---: |
| Timely deliveries / offered recipient frames | 97.467% | 99.471% |
| Memory-policy rejected input frames | 544 | 0 |
| Scheduled-arrival p50 | 174.847 µs | 208.127 µs |
| Scheduled-arrival p99.9 | 5365.759 µs | 4100.095 µs |
| Peak arena reservation | 276368 B | 276368 B |

This fixes the adaptive prototype. It is not evidence that adaptation beats the
original policy-off baseline. The p50 movement also prevents presenting this as
an unconditional latency win.

### Nonelastic mode does not need 32 root records

The fixed single-root policy originally scanned a prepared 32-record table.
It now prepares one record when elasticity is disabled. Seven three-second
paired measurements showed timely delivery medians of 99.317% and 99.467%,
respectively; CPU per timely delivery was 2.842 and 2.880 µs. That CPU difference
does not establish a throughput improvement on this host. The structural benefit
is less metadata and fewer empty-record visits.

### Correlation depends on the observation interval

Offline analysis of the exact arrival schedule at 8000 frames/s finds mean
pairwise frame-count correlation 1.0 for synchronized bursts and approximately
-0.143 for shifted bursts in 1 ms windows. In 100 ms windows both have constant
counts, so Pearson correlation is undefined, not zero. The controller cannot infer
these short bursts from its 100 ms aggregate alone.

The artifact reports the interval, undefined pairs and window peak for every
pattern. These are arrival-count correlations; they are not measured covariance
of retained backings. A future correlation policy needs a timescale-sensitive
retention signal before it can claim to predict shared memory pressure.

### Small roots lose useful capacity before routing starts

These are checked layout figures, independent of replay timing:

| Unreserved topology | Initial arena bytes | Active slots per class | Standby slots per class | Additional external model |
| --- | ---: | ---: | ---: | ---: |
| One root, memory policy enabled | 276368 | 128 | 0 | 14216 B |
| Ten roots, 10% cold chunks | 258720 | 110 | 0 | 27856 B |
| Ten active plus three standby roots | 336336 | 110 | 33 | 27856 B |
| Twenty-one roots, 5% cold chunks | 273168 | 105 | 0 | 27856 B |

The 10% cold topology saves 6.39% of initial reservation but loses 14.06% of
payload slots. The 5% topology loses 17.97% of slots while saving only 1.16% of
reservation. Metadata duplication is a real cost, not unused growth headroom.

Reserved mode adds another loss. A monolithic root gives each of eight streams
16 slots per class. A small root has 11 slots per class; integer partitioning
gives each stream one and leaves three unassigned. Ten initial roots therefore
give each stream ten slots, with 30 unassigned slots per class. Even thirteen
roots give only thirteen slots per stream, below the original sixteen.

These rights cannot be borrowed or silently relabelled as available to a stream.
A sensible next catalog experiment is to preserve a large initial root and use
small roots only for additions, then compare that with class-specific catalogs.
Neither alternative is implemented or credited with a performance win here.

This also exposed a preflight bug: `require-envelope` validated only the original
root. The executable counterexample uses eight streams, eight subscribers,
queue 64, batch 8, two classes with 4616 slots, hard limit 865500 and Reserved
cold roots. The original plan covers 577 slots per stream/class; the selected
initial roots give 570. Preflight now validates the selected active topology,
including grant rounding. Both Reserved and Unreserved regressions reject the
deficit, accept sufficient selected capacity, and preserve explicit report mode.

### Ordinary TCP screening is not primarily memory-limited

Seven 15-second runs per cell, medians:

| Policy/topology, steady arrivals | Timely delivery | p50 | p99.9 | Server CPU / timely delivery | Peak arena |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off, original root | 99.367% | 69.439 µs | 5349.375 µs | 7.402 µs | 276368 B |
| Fixed, original root | 99.422% | 68.223 µs | 4804.607 µs | 7.365 µs | 276368 B |
| Adaptive, original root | 99.269% | 70.015 µs | 5402.623 µs | 7.549 µs | 276368 B |
| Adaptive, cold small roots | 99.342% | 69.375 µs | 5115.903 µs | 7.508 µs | 258720 B |
| Adaptive, warm small roots | 99.265% | 68.287 µs | 5517.311 µs | 7.419 µs | 336336 B |

These steady runs had no ingress admission rejections. Source pacing missed a
median 759 input frames in the off case and 877 in the adaptive case; each
represents eight planned recipient deliveries. It would be incorrect to attribute
those differences to allocator exhaustion.

With synchronized bursts and the slow-consumer pause, all five policies had
approximately 98.646% timely delivery and no root growth. The static equal-peak
control reserved 344976 B and still delivered approximately 98.646%. Warm standby
therefore paid for unused capacity in this profile. Queue eviction, cancelled
queued ownership, writer aborts, expired deliveries and missing subscribers remain
in the raw loss accounting.

The seven 60-second phase-change pairs also failed to establish an improvement:
off versus adaptive timely delivery was 98.024% versus 97.991%; p50 was
174.207 versus 181.759 µs; p99.9 was 4057.087 versus 4157.439 µs. CPU estimates
were 2.854 versus 2.886 µs per timely delivery. CPU figures use sampled process
ticks, include setup and are limited by sampling granularity.

### Under tighter capacity, policy rejection dominates

The separate pressure series uses 32 slots per class, 16000 frames/s,
synchronized bursts and a constant slow consumer: seven five-second repetitions.
It is a short saturation experiment, not a long-duration overload soak.

| Policy/topology | Timely delivery | High-priority delivery | Low-priority delivery | Policy-rejected input frames, median | Peak arena |
| --- | ---: | ---: | ---: | ---: | ---: |
| Off, original root | 98.085% | 95.598% | 99.515% | 0 | 70800 B |
| Fixed, original root | 91.181% | 95.389% | 72.225% | 5792 | 70800 B |
| Adaptive, original root | 91.140% | 95.253% | 72.625% | 5775 | 70800 B |
| Adaptive, cold small roots | 75.063% | 94.890% | 52.830% | 18387 | 79296 B |
| Adaptive, warm small roots | 75.155% | 94.887% | 52.940% | 18372 | 85904 B |

Across all seven off runs there were 1832 `ClassDepleted` input refusals.
Single-root adaptive had zero such refusals, but 43041 memory-policy refusals.
Reducing allocator failures therefore did not mean reducing delivery loss.
Lower-priority delivery fell without a high-priority improvement.

Cold and warm small roots grew by a median two roots, yet delivery remained
substantially worse. The combination of smaller useful capacity, one-root routing
and nonborrowable soft assignments is unsuitable as a default for this workload.
Warm reserve paid more without a meaningful benefit.

The next policy experiment should distinguish a soft scheduling preference from
an unconditional admission ceiling. Borrowing *soft opportunity* under low
physical pressure would require an explicit contract and debt rules; it must not
borrow physical Reserved grants, move existing charges or bypass logical hard
limits. The current AP-13 policy intentionally does not perform that borrowing.

### Profiles point at queue/runtime work, with limited resolution

Separate six-second software `cpu-clock:u` profiles collected 129/144/151 samples
for off/adaptive/cold, with zero reported lost samples. Approximately 19–21% of
samples landed in `__vdso_clock_gettime`; queue maintenance, future polling,
writer work and scheduler paths also appear prominently. These small samples
suggest investigating redundant clock/maintenance calls. They do not quantify a
possible speedup or justify removing deadline checks before write attempts.

Hardware `cycles`, `instructions` and `cache-misses` reported `<not supported>` on
this VM. No cache-coherence or PMU conclusion follows from this campaign.
Separate `strace -f -c` runs retained syscall/error counts, but instrumentation
reduced delivery to approximately 14–17%. Those timings are excluded from native
tables. In particular, tracing-time futex totals are not allocator lock costs.

### Corrected retention replay

The corrected source completed 252 Unreserved and 56 Reserved replay runs,
covering steady, synchronized, shifted and phase-change arrivals, grouping,
priority, headroom and chunk alternatives. The following medians use synchronized
arrivals at 2000 frames/s for 15 seconds, seven seeds per cell:

| Replay policy | Timely recipient events | High-priority events | Peak arena |
| --- | ---: | ---: | ---: |
| Unreserved fixed | 44.890% | 51.120% | 276368 B |
| Unreserved adaptive | 44.960% | 45.450% | 276368 B |
| Unreserved group estimator | 44.907% | 45.427% | 276368 B |
| Unreserved cold | 43.579% | 44.061% | 284592 B |
| Unreserved warm | 43.579% | 44.061% | 336336 B |
| Unreserved 5% chunks | 42.623% | 43.084% | 286176 B |
| Reserved fixed | 42.580% | 44.960% | 283088 B |
| Reserved adaptive | 44.995% | 44.890% | 283088 B |
| Reserved cold | 41.800% | 41.797% | 351312 B |
| Reserved warm | 41.800% | 41.797% | 351312 B |

Warm and cold had the same logical delivery outcome in these comparisons. Warm
Unreserved consumed about 19.5% more reservation-time. This is a synthetic
retention result; preparation durations are still real execution observations,
and small initial preparation-time contributions to the integral can vary.
Logical outcomes were separately replayed twice and matched.

A further 1,800,000-event run represents 30 minutes of **logical** cyclic time at
1000 frames/s. The rights oracle and flow reconciliation passed, peak reservation
was 310464 B and confirmed shutdown returned reservation to zero. It grew twice
and did not shrink. This is not a 30-minute native TCP soak, nor evidence of
repeated grow/shrink convergence. The measured overload phases did not provide
the five-second low-occupancy interval needed for contraction.

## Verification and interpretation

The invariant tests cover cross-generation hard limits, shared-view retention,
soft debt, rollback and panic during preparation, shutdown/abandonment, generation
exhaustion, bounded records/attempts, remote final return, controller allocation and
trace overflow. A separate rights oracle generates acquisition/clone/drop/recycle
histories. A mutation that ignores existing admitted generations is detected by
the named hard-limit test.

The allocation probe requires a nonempty, successful acquire/freeze/clone/drop
workload. Zero successful work cannot pass as an allocation-free result. It
includes steady-state maintenance but excludes explicit cold root changes.

Three observation/validation defects were found during the campaign:

1. Replay initially used an epoch sampled before Bank preparation. A slightly
   later Bank epoch shifted the first update and introduced wall-clock influence.
   Replay now uses the controller's epoch. Root transitions also integrate
   reservation at their supplied logical timestamp, rather than mixing a replay
   clock with wall time. Earlier replay artifacts remain preliminary and are
   replaced by corrected runs.
2. Consumer validation calculated the schedule before rejecting an out-of-range
   frame identity. `u64::MAX` at rate 1 could overflow duration conversion.
   Identity rejection now precedes schedule arithmetic; a regression supplies
   that exact malformed header.
3. A growth trace combined post-transition reservation with the pre-transition
   active-root count. Samples now count active records after the transition.
   A controlled test checks both the growth sample and the reservation integral
   across the exact transition boundary.

One native strict-priority repetition overlapped a 2.2-second offline analysis
process. Preserve that original record, exclude it from the comparison, and use
the separately recorded replacement. Do not silently overwrite the evidence.

### Executed checks and source identities

The main native timing snapshot has verification source SHA-256
`a229e78bdadb9bd6d4af6c290add82ff63cb06b2b951451d441aafd4ca4a451e`;
its `spike_probe` binary is
`ab8b728942495b72f515817e69512bc8481022910f5379d6b64011fd949865ce`.
It passed 1588 native test executions, 77 doctests, 76 AP-13 Miri executions and
19 executions each for ASan/TSan, including each sanitizer configuration check.
These results apply to that snapshot, not automatically to later fixes.

The delivered runtime/test source has verification SHA-256
`63763d14cc544d27c5d46bb407d582d97959a2153d3b864b840947b796d5c5d3`;
its `spike_probe` binary is
`2503b9ea12239cdaaff313da288ec5c41d7951cb51bcbe4ad3a2119ea8e77b08`.
It adds the replay/transition clocks, post-transition trace count, selected
envelope preflight, malformed-identity guard and stricter evidence/allocation
controls. The final report prose was completed afterward.

| Delivered-source check | Actual result |
| --- | --- |
| Seven supported native feature configurations | 1608 test executions and 77 doctests passed; fmt and all-target clippy passed. |
| Miri for four new clock/preflight/identity regressions | 16 executions passed: seeds 1/17, strict provenance, Stacked and Tree Borrows. |
| ASan AP-13 group | 21 tests plus instrumentation configuration check passed; intentional abandonment excluded. |
| TSan AP-13 group | 21 tests plus instrumentation configuration check passed, using the accepted acquire-load variant. |
| Prepared allocation probe | 100000 successful acquire/freeze/clone/drop cycles, zero allocations, reallocations or frees in the measured region; positive allocator control detected allocation/free. |
| Empty/refused allocation controls | Both rejected; failure reason checked so an unrelated CLI error cannot pass the control. |
| Python ordinary and optimized mode | 70 nonprocess tests plus six real-process tests passed in each mode. |
| Corrected replay | 309 runs, including the 1.8 million-event logical soak, passed rights/flow/teardown checks. |
| Final-source TCP smoke | Off/adaptive/cold/warm pressure profiles passed flow, payload, trace and reservation checks; no performance promotion inferred from these four runs. |

Stable native/compiler evidence uses Rust 1.96.0 (`ac68faa20`), Linux x86-64.
Miri/sanitizers use nightly-2026-10-03, Rust 1.101.0-nightly (`0abfedbc7`).
Property tests use eight native cases and four Miri cases. The original nineteen
AP-13 Miri scenarios were not all repeated after the final safe clock/preflight
changes; the later Miri runs target the four new regressions. No new Loom campaign
is claimed: the core atomic protocol is unchanged.

Preserved failed/incomplete attempts include the first isolated Miri property
configuration's forbidden `getcwd`, the killed hard-limit mutation, deliberate
negative controls, and a real-process test attempt missing its `mosaic_probe`
fixture. The property configuration was corrected; the fixture was built from
the same frozen source and the six process tests were rerun successfully in both
Python modes. Earlier failures and the excluded timing repetition remain in the
artifact history.

Component binary/source bindings and Cargo fingerprints establish the earlier
before/after comparison; they are retained separately from the main build record.
The local evidence bundle is `adaptive-memory.tar.gz`, with a SHA-256 manifest,
under ignored `docs/evidence/2026-10-20/`. It includes exact commands and raw
records; the archive is intentionally not part of the public repository.

## Next experiments

1. Separate routing fragmentation from physical exhaustion with an explicitly
   bounded capacity-aware policy. Measure its observation/scan cost and preserve
   exactly one core admission attempt.
2. Compare class-specific small-root catalogs with the proportional catalog.
   Account duplicated domain/grant metadata and Reserved rounding before claiming
   equal capacity.
3. Compare short-window burst envelopes and retained-backing age against the
   current 100 ms offered-byte estimator. Retain the known marginal workloads.
4. Improve hot-path accounting observation only with a before/after measurement.
   The current safe upper-bound check reads up to roots × streams ledgers per
   admission; it does not change remote Drop to maintain an auxiliary total.
5. Exercise automatic growth followed by a sustained low-occupancy interval and
   blocked old-generation drain. The current logical soak did not exercise
   repeated contraction.
6. Require a dedicated-host delivery/tail/CPU win before promotion. A native
   long-duration overload soak and independent review remain release gates.
