# Shared soft opportunity on a fixed arena

This report covers the opt-in [AP-14](../../../CONTRIBUTING.md#ap-14-shared-soft-opportunity-and-conservative-admission-observations)
adapter and its [usage controls](../../adaptive-memory.md#shared-opportunity-with-fixed-storage).
It extends AP-13's experiment without changing the allocator core, arena layout,
Cargo dependencies, reference counting or remote final return.

## Decision and mechanism

Keep `memory-policy=off` as the default. `adaptive-shared` is a research option
for one Mosaic issuer, one Unreserved root and `elasticity=off`.

The earlier adaptive policy can refuse a full slot because a stream's target is
too small while other streams leave opportunity unused. It can also divide one
512-byte slot into two unusable 256-byte targets. AP-14 permits borrowing unused
shared opportunity, with the global soft total and immutable hard limits checked
before every actual grant. Targets do not grow on a loan.

Physical class exhaustion activates a short pressure interval for that class.
During pressure, both the stream and its group must start below their targets,
but one whole slot may cross a target. Total-budget pressure applies to all
classes. No view, buffer or retained charge is revoked to repay debt.

The additional inputs are deliberately small:

- Demand after queue preflight, with raw offered traffic still counted separately.
- Class pressure from actual `ClassDepleted`, with 0/10/50/100 ms controls.
- A conservative issuer-local cache of real domain-ledger observations.

The cache commits successful charges under a rollback guard and decreases only
from real ledger reads. It refreshes once before a count-based rejection, and
during maintenance. It introduces no allocator retry, callback, unsafe block or
remote-return work. Ordinary admission checks remain bounded; this does not
promise bounded elapsed time or fairness under indefinitely retained ownership.

## Reproduction and evidence scope

Build `spike_probe` and `spike_alloc` with
`local-branches-spike,bytes-interop`, release debug information level 1 and
stripping disabled. The native screen uses:

```sh
python3 scripts/memory-review.py --suite=shared-screen --target=target \
  --output=verification/artifacts/shared-screen --repeats=7 --seconds=15 \
  --slots=32 --pattern=synchronized --slow-profile=constant
```

The matched profile has eight streams, eight subscribers per stream, local
queues/returns, 64 queue credits, Skip, adaptive freshness, an eight-frame writer
batch, a 100 ms deadline and a 4096-byte requested send buffer. Classes are 512
and 1500 bytes, with 32 slots each; the stream hard limit is 128768 bytes. The
native offered rate is 16000 input frames/s. Only subscriber 0 of stream 0 is
artificially slow.

The runner alternates variant order across seven trials. Every run validates
source pacing, consumer payloads, complete loss accounting, final refunds and
confirmed teardown. Affinity uses separate physical-core selections for server,
generator and readers. The host is a shared x86-64 virtual machine with other
process activity, so timing is exploratory, not a dedicated-host performance gate.
CPU figures are sampled server user/system ticks divided by timely recipient
deliveries; tails are medians of run percentiles, not pooled percentiles.

An immutable candidate source snapshot binds tests and binaries. Its verification
source identity is
`fd218dc48e4438f671ccc29a9deab408b3146389cd40006bd22294608a365b93`.
Native compiler: Rust 1.96.0; instrumented compiler:
`nightly-2026-10-03` (Rust 1.101.0-nightly); target:
`x86_64-unknown-linux-gnu`. Raw local artifacts retain the pre-change snapshot,
candidate source manifest, build commands, binary hashes, environment, all
outcomes and initial incomplete invocations. Generated evidence stays outside
tracked documentation.

## Conservation and validation

The new `ap14_` selector contains 17 tests. It covers whole-slot crossing,
shared-total and hard bounds, class-specific pressure, expiry, retained debt,
offered versus eligible demand, error/unwind rollback before and after the core
grant, real remote final return, and full-versus-cached outcome equivalence.
An independent property model counts unique allocations and surviving owners
instead of reproducing ledger atomics.

| Check | Result and scope |
| --- | --- |
| Native adapter tests | 128 passed, including all 17 AP-14 tests and previous queue/I/O/lifecycle regressions. |
| Adapter feature compatibility | 75 passed with Berry; 114 with Berry+Mosaic without bytes interop/local branches. |
| Native library tests | 120 passed with `local-branches-spike,bytes-interop`. |
| Miri | 68 executions: 17 tests × Stacked/Tree Borrows × seeds 1/17, strict provenance. |
| ASan | 17 AP-14 tests plus instrumentation configuration check passed. |
| TSan | 17 AP-14 tests plus actual Acquire-load variant/configuration check passed; no race suppressions. |
| Directed mutations | Shared-total bypass and cache undercharge both killed by their named detectors; unmodified detectors passed first. |
| Harness tests | 79 discovered, 73 passed and six platform skips, under ordinary and optimized Python. |
| Formatting/lints | `cargo fmt --all --check`; clippy for both runnable probes with warnings denied. |

Initial native fixture errors concerned weighted rounding and a stale fixture
root limit. They were corrected without weakening conservation assertions. An
initial ad-hoc instrumented invocation omitted compiler identity in the report;
those incomplete records are excluded from the qualified counts above. The
qualified rerun uses the verification runner's compiler/configuration discovery.
The standalone `mosaic-spike` invocation of `spike_probe` was rejected by Cargo:
this harness declares `berry-spike` as required. The valid Berry+Mosaic invocation
passed; the rejected invocation is retained as a command-configuration failure.

No new atomic protocol was introduced, so these tests do not constitute new Loom
coverage or a new proof of allocator soundness. The paused-publication unit test
is a controlled accounting observation; it does not model memory visibility.

## Allocation and observation cost

Each policy completed 100000 prepared cycles of admission, initialization,
freeze, clone, drop and maintenance. The instrumented positive allocation
control was detected; the measured region made zero allocations, reallocations
and deallocations. Root creation/destruction and diagnostic JSON are outside
that region.

| Observation in sequential full/cached comparison | Full | Cached |
| --- | ---: | ---: |
| Accepted allocations | 100000 | 100000 |
| Ledger reads | 800000 | 1584 |
| Cache-only policy passes | 0 | 99802 |
| Arena reservation, 128 slots per class | 276368 B | 276368 B |

The read reduction is structural evidence, not a native CPU speedup. Cached
occupancy peaks and loan observations can exceed actual instantaneous retention;
they are labelled conservative. In this immediate-drop workload the full mode
sees a 512-byte stream peak while the cache can retain a 32256-byte observation
until refresh. Neither observation is authority for reclamation.

In the 32-slot TCP profile, all variants use a 70800-byte arena. The selected
external model is 706272 bytes with policy off, 720712 with legacy adaptive and
725488 with shared accounting. Shared adds 19216 bytes to the matched external
model (2.72%), including 4776 bytes beyond the legacy policy. This fits the
existing aggregate 5% allowance; it is not a claim about total process RSS.
The separate outer supervisor reserves the inherited 25% envelope: its peak
charge is 88500 bytes in this profile, although actual arena reservation remains
70800. `root-headroom-percent=0` selects an exact one-root supervisor allowance.
This distinction matters for embedding-level quota planning; arena bytes and a
conservative supervisor charge are separate ledgers.
A separate three-second TCP check with zero headroom confirmed both peaks at
70800 bytes and both final charges at zero. It is a functional resource-envelope
check, not an additional performance sample.

## Logical retention replay

The matrix executes 84 deterministic scenarios seven times: three capacities
(16/32/64 slots per class), four arrival patterns and seven enabled policies.
All 588 executions passed accounting, payload and final-drain checks. Each has
30000 input events over 15 logical seconds at 2000 frames/s. Seeds do not change
the replay schedule, so repetitions are not additional independent histories.

Replay uses a synthetic stop/resume recipient in every stream, unlike TCP's
single slow recipient. It writes an identity marker rather than the complete
native payload. Its timely fractions are properties of that retention schedule,
not measurements of socket throughput or elapsed latency.

| Timely recipient deliveries / offered | Legacy | Shared 0 ms | Shared 10 ms | Shared 50 ms | Shared 100 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 slots, steady | 36.947% | 37.120% | 37.120% | 37.033% | 37.033% |
| 32 slots, synchronized | 38.205% | 38.240% | 38.240% | 37.176% | 36.287% |
| 64 slots, phase change | 40.043% | 40.103% | 40.103% | 37.082% | 35.965% |

The table isolates pressure with offered demand and full accounting. Eligible
full/cached variants produced identical non-diagnostic replay results in all
12 capacity/pattern configurations and seven repeats. Eligible demand did not
improve these particular schedules. Longer pressure intervals can retain stale
restrictions and reduce useful delivery; remembering overload longer is not
automatically a better policy.

Five separate three-second native traces covered 16/32/64-slot configurations,
steady/synchronized/phase-change arrivals and 0/10/100 ms pressure. Each retained
29 complete samples, reconciled refunds and confirmed teardown. Pressure masks
and debt are observations at the sampling times; 100 ms trace sampling cannot
reconstruct every 10 ms pressure interval. These traced runs are excluded from
the native timing tables.

## Native screening

All 56 runs completed the functional and accounting checks. Each table entry is
the median of seven 15-second runs; the range retains every run, including severe
service degradation.

| Policy | Timely / planned | Run range | High-priority timely | Server CPU µs / timely delivery | Arrival p50 µs | Arrival p99.9 µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 97.093% | 12.021–98.120% | 94.720% | 3.184 | 562.175 | 12066.815 |
| Legacy adaptive | 90.005% | 89.144–90.969% | 95.462% | 2.820 | 466.431 | 5431.295 |
| Shared 0 ms, offered/full | 98.196% | 97.384–98.234% | 95.648% | 2.551 | 441.087 | 5210.111 |
| Shared 10 ms, offered/full | 98.257% | 97.258–98.282% | 95.637% | 2.491 | 457.215 | 5976.063 |
| Shared 50 ms, offered/full | 98.033% | 96.396–98.151% | 95.497% | 2.577 | 453.119 | 5910.527 |
| Shared 100 ms, offered/full | 97.640% | 63.978–98.210% | 95.298% | 2.535 | 440.319 | 5906.431 |
| Shared 10 ms, eligible/full | 98.166% | 10.293–98.293% | 95.611% | 2.561 | 441.599 | 5885.951 |
| Shared 10 ms, eligible/cached | 97.858% | 97.435–98.330% | 95.298% | 2.553 | 449.535 | 5701.631 |

The shared policy removes much of the earlier soft-partition loss in this
workload. Pressure adds only 0.061 percentage points over the unrestricted
borrowing control in median timely delivery. That small difference does not
establish that pressure prediction is useful.

With the soft total equal to complete payload capacity, `off` already shares
Unreserved physical capacity. Borrowing primarily repairs AP-13's extra soft
partition restriction; it does not create capacity beyond `off`. Timing and
delivery differences against `off` therefore need separate experimental support,
especially when added policy work also changes task scheduling.

Cached accounting reduces median ledger reads from 1918880 to 41176 in the
eligible-demand comparison, while median CPU per timely delivery changes only
from 2.561 to 2.553 µs. Timely delivery is 0.308 percentage points lower with the
cache. This screen does not demonstrate a useful end-to-end speedup from caching,
despite the large reduction in ledger reads. These local-return, eight-stream
ledgers are also a much cheaper read workload than a contended remote-return
configuration would be.

The severe eligible/full outlier delivered 10.293% on time. Its 123328 discarded
input frames include 9007 policy refusals, 178 core refusals and 114143 frames
with no eligible recipients. Another 203489 enqueued deliveries were freshness
evictions; the generator missed 35293 planned arrivals. Reported host CPU
pressure was elevated in the same interval. This supports a mixed pipeline and
scheduling explanation, not attribution of the entire loss to memory admission.
The off and 100 ms controls also have severe outliers. None were removed.

Exploratory finalist gates require either +0.5 percentage points timely delivery
or −5% CPU versus off, no more than 0.5 points of high-priority loss, and no more
than 5% CPU or p99.9 regression. All six shared controls pass these median-based
screening gates on this host. The highest median timely fraction selects
`shared-10-offered-full` for a longer comparison. Passing this screen cannot
promote a default: the shared host and wide ranges remain material limitations.

## Longer comparison

The selected 10 ms, offered/full variant and off completed seven interleaved
60-second runs each, using held-out payload seeds starting at 1000003. Those
seeds change payload contents, not the arrival schedule. All runs passed flow,
payload, reservation and confirmed-refund checks.

| Metric, median of seven runs | Off | Shared 10 ms, offered/full |
| --- | ---: | ---: |
| Timely / planned deliveries | 98.086% | 98.337% |
| Timely fraction range | 97.883–98.368% | 97.980–98.380% |
| High-priority timely fraction | 95.532% | 95.741% |
| Server CPU µs / timely delivery | 2.465 | 2.422 |
| Scheduled-arrival p50 | 428.543 µs | 410.111 µs |
| Scheduled-arrival p99.9 | 6402.047 µs | 4120.575 µs |
| Missed source arrivals | 800 | 448 |
| Discarded input frames | 1930 | 381 |
| Late delivered frames | 150 | 151 |
| Arena reservation | 70800 B | 70800 B |

The difference between timely medians is +0.251 percentage points. Within
matched trial pairs, the median difference is only +0.042 points, with a range
of −0.020 to +0.440. Server CPU per timely delivery improves by 1.74% between
medians. Neither reaches the predefined +0.5-point or −5%-CPU benefit gate.
The observed p99.9 reduction is 35.64%, but remains host- and workload-specific.

Do not promote the policy on this evidence. The implementation repairs unwanted
soft-partition rejection and provides useful measurement controls; it has not
demonstrated a sufficiently large, general delivery or CPU benefit over off.
The default remains off, including after this experiment.

## Follow-up boundaries

- Test pressure versus unrestricted borrowing on a dedicated host and under
  targeted priority wake-up/retention phases. Most of the measured improvement
  over legacy comes from sharing opportunity; added prediction must earn its
  complexity separately.
- Test cached observations with more domains and actual remote returns.
  Fewer ledger reads are established; useful end-to-end CPU savings are not.
- Preserve eligible demand as an explicit comparison. Its semantics exclude
  unusable recipients, but these experiments do not establish a throughput gain.
- Keep return accounting and arena lifetime independent of policy observations.
  A target change, view drop, queue eviction or pressure expiry cannot refund
  backing storage.

The focused validation here does not replace an independent unsafe audit or a
rerun of the complete historical Loom, fuzzing and long-soak campaigns.
Local raw records and immutable source snapshots are stored under the ignored
`verification/artifacts/shared-memory-20261020/` directory, with an evidence index.
Build caches are excluded from that archive.
