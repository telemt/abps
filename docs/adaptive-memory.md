# Adaptive memory research

The opt-in AP-13 adapter explores soft admission targets and multiple Mosaic root
lifetimes. It leaves the core allocator, immutable domain limits, Reserved grants,
reference counting and remote return unchanged. The canonical contract is
[AP-13](../CONTRIBUTING.md#ap-13-experimental-adaptive-memory-and-root-generations).
It is a research prototype, not a default policy or production readiness claim.

The separate [AP-14](../CONTRIBUTING.md#ap-14-shared-soft-opportunity-and-conservative-admission-observations)
experiment adds `adaptive-shared` for a single unreserved root. It borrows idle
soft opportunity without changing arena capacity or the core ownership protocol.
The AP-13 description below continues to apply to `fixed` and `adaptive`;
AP-14's admission rules are described separately.

## Rights and limits

| Quantity | Meaning |
| --- | --- |
| Original reservation | One matched root's complete checked layout, including metadata and padding. |
| Aggregate ceiling | At most 125% of that reservation, including every retained generation. |
| Logical hard limit | One stream's admitted charge summed across all root generations. |
| Soft target | Admission opportunity, recalculated without reclaiming live ownership. |
| Soft contraction debt | Existing admitted charge above a new target; retained until actual refund. |
| Group target | A separately enforced policy opportunity for explicitly grouped streams. |
| External model | Prepared selected adapter structures; at most 105% of the matched baseline. |
| Trace budget | Independent fixed sample capacity, explicitly reported; overflow invalidates completeness. |
| RSS | OS observation; neither arena nor external accounting promises to bound all RSS. |

A synchronous issuer check observes all admitted generations before acquisition.
Returns may lower these observations; no other admission or callback can increase
them before the single core attempt. The conservative sum is safe for admission,
not a claim of coherent snapshot semantics. A view retains its complete backing
charge. Removing seven of eight views can release no physical capacity at all.

Demand is rounded offered bytes, including requests later rejected by policy or
queues. Each 100 ms interval updates an EWMA and mean absolute deviation (400 ms
time constant); the estimate is projected over the freshness horizon. Active
demand includes at least one full requested-slot quantum. A bounded activation
step can use unassigned soft opportunity after subtracting contraction debt; it
cannot take another group's target or bypass a hard limit. A stream already in
debt cannot use this step. This matters after idle periods: waiting for the next
sample would otherwise discard the first burst despite spare memory.

Weighted policy gives 10% of the total equally to active groups, then distributes
the remainder with weights 8:3:1. Strict priority serves high, normal and low in
that order. Capped distribution is equal among streams within a group. Byte-sized
opportunity smaller than a class charge need not admit a frame; neither scheme
promises service under arbitrary retained ownership. Strict starvation is an
observable policy outcome, not allocator exhaustion.

## Root topology and lifecycle

`elasticity=off` uses the original single root. `cold` and `warm` select a small-root
catalog whose complete per-root cost is at most 5% or 10% of original reservation.
Initial roots fit inside the original bound. Extra copies of metadata reduce
payload capacity: equal arena bytes are not equal slot counts. The first catalog
keeps the original class proportions; it does not yet predict a different hot
class distribution.
`capacity-check=require-envelope` checks the selected initial active slots and
grants as well as the original plan. Standby and future growth do not cover an
initial deficit; `report` exposes the topology without that coverage promise.

Cold growth prepares on the issuer. Warm growth first activates standby roots;
standby consumes budget from startup. Both have a maximum of 32 retained records,
64 runtime reconfiguration attempts and one change per second. Growth requires a
full second of observed physical refusals plus positive completion and refund
progress. This prototype samples refusal intervals, so isolated refusal bursts do
not establish that predicate. After five seconds below 25% of the initial byte
reservation, extra active roots stop admitting and drain. This occupancy threshold
includes metadata in its denominator and is deliberately exposed as a candidate
policy, not a payload utilization guarantee.

Routing searches prepared active records, then makes one core attempt on the next
root. A failure describes that root, not all roots simultaneously. This initial
round-robin router can strand capacity and is an experimental comparison point.
Do not silently retry all roots or reinterpret its core error as aggregate exhaustion.
All root changes occur before frame admission; an already acquired frame retains
its old origin through input, fan-out, writes and final refund. Wire sequence and
keyframe recovery epochs are independent of root generation.

Cold maintenance examines at most 32 domain records of one root per 10 ms tick.
Budget recomputation is bounded by 64 streams, eight groups and 32 roots every
100 ms. Preparation/destruction remain explicitly cold, measurable operations.
A successful owner teardown and diagnostic release refund reservation. Busy,
timeout, forgotten buffers and abandonment retain it. The existing SI-16 outer
supervisor conservatively reserves the full elastic envelope for the worker;
`memory` reports the actual internal reservation separately. This is not a new
SI-16 recovery attempt or permission to reconnect/replay streams.

## Running comparisons

Build with `local-branches-spike,bytes-interop`; this adapter uses scalar/batch
handles, not local branch families. The initial supported TCP profile requires
one Mosaic issuer, local queues/returns and explicit Skip.

```sh
cargo build --locked --release --example spike_probe --example spike_alloc \
  --features local-branches-spike,bytes-interop
python3 scripts/memory-review.py --suite=screen --target=target \
  --output=verification/artifacts/memory-screen --repeats=7 --seconds=15
```

Use a separate output directory for every source/binary/affinity revision. The
runner preflights the typed plan before sockets and validates ownership flow and
the memory ledger before reporting success. Its suites separate deterministic
retention replay, TCP screening, longer comparisons, tracing and cyclic soak.
Replay uses ordinary collections and a unique-allocation/owner oracle; its
synthetic retention deadlines are not measured socket delivery or CPU latency.

Relevant explicit flags:

| Flag | Values |
| --- | --- |
| `memory-policy` | `off` (default), `fixed`, `adaptive`, `adaptive-shared` |
| `memory-pressure-ms` | Shared only: `0`, `10` (default), `50`, `100`. Zero is an unrestricted borrowing control. |
| `memory-demand` | Shared only: `eligible` (default) or `offered`. |
| `memory-accounting` | Shared only: `cached` (default) or `full` ledger refresh before each attempt. |
| `memory-priority` | `weighted` (default), `strict` |
| `memory-groups` | One `id:high`, `id:normal` or `id:low` per stream; IDs 0..7. Default groups cycle high/normal/low. |
| `memory-soft-bytes` | Soft total; default initial payload capacity. Does not change logical hard limits. |
| `memory-correlation` | `individual` (default), `group` aggregate estimates. |
| `elasticity` | `off` (default), `cold`, `warm` |
| `root-chunk-percent` | `5` or `10` (default). Complete layout cost, not payload-only. |
| `root-headroom-percent` | `0`, `10`, `25` (default). |
| `memory-trace-capacity` | 0 (off) through 1,000,000 fixed samples. Separate from native timing. |
| `arrival-pattern` | `steady`, `synchronized`, `shifted`, `phase-change`; uniform routing for bursts. |

Burst patterns preserve per-stream counts and payload sequences over complete
four-frame cycles. The pacer changes correlation, not mean offered rate. Arrival
and payload validation remain active at the consumers. Report priority delivery
fractions against planned frames, tails and gaps, all losses, actual reservation,
reservation-time, preparation pauses, policy/core failures, CPU, RSS and page
faults. Compare static equal-peak-budget controls before attributing any gain to
elasticity. Hardware-specific conclusions still require a dedicated host.

Current measured evidence belongs in the dated [research report](reports/2026-10-20/adaptive-memory.md).

## Shared opportunity with fixed storage

AP-14 retains the global soft total as a strict admission bound and each stream's
immutable hard limit. Stream and group targets become scheduling preferences:
outside pressure, a stream can borrow unused opportunity. Lowering a target
creates debt, not a refund. Retained bytes do not become new demand. The adapter
never evicts an owned buffer or increases capacity to repay this debt.

A confirmed `ClassDepleted` starts pressure for that class only. A shared-total
refusal starts pressure across all classes. While pressure is active, both the
stream and its group must begin below their targets. One complete slot may cross
those targets, provided the hard and total bounds still hold. This avoids making
a physical slot unusable merely because targets were divided into smaller byte
amounts. The interval uses the existing 10 ms maintenance clock; it is coarse
policy time, not a per-frame clock read or a wall-clock deadline guarantee.

Raw offered headers are always counted. Eligible demand counts attempts that
reach admission after queue preflight, including policy/core refusals. Frames
with no eligible recipients therefore do not sustain memory demand. This is
independent of freshness eviction, subscriber disconnection and wire framing.

Cached accounting is issuer-local. It stores upper bounds for admitted bytes
and checked group/total sums. A guard provisionally charges the cache before
the single core attempt and rolls back on failure or unwind. A successful grant
commits the observation before returning ownership. Only real domain-ledger
reads can decrease it: periodic maintenance and one full refresh before a
count-based rejection. Dropping a view never refunds this cache. `full` supplies
the same policy with a full refresh on every attempt as an experimental control.

The fixed observation block is included in the existing external metadata
ceiling of 105% of the matched baseline. This mode rejects Reserved and elastic
root configurations before creating resources. Core atomics, remote returns,
arena layouts and default `memory-policy=off` are unchanged.
The inherited `root-headroom-percent=25` still makes the outer supervisor reserve
a conservative byte allowance, even though this mode cannot grow roots. Set
`root-headroom-percent=0` to charge exactly the one-root envelope there. Neither
setting changes the prepared arena or authorizes additional roots.

Diagnostics expose offered/eligible/accepted bytes, ledger refresh/read counts,
cache passes, class/global pressure, observed debt, loans and backing refunds.
Stream and group debt overlap and must not be added. Cached peaks and loan
observations may overestimate instantaneous retention. Refunds are derived from
admitted-ledger differences and accepted charges, then reconciled after
confirmed teardown; they are not counts of dropped views.

```sh
python3 scripts/memory-review.py --suite=shared-screen --target=target \
  --output=verification/artifacts/shared-screen --repeats=7 --seconds=15 \
  --slots=32 --pattern=synchronized --slow-profile=constant
```

The eight controls separate disabled policy, AP-13, unrestricted pooling,
10/50/100 ms pressure, eligible demand and cached accounting. Deterministic
`shared-replay` uses 10 ms policy ticks with 100 ms demand recomputation;
synthetic retention results must remain separate from native TCP delivery.
`shared-final` uses held-out seeds. Compare all loss categories, priority delivery,
tails and CPU with identical queues, capacities and wire policies. A shared host
can screen candidates, but cannot justify promoting a production default.

The [shared-memory report](reports/2026-10-20/shared-memory.md) records validation,
allocation observations and the separated replay/native comparisons.

The cached path reuses a successful policy decision within one unchanged issuer
operation. A refresh requires a fresh decision; full accounting refreshes first.
Neither shortcut skips the pending-cache guard or the core admission attempt.
Class charge lookup scans fewer than 64 classes and uses a binary search at 64;
both select the smallest sufficient class and charge its actual capacity.
See the [implementation comparison](reports/2026-10-23/memory-shortcuts.md).
