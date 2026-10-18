# Berry: single-issuer experiment

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

This opt-in backend implements the accepted narrower execution model in
[SI-01 through SI-06](../CONTRIBUTING.md#berry-single-issuer-experiment). It preserves the default
safe reference as an independent semantic oracle. The package is unpublished;
the public API and representation remain experimental.

The architectural changes address the reference audit's root mutex, per-freeze
heap wrapper, repeated root-reference traffic during fan-out, and full diagnostic
scans. They do not implement multi-issuer Shared admission or the proposed counter
tree. No helping protocol, per-thread registration, magazine, or deferred return
queue has been introduced.

## Execution and resource model

Each worker prepares and owns its root. `StorageOwner`, `Pool`, `DomainOwner`, and
`Domain` are neither Send nor Sync. `BufMut` is Send, exclusive, not Sync/Clone;
`Buf` is Send + Sync. Returning a buffer on a previously uninvolved thread requires
no registration. Transfer through an application queue preserves its origin.

A root supports 1..=64 increasing classes, each with 1..=usize::BITS slots. These
are checked implementation limits. Classes have one atomic free bitmap; admission
never scans multiple words, borrows another root's capacity, spills into another
class, or falls back to the heap. Class lookup uses at most seven comparisons.

Preparation borrows `Config` and performs two owned allocations: an arena and an
independent diagnostics record. The arena contains fixed class/domain/slot tables,
a prepared domain-index stack, and payload. Checked `Layout` composition accounts
for typed metadata alignment, padding, both allocations, and the entire payload
reservation. System allocator overhead and external handles/queues are outside
this charge. Payload starts uninitialized; physical page residency is distinct
from reserved layout bytes. There is no page-locking or fault-latency promise.

Domain creation pops a prepared free-record stack. The last issuer-side control
handle checks only its own record: a drained record recycles immediately; a record
with outstanding allocations enters an intrusive FIFO in the prepared metadata.
Remote buffer drops still publish capacity and refund charge synchronously; they
never touch this FIFO. `Pool::poll_domains(n)` visits at most `min(n, entry_pending)`
records, reclaims completed candidates, and rotates unfinished ones. Snapshots do
not perform maintenance. An empty free stack reports `DomainMaintenanceRequired`
when candidates exist, otherwise `DomainCapacityExhausted`. Neither result promises
future availability. Generations retire at exhaustion; a pending record itself does
not pin the arena after its real ownership claims finish.

The explicit example adapter `open_after_maintenance` examines at most 32 records
per call, then returns success or the remaining error. Further retries belong to
the caller. Domain limits may sum above backing reservation: byte limits alone
are not dedicated reservations.
Opaque domain identities are scoped to their root, not globally unique stream IDs.

`CapacityPolicy::Reserved` adds a separate hard partition of each class inside a
root. `try_open_reserved_domain(limit, grants)` commits every class grant together;
unavailable rights return `ReservationUnavailable`. Idle grants cannot be borrowed.
The prepared matrix contains one `usize` mask per domain/class, only in Reserved
mode. Complete domain recycle returns its grants, including when a record retires;
remote buffer Drop remains unchanged. Acquiring from an empty domain mask returns
`DomainClassDepleted`. The default Unreserved policy and `try_open_domain` retain
their existing semantics. See the [reservation and freshness follow-up](reservations-performance.md).

The four ledgers have distinct roles:

| Ledger | Concrete representation and boundary |
| --- | --- |
| Reservation | Immutable checked arena/diagnostic layouts until cold teardown. |
| Class capacity | One free bit per reusable slot; only its issuer clears bits. |
| Admission | Atomic bytes per origin, including pending admission/return; a separate count pins each outstanding allocation. |
| Ownership | Prepared slot refcount plus exclusive mutable or immutable handles; one root claim per allocation, independent of view count. |

Idle capacity in another root is stranded. With four equal partitions, a stream
confined to a full partition cannot use the other three quarters of idle slots.
The application must choose root sizing and stream placement with this limitation
visible. Aggregate reservation across all current, replacing, and abandoned roots
remains an application responsibility.

## Admission and return protocol

The implementation is split into [atomic transitions](../src/berry/protocol.rs)
and a [private storage boundary](../src/berry/raw.rs). The Loom build swaps
the atomics used by those exact transitions; it does not duplicate the algorithm.

Admission runs only on the issuer:

1. Check size, class, root/domain lifecycle, and a bounded root lifetime claim.
2. Reserve the full class charge with one checked CAS. A changed word returns
   `Contended`; insufficient observed headroom returns `BudgetExceeded`.
3. Load the class bitmap, intersecting with the origin's grant in Reserved mode.
   A zero hint requires one Acquire `fetch_or(0)` confirmation using the same mask.
   Only a zero confirmation observes `ClassDepleted` or `DomainClassDepleted`.
   A positive hint or confirmation is claimed with an Acquire fetch-and; no other
   issuer can steal it. An empty grant mask needs no bitmap access.
4. Install origin, reset ownership/initialization metadata, and commit the domain
   allocation claim before exposing the exclusive lease.

An admission guard restores its own bit, quota, and lifetime claim on failure or
unwind. Size/lifecycle failures cannot invoke user code or expose partial ownership.
Tests inject failure after charge, after bit claim, and after ownership setup.

Freeze consumes `BufMut` without allocation or payload copy. `try_clone` and
`try_slice` perform one checked refcount CAS and may return `ReferenceLimit` or
`Contended`; no automatic retry or infallible `Clone` has been added.

Final release completes this sequence synchronously:

```text
release refcount decrement + final acquire fence
    -> copy origin and every return operand out of reusable slot metadata
    -> release-OR free bit
    -> refund the saved origin's charge
    -> release that domain's allocation claim
    -> release the root lifetime claim as the final arena access
```

After bit publication, another acquisition may reuse the slot before the old
refund completes. The old return uses only saved operands and pinned, stable
root/class/domain records. It never rereads slot origin or refcount. Thus a new
origin cannot receive an old refund, and record reuse/teardown cannot precede
completion. This deliberately permits conservative pending-return charge after
capacity becomes available.

PublishedReturn holds the atomic admitted/allocation fields separately. Its terminal
root claim uses a transparent Claims wrapper; no padded ledger borrow survives the
deallocation-enabling decrement. This additional OWN-03 obligation is explained in
the [final-refund correction](final-refund.md).

A thread stopped before publication retains its issued slot. One stopped after
publication retains its remaining quota/allocation/root claims. No other thread
helps it; it can delay admission on that domain and prevent drain. This is an
explicit scope change from original PROG-03, not a claim of equivalent progress.
Completed Drop has no backlog left to process.

### Work and synchronization bounds

Counts below are ABPS source-level atomic operations, excluding explicitly
requested payload initialization/copying. They do not bound machine steps in an
atomic primitive or elapsed time under preemption.

| Operation | Source-level work bound |
| --- | --- |
| Acquire, positive first hint | At most seven class comparisons; three atomic loads, four RMWs (one CAS), two stores. |
| Acquire, positive confirmation | Same loads/stores, five RMWs including confirmation. |
| Confirmed depleted, including rollback | Three atomic loads, five RMWs: root pin, admission CAS, confirmation, quota refund, root release. |
| Empty grant, after successful byte admission | Two atomic loads, four RMWs; no bitmap access. Budget errors still precede the grant check. |
| Freeze | One metadata store; moves ownership without a copy. |
| Try-clone / try-slice | Checked range arithmetic, one load and one CAS. |
| Nonfinal drop | One release RMW. |
| Final drop | Five RMWs and one acquire fence, independent of payload size and domain count. |
| Admission rollback after bit claim | One refcount store and three RMWs, replacing commit; at most seven RMWs for the complete failed attempt including confirmation. |
| Root metrics | At most 64 class-word loads plus the root claim load; no allocation. |
| Selected-domain metrics | Two atomic loads plus issuer-owned state; no allocation. |

No operation above contains a retry loop, mutex, syscall, user callback, collection
growth, or system allocator call. Full snapshots and root close are cold scans.
Unreserved domain control release/open are constant work. Reserved open is bounded
by class lookup and grant selection; recycle visits the class table. Explicit
maintenance is bounded by its record count times that per-record work. The
refcount, bitmap, admission, domain claims, and root claims
remain distinct synchronization sites; they can still transfer cache lines between
CPUs on remote return.

The zero hint is advisory, not a valid depletion witness by itself. An identity
RMW reads the immediately preceding value in the bitmap's modification order.
Only the issuer clears bits and its grant stays fixed throughout admission, so
zero at confirmation witnesses absence of eligible capacity; a later return may
make capacity available before the caller sees the error. Acquire loads alone do
not provide this RMW observation rule. See the
[Rust memory-model overview](https://doc.rust-lang.org/nomicon/atomics.html) and
[RMW rule](https://eel.is/c++draft/atomics.order#11).

The confirmation regression passes an intentionally stale zero hint to the same
private continuation called by production admission. Removing confirmation must
fail that regression at runtime. It tests the chosen confirmation protocol, not
the claim that a publication occurring after a real empty observation would make
that earlier observation invalid. Bounds and new evidence are in the
[depletion review follow-up](berry-depletion-review.md).

`try_as_io_slice()` borrows only filled bytes and reports `TooLarge` if a single
Windows descriptor cannot represent the view length. `as_io_slice()` retains the
standard constructor's panic for that case. Neither creates another owning right.
Root identities are opaque process-local values, not addresses; domain identity
is root-scoped and unassigned root identity zero is not a unique key. See
[identities and observations](concepts.md#identities-and-observations).

## Safety and lifecycle proofs to review

**Storage separation (MEM-01, OWN-01/03).** Checked layouts produce disjoint typed
metadata and per-slot payload ranges. Pointer derivation retains the arena's
provenance; no temporary `&mut` spanning the arena or adjacent live slots is
created. Only `&mut BufMut` exposes writable initialized bytes in its slot.
`filled_len <= initialized_len <= capacity` is checked. Reacquisition resets both
lengths, so stale contents cannot become safe payload. `initialize_to` explicitly
zeros the newly initialized prefix; generic TCP reads are not given uninitialized
`&mut [u8]`. The optional `bytes-interop` feature exposes the exclusive tail as
`bytes::buf::UninitSlice`, checks cursor bounds, and relies on the unsafe advance
caller to initialize its prefix. Safe Tokio `read_buf` uses a frame-length limit;
partial reads cannot expose the remaining spare bytes. There is no owning `Bytes`
conversion or Tokio dependency in the core. Slice arithmetic stays within the frozen filled range, including empty
views that still retain the whole backing charge.

**Publication and reuse (SI-01, OWN-03).** Safe transfer supplies publication of an
owned buffer to another thread. Mutation rights end at freeze. Release decrements
and the final acquire fence collect prior readers before the release bitmap OR;
the next issuer's acquire RMW precedes overwrite. Slot metadata is not accessed
after publication by the old return. Removing the fence makes the Loom visibility
regression fail before any join can supply synchronization.

**Storage retirement (LIFE-01/02).** Each outstanding allocation pins the root
through its last return; control handles also pin it. Only the issuer adds root
claims and remote paths only decrement, so the checked increment cannot overshoot
its limit. Closed domains are reusable only after acquire observations of their
completion counters. Cold owner destruction requires an acquire observation of
the sole remaining owner claim. A test permits teardown before joining the returning
thread, so join synchronization is not assumed. No buffer inherits deallocation.

Early owner Drop closes admission and marks an arena abandoned while retaining
its owner claim until process exit. Diagnostics expose abandonment bytes and the
claims/charge sampled at abandonment; those values are historical, saturating
diagnostics, not current admission evidence. A separately owned diagnostics record
can outlive successful arena teardown; its final observer drop may free that record
on the cold monitoring path. Forgetting an owner emits no event. Forgetting any
claim cannot authorize reuse or a refund. These are terminal resource losses, not
a normal replacement-root strategy.
The application must retain its diagnostics observer outside the owner to inspect
an abandonment after losing that owner; Drop does not invoke a logging callback.

## Running the comparison

The default crate exports the original reference API. Consumers opt in with the
`berry-spike` feature and use `abps_reference::berry`. Create roots
inside the worker that will issue allocations. For the included experiments:

```sh
CARGO_TARGET_DIR=/tmp/abps-reference-target \
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
  cargo build --release --locked --features bytes-interop --examples

python3 scripts/berry-performance.py --suite=smoke \
  --output=/tmp/abps-berry-smoke
python3 scripts/berry-performance.py --suite=all \
  --output=/tmp/abps-berry-measurement --repeats=3 --seconds=1 --tcp-seconds=10
```

The Linux runner accepts `--micro-cpus`, `--server-cpus`, `--generator-cpus`, and
`--reader-cpus`. Pacing has its own thread; independent source writers and consumers
run on the client I/O runtime. Server/client sets must be disjoint; select separate physical cores where
possible. Affinity is measurement support, not part of the allocator. Use a new
output directory after changing source or binaries; the runner verifies hashes
before resuming. Suites are `core`, `alloc`, `observe`, `tcp`, `pressure`, and correctness-only
`smoke`; `architecture` adds workload size, concurrent views, domain churn, load
ramps and bounded batching/read-buffer variants in seeded randomized trial order.
An explicit `--server-binary` override records the preserved server hash for
before/after comparison using the same current client. Each JSON record includes commands, configuration, failures, useful work,
drain checks, and separately sampled process RSS/user/system CPU ticks. Builds/tests/profilers must not
overlap native measurements.

The TCP server has one current-thread Tokio runtime per worker and creates each
nonshared root on its issuer. Remote-return scenarios route egress to another
worker; the one-issuer case uses a second egress worker. All backends get identical
total class capacity, domain limits, arrival schedules, queue limits, and overload
policy. The four comparisons are shared-root reference, per-worker reference,
Berry, and per-worker budgeted Bytes. Slots are partitioned across worker
roots; they are not multiplied by the worker count.

A separate process generates arrivals and checks framing, payload endpoints, and
subscriber order. Per-stream bounded source queues isolate blocked socket writes;
per-stream seeded payload schedules prevent routing from concentrating keyframes.
Its clock-based pacing is outside server profiles. Late arrivals remain explicit
misses; the generator does not make catch-up bursts. Latency starts at scheduled
arrival, and useful throughput uses the entire offered interval even after
failure. Planned, pacing misses, attempted, source-queued/refused, sent/failed, received,
accepted/discarded, subscriber-enqueued, written, delivered, skipped,
disconnected, and cancelled work are recorded separately. The runner checks their
conservation; undelivered queued or partial frames cannot disappear from reports.

The server reports a cold `PipelinePlan` with per-root class capacity, domain
limits, ingress headroom, conservative unique backing bounds for independent queues
and writer batches, and modeled external item/scratch storage. It does not promise
availability from a byte limit. Queue permits are reserved immediately after the
header, before acquisition. Only recipients with permits need views, and all views
must succeed before any publication. Each adapter operation gets at most three
explicit attempts on `Contended`.

Queue overload disconnects by default. Explicit `--overflow=skip` also handles
BudgetExceeded/ClassDepleted/Contended by draining the rejected payload through
prepared scratch of at most 16 KiB per stream. Closed/ReferenceLimit remain fatal;
EOF or cancellation within payload terminates ingress. A fully read rejected frame
is reported as discarded, never as accepted useful work. `--ingress=read-buf`
requires the single backend and `bytes-interop`; initialized reads remain default.

`--write-batch=1` is default. Values up to eight consume only already-ready messages
on the same socket, using at most sixteen stack descriptors without a coalescing
wait. Partial writes release each complete frame before processing the remaining
batch. `write_api_calls` counts asynchronous write operations, not kernel syscalls;
use the separate strace pass for syscall counts. All workers finish returns before
owner teardown. Readers preserve completed samples on cancellation. Process setup
and measurement have deadlines; a failed run does not become a timing sample.

The kernel's TCP buffers, task/channel allocations, and descriptor setup remain
outside ABPS accounting. A zero-allocation core does not imply a zero-allocation
Tokio pipeline, a bound on external queues, or immediate delivery to a slow reader.

## Validation

The original validation below documents the initial spike. Later checks are
recorded in the [architecture follow-up](architecture-performance.md) and the
[reservation and freshness follow-up](reservations-performance.md), including
eight Loom models and the completed 30-minute resource soak.


The following checks ran locally on Linux x86_64 GNU, Rust 1.96.0:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
PROPTEST_RNG_SEED=424242 cargo test --all-targets --locked
PROPTEST_RNG_SEED=424242 cargo test --all-targets --features berry-spike --locked
cargo test --doc --features berry-spike --locked
cargo test --features loom-model --lib berry:: --locked
```

Fifteen new storage tests cover failure/unwind rollback, initialization and live
neighbor boundaries, full-word capacity, reference limits, empty retained views,
cross-thread mutable handoff/final return, interrupted return followed by reuse,
domain generation/drain, close, cold teardown without join, forgotten ownership,
observable abandonment, checked layout, stranded roots, and transform headroom.
The same independent four-ledger property oracle runs 128 generated sequences
against each backend. Five new compile-fail doctests constrain control and mutable
auto traits. Existing reference and TCP tests remain in place. A new TCP-reader
regression verifies accounting through cancellation during a partial frame.

Five Loom models execute the actual protocol with at most two concurrent returners,
one issuer, two slots, and a preemption bound of two. `max_branches=10000` bounds
per-execution exploration; there is no permutation cap. They check final-reader
visibility, reuse before refund, close/teardown claims, refcount/admission races,
and distinct-bit conservation. Library unit tests under `loom-model` replace the
protocol atomics and exclude raw storage. Ordinary builds and integration tests
still use std atomics, including when all features are enabled. Loom does not
execute pointer arithmetic or establish progress beyond the specified model.

Focused Miri uses nightly-2026-10-03 (1.101.0-nightly, 0abfedbc7):

```sh
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17' \
  cargo +nightly-2026-10-03 miri test --features berry-spike \
  --lib berry:: --locked
```

Seeds 1, 17, and 42 passed all fifteen storage tests. Leak reporting is disabled
only because forget/abandonment
tests deliberately retain storage; this does not disable UB or race checks.
The initial CI covered those fifteen tests. Current CI adds the optional interop
coverage described in the follow-up. No completed remote CI run is claimed.

Two mutation checks ran in isolated temporary copies. Removing the final acquire
fence failed the Loom visibility assertion (`0 != 1`). Removing the saved-domain
refund failed the independent `MEM-02 admission ledger` assertion (`8 != 0`),
shrinking to three operations. The normal source was never mutated.

Allocation instrumentation uses a separate executable and an intentional allocating
positive control. It checks complete operations, rollback/failure paths and first
final return from a fresh thread. Checks assert outside allocator hooks. Preparation,
reporting, task setup, and cold destruction are outside the measured core spans.

Sanitizers, fuzzing, long-running soak, big-endian/32-bit targets, other OSes, and a
dedicated performance host were not validated in the initial spike. Neither
Miri nor Loom is a proof of soundness. Original Shared helping, arbitrary slot
counts, hierarchical budgets, and cross-root migration remain unimplemented.

## Performance evidence

The measurements below predate explicit domain maintenance and the SI-04 adapters.
Use the [architecture follow-up](architecture-performance.md) for current before/after
results and the corrected load generator; do not compare these TCP series as if
their schedules and client CPU allocation were identical.


Measurements ran on 2026-10-03 on the same Linux 6.12.90 Hyper-V VM as the reference
audit: exposed Ryzen 5900X topology, ten logical CPUs/five cores, Rust 1.96.0,
release with debug information, default target CPU, no LTO override. Micro workers
were pinned in order `0,2,4,6,8,1,3,5`; TCP server workers used `0,2,4,6` and the
generator used `9`. The profiled generator used `8,9`. This is a shared VM, with
SMT and no isolated-host performance gate. Hardware cycles/instructions/cache-miss
events returned `not supported`; only software profiling is reported.

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The local measurement summary (`evidence/single-issuer-2026-10-03.json`) contains
medians, ranges, failures, delivery conservation, allocation counts, source/binary
hashes, and environment manifests. Detailed JSON/RSS series and profiles are under
`/tmp/abps-single-issuer/` in this workspace:

- `final-core`: 168 native/latency trials, three trials per configuration,
  0.2-second warm-up and one measured second. This supersedes the preliminary
  micro series and normalizes per-domain limits across root layouts.
- `final-allocation`: 30 instrumented operation configurations, 10,000 iterations
  each, outside native timing. Positive controls and first-thread returns passed.
- `validated-measurements`: 27 monitoring trials and 84 ten-second TCP trials.
  These preceded the final cold micro-setup limit correction; storage/protocol and
  TCP behavior were unchanged. Earlier micro/allocation rows in this directory
  are superseded by the two final series above.
- `pressure`: 24 five-second runs, both policies under larger skewed payloads.
- `traces`: separate software CPU profiles and syscall summaries; their timings
  are excluded from native throughput.
- `final-smoke`: eight server/generator conservation and teardown checks.

### Core throughput and allocation

These are successful `acquire(1500) -> freeze -> drop` cycles **without filling
payload**. Rates are medians of native passes; p99 comes from separate instrumented
latency passes. No failed attempt is included in useful throughput.

| Backend | 1 worker, M cycles/s | 4 workers, M cycles/s | 8 workers, M cycles/s | 8-worker failures | 8-worker successful p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Reference, shared root | 7.26 | 2.26 | 1.55 | 78.86% Contended | 38.56 us |
| Reference, root per worker | 6.83 | 29.55 | 43.13 | 0% | 221 ns |
| Berry, root per worker | 14.59 | 64.72 | 104.15 | 0% | 120 ns |
| Budgeted Bytes, root per worker | 8.38 | 34.12 | 47.60 | 0% | 210 ns |

Most of the scalability gain comes from partitioning roots. The new representation
adds about 2.1–2.4x successful cycle throughput over the already partitioned
reference in these cases. Neither comparison establishes a sustained media byte
rate, a maximum server capacity, or a wall-clock bound.

For 10,000 complete freeze/drop cycles, reference performed 10,000 allocations and
10,000 frees (64-byte wrappers). Budgeted Bytes performed 20,000 of each, including
payload. Berry performed **zero allocations, frees, or reallocations**.
Its acquire/drop, copy, freeze, final drop, fill, explicit initialization, clone,
slice, and twenty-view fan-out checks also reported zero; failure paths and first
final return on a new thread reported zero. This statement covers prepared core
spans, not Tokio setup, queues, reporting, or cold arena destruction.

Fan-out did not improve: twenty views plus release had median measured p50 of
601 ns for reference and 692 ns for Berry in the final series. The earlier
series was approximately equal. There is no evidence to advertise faster fan-out;
checked refcount operations and benchmark dispatch still cost time.

For a 2-MiB copy/freeze/drop cycle, Berry p50 was 50.5 us versus reference
54.0 us. Explicit initialize-then-fill measured 87.7 us versus 93.1 us, even though
that span excludes acquire/drop. Removing allocator work does not remove the
extra payload pass required by the current initialized-slice ingress adapter.
Large-copy tails varied considerably between trials/series on this VM.

### Monitoring

With 65,536 domain records, full Berry snapshots averaged 525 us per sample
at approximately 100 Hz, and cycle rate fell from 15.95 to 14.84 M/s in the
one-second monitoring pass. Reference full snapshots averaged 428 us. Full
snapshots remain costly cold work and did not become a cheap monitoring API.

The single-class `metrics()` sample averaged 110 ns at 100 Hz, with measured rate
15.88 M/s versus 15.95 M/s without monitoring. It provides aggregate capacity and
root claims; it does **not** compute a 65,536-row domain/ownership snapshot.
Selected-domain metrics are a separate constant-work query. This reduction in
requested information is deliberate, and does not introduce a counter tree.

### TCP and overload

At 1,000 planned packets/s with four streams and twenty subscribers, normal local
delivery medians were 98.91–99.25% of planned deliveries across the four backends.
Berry reached 99.15%, with scheduled-arrival p99 of 5.26 ms. This workload
does not establish an end-to-end improvement from the much faster empty core cycle.

The remote-return Berry series delivered 94.86% at the median, compared
with 98.72% for partitioned reference. All of its successfully sent frames were
accepted, enqueued, written, and delivered; the three generator runs missed
265, 604, and 514 of 10,000 scheduled arrivals, with no failed writes or Contended
attempts. These losses stay in the reported denominator. Disjoint CPU sets alone
do not prove that the generator or shared host can sustain a given schedule.

Skew plus the small default queues produced unstable disconnect outcomes: single
issuer delivered 63.13–99.12% across three runs; partitioned reference delivered
8.78–68.28%. These are overload-policy outcomes, not evidence that one allocator
has a stable low-latency capacity at that load. The global deterministic class
sequence can also correlate keyframes with particular streams; it is a synthetic
workload, not a validated codec distribution or external application workload.

The 200-frames/s, one-subscriber-per-stream frame runs delivered all 2,000 planned
frames for every backend, with all roots drained. Tiny-packet slow-reader runs
often filled kernel buffers without filling the bounded application queues:
ABPS could return its memory while the reader still had a large delivery backlog.

The additional pressure runs use 64-KiB/2-MiB classes, 80 planned frames/s, skew,
twenty subscribers, one slow reader, and a five-second offer window. Every
disconnect run disconnected exactly one subscriber; every skip run kept it
connected and reported 243–263 skipped deliveries. In the Berry runs,
skip counts were 263, 247, and 248. No ingress stopped, flow counters conserved
work, and all domains/slots drained in both policies. Delivered fractions remained
about 95–96%, reflecting the intentional slow-reader loss and deadline; rejected
or unfinished work is not presented as throughput.

### Profiles and remaining work

An eight-worker diagnostic run recorded 156,251 `futex` calls for shared reference
versus 70 for Berry, including startup, taskset helpers, barriers, and joins.
The latter total is not a claim that the whole executable has no syscalls. The
reference CPU profile attributed 26.15% of user samples to `lock_contended`.
In Berry, acquire and final release account for substantial work instead;
the prepared protocol still performs multiple atomic operations per allocation.

Server-only TCP profiles contain I/O, queue, and reactor work and exclude generator
pacing. They collected only 77/70 user samples at this modest load, so their fine
percentage breakdown is not statistically useful. Reproduce the diagnostic passes
with `bash scripts/berry-trace.sh OUTPUT_DIRECTORY`; the script's default
CPU topology and tool paths are specific to the recorded host.

The next optimization decisions need an external workload and a dedicated host:
measure remote-return cache traffic before changing metadata layout; measure
fan-out before replacing fallible views; evaluate an explicit initialization-aware
ingress adapter before removing zeroing; and size shards/queues/headroom against
bursts and slow readers. The prototype makes none of these changes implicitly.
Counters in a hierarchy, helping, larger bitmaps, and cross-root balancing require
new accepted semantics before implementation.
