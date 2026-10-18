# ABPS reference spike

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

The default backend is an executable resource-accounting model. Its module has
`#![forbid(unsafe_code)]` and no runtime dependencies. It exercises the public
ownership shape before selecting a production Shared algorithm.

The separate opt-in [Berry experiment](berry-spike.md) leaves this
oracle intact under `src/reference/`. The limitations below describe this backend.

**It does not meet the production hot-path contract.** Mutex operations can block,
freeze allocates an `Arc<Allocation>` wrapper, final immutable release frees that
wrapper, and metadata reservation is modeled rather than physically bounded.
The authoritative exceptions are in
[CONTRIBUTING.md](../CONTRIBUTING.md#reference-backend-scope).

## Running the experiment

From the repository root, with Rust 1.96 or later:

```sh
cargo run --locked --example tcp_fanout
cargo run --locked --example tcp_fanout -- --overflow=skip
```

The experiment sends 24 framed payloads through loopback TCP ingress, a domain,
twenty bounded queues, TCP writers, and validating consumers on a four-worker
Tokio runtime. Each queue holds at most two frames. Subscriber zero is deliberately
stalled until after frame 5. Ingress waits for consumer/return acknowledgements
between frames to make this scenario reproducible on shared machines. It is not
an independently scheduled load generator or a performance benchmark.

All twenty views are prepared before any enqueue. Reference creation failure
terminates ingress separately from queue overflow. Each acquisition/view operation
gets at most three application attempts on `Contended`, yielding between attempts;
core operations never retry. The example accepts only `--overflow=disconnect`
(default) and `--overflow=skip`.

| Policy | Outcome in the 24-frame run |
| --- | --- |
| Disconnect | One subscriber disconnected; 458 deliveries enqueued, 456 delivered, two previously queued frames released when its writer task is cancelled and joined; one newly offered delivery rejected on overflow. The other 19 consumers receive all 24 frames. |
| Skip | Four new frames skipped before enqueue; all 476 enqueued deliveries delivered. Subscriber zero receives IDs `0, 1, 6, 7, ..., 23`. No disconnect. |

Both runs end with all 32 payload slots available, zero admitted bytes/read rights,
both domain records reusable, and successful owner-side destruction. They also
reuse a drained domain record and demonstrate `BudgetExceeded` when an input
occupies its entire domain limit and output needs another slot. Offered frame
attempts, planned frames, successful ingress, enqueued deliveries, and actual
deliveries are separate counters; rejected traffic is not reported as throughput.

The adapter sends a stack header and borrowed payload using two `IoSlice`s, advancing
both on short writes. Ingress explicitly initializes the writable range and sets
filled length after successful reading. Cancelling a partial frame terminates the
connection. Regular TCP I/O and the pending-write tests retain their owners until
task destruction completes; no conclusion about registered buffers, kernel
zero-copy sends, or hardware surfaces follows.

## Executable semantics

`StorageOwner::prepare` fixes classes, slot payloads, and the domain table.
`activate` opens root admission. `Pool::try_open_domain(limit)` leases a prepared
record and returns one close authority (`DomainOwner`) and an admission handle
(`Domain`). Zero is a valid domain limit; zero-length acquisition is `InvalidSize`.
Limits cannot change during an incarnation. An opaque generation never wraps;
its exhausted record retires visibly instead of being reused.

Dropping or closing `DomainOwner` closes domain admission. Reuse requires that
owner, all ordinary handles, and all allocations/charges to disappear. Moving,
cloning, and slicing a buffer preserve its origin. Even empty views retain the
whole slot. Ordinary handle clones are also fallible and hold the domain open
for drain, but cannot reopen admission after close.

The reference state mutex serializes root/domain gates, charge reservation,
class credits, concrete slot claims, and commit. `Admission` restores its own
charge/credit/payload on each failure and during unwind. Final allocation Drop
publishes capacity and refunds the origin before completing. A stopped thread
holding this mutex blocks peers: this is precisely the progress limitation that
the later algorithm must replace, not evidence for production helping.

`BufMut` owns a prepared `Box<[u8]>` exclusively. Freeze moves that same Box into
an Arc wrapper; it does not convert it into `Arc<[u8]>` or copy the payload. Only
the actual final Arc destructor returns the Box. Mutable leases are `Send`,
not `Sync` or `Clone`; immutable buffers are `Send + Sync`, with fallible views.
Logical filled and initialized lengths start at zero on every lease, even though
the safe reference backend physically initializes payload at preparation.

`reference_limit` checks read rights per allocation; `handle_limit` checks root
claims including the owner, management handles, and allocations. Both default to
`isize::MAX`, cannot be zero or exceed it, and return `ReferenceLimit` before an
increment would exceed the configured limit. Generations and raw slot indices
are private. Configuration overflow is rejected before preparing payloads.

`try_destroy(&mut self)` closes root admission and returns `Busy` while preserving
the owner if any root claim remains. Buffers cannot inherit arena destruction.
Early owner Drop deliberately retains an arena claim until process exit. An
external `Diagnostics` observer has no arena claim and contains one bounded
abandonment record plus per-root `abps_abandoned_roots` and
`abps_abandoned_bytes` counters. The latter uses modeled reservation bytes in this
reference. Abandonment is terminal even after later refunds. Forgetting an owner
runs no destructor and emits no event. Tests intentionally retain a few small
arenas for these cases; they are not leak-free shutdown tests.

Aggregate root limits and aggregation of diagnostic counters belong to the
application. There is no root registry, deferred retirement queue, participant
registration, or TLS magazine in this crate.

Snapshots allocate and take the state lock. Reservation/admission/capacity fields
form a mutex-consistent observation. Ownership counts from Weak references are
exact only at quiescence: an Arc destructor can have ended its last read right
while still waiting to return the allocation. The oracle samples after complete
operations. Snapshots must not be used as a concurrent admission protocol.

## Remaining allocations and API findings

This is the source-level inventory from the initial spike. The later
[performance audit](performance.md) adds allocator-hook measurements with a
positive control, native timing and tracing. Use its measured regions and
limitations when interpreting allocation counts; this table alone is not evidence
about mutex/runtime/allocator internals.

| Layer | Allocation or work remaining |
| --- | --- |
| Preparation | Config/tables/free-index vectors, one payload Box per slot, root and diagnostics Arc storage; payload initialization. |
| Domain lease / acquire | Prepared record/slot moves and Arc count increments; no payload fallback. Class selection scans the configured class list. |
| Freeze / final immutable drop | One ownership-wrapper allocation and its eventual free; payload Box itself moves back into the slot. |
| Clone / slice | Arc increment under `try_lock`; no per-view object allocation in ABPS source. |
| Snapshot / reporting | Snapshot vectors, report vectors, strings/formatting. Cold diagnostic reads clone a fixed-size record. |
| Adapter / queues | Prepared descriptor/view arrays and stack headers; subscriber/task setup and channel storage allocate. The performance audit measures combined adapter/runtime allocations without claiming per-component attribution. Explicit ingress initialization is O(payload size). |
| Tokio / OS | Task, scheduler, reactor, socket, and kernel buffers outside the core ledger; the audit reports sampled process RSS, which excludes kernel socket memory. |

The fan-out experiment contains fallibility at two adapter boundaries: acquire
and preparation of all subscriber views. Borrowed egress needs no fallible view
operation. Setting the view limit below fan-out produces a distinct, tested
`ReferenceLimit` before any queue receives the frame. This is enough to keep
`try_clone`/`try_slice` as the primitive for the next experiment; it does not
justify introducing an infallible `Clone` or claim ergonomics for a full codec or
Telemt pipeline.

The [performance audit](performance.md) compares
`Bytes + byte budget + bounded queues` with the reference under matched workload
policies. It characterizes this implementation on one VM, without establishing
production performance, maximum host capacity or a long-running RSS bound.

## Validation and gaps

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
PROPTEST_RNG_SEED=424242 cargo test --all-targets --locked
cargo test --doc --locked
```

The original local workflow selected these checks and both example policies.
No GitHub Actions workflow is present in upstream `668d2f3`; use the commands above
and the [verification guide](verification.md) to run checks locally. The historical
change was validated on Linux x86_64 GNU, Rust 1.96.0; a remote CI run is not claimed. Cargo.lock records the resolved
Tokio 1.53.1 and proptest 1.11.0 versions; both are development dependencies.

Coverage includes:

- Fifteen unit scenarios for accounting, each admission checkpoint/rollback,
  unwind, initialization, exact depletion, limits, independent lifetimes,
  forgotten handles, abandonment, generation retirement, and root/domain close
  racing acquisition/final return on newly participating threads.
- An independent HashMap/copying oracle: 128 generated sequences of up to 249
  operations, comparing all four ledgers after every completed operation, along
  with bytes and domain identity. Seeds and shrunk failures are reproducible.
- Eight async scenarios: twenty-way normal/disconnect/skip fan-out, view-limit
  rejection before enqueue, partial vectored writes, malformed/truncated ingress,
  pending-write cancellation, and real TCP ingress cancellation followed by drain.
- Three doctests, including compile failures for mutable `Sync` and `Clone`.

A negative control in a separate temporary copy removed the domain refund on
return. The oracle rejected it with `MEM-02 admission ledger: 8 != 0` and shrank
the failing sequence to open domain, acquire, drop. The normal source was unchanged
by this check. This demonstrates sensitivity to one lost-charge defect, not
completeness of the oracle. The oracle now lives in `tests/support/ledger_oracle.rs`;
failure persistence writes alongside that file.

Reference race tests sample the mutex implementation; they are not exhaustive
schedule or weak-memory models. The separate Berry backend now has
[Miri and Loom evidence](berry-spike.md#validation). Sanitizers, fuzzing,
long soak and cross-target validation have not run. The
performance audit is separate reference evidence, including a Bytes owning
conversion baseline; it does not validate arbitrary foreign APIs.
The original multi-issuer Shared target still needs an accepted progress/return
protocol and refinement to these ledgers. SPEC.md extraction remains a pre-release
documentation task.
