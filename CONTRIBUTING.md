# Contributing to ABPS

This document is the canonical engineering contract for ABPS. Invariant IDs are
stable references for designs, reviews, proofs, and tests. README summarizes this
contract; AGENTS makes it operational. Neither defines a competing specification.

**Status: executable reference, Berry and Mosaic experiments.** The unpublished
`abps-reference` crate retains its safe mutex oracle and adds two opt-in backends
with worker-local admission, prepared raw storage, and concurrent buffer returns.
None is the original multi-issuer Shared implementation. There is no accepted
multi-issuer Shared algorithm, stabilized API, or performance guarantee.
The requirements below remain the production target unless an exception is named
in [Reference backend scope](#reference-backend-scope),
[Berry](#berry-single-issuer-experiment) or
[Mosaic](#mosaic-segmented-single-issuer-experiment).

## Backend names

**Berry** is the single-issuer backend with one bitmap word per size class.
**Mosaic** is the segmented backend with multiple bitmap words per size class;
it also uses single-issuer admission. These names identify implementations, not
different ownership or concurrency guarantees.

| Implementation | Public module | Feature | CLI backend |
| --- | --- | --- | --- |
| Berry | `abps_reference::berry` | `berry-spike` | `berry` |
| Mosaic | `abps_reference::mosaic` | `mosaic-spike` | `mosaic` |

The naming transition removes former module, feature and CLI names without
compatibility aliases. The package remains `abps-reference`; invariant IDs,
including SI-01..17, BR-01..06 and AP-01..09, remain stable. Historical evidence
retains its original names, commands and source hashes. Current documentation
uses the new names and equivalent current commands without claiming new measurements.
Archived evidence calls Berry `single` / `single_issuer` and Mosaic `segmented` /
`segmented_issuer`; those names are historical identifiers, not supported aliases.

## Direction and v0 scope

Resolve tradeoffs in this order:

1. Soundness.
2. Bounded resource semantics.
3. Bounded hot-path work and deterministic reclamation.
4. Throughput.
5. Ergonomics.

ABPS optimizes predictable resource consumption before minimum average allocation
latency. Expensive work belongs to explicit preparation, teardown, or an operation
whose cost the caller requests and can observe.

The first version targets CPU-resident byte buffers, encoded packets, and network
payloads. It uses Shared ownership, fixed storage and size classes, flat accounting
domains, mutable leases, immutable views, and borrowed scatter/gather descriptors.
Shared pool/domain handles and immutable buffers support `Send + Sync`; mutable
leases are exclusive, `Send`, not `Sync`, and not `Clone`. The first integration
is Tokio TCP through a bounded queue across workers; the core remains synchronous
and runtime-agnostic.

Storage, classes, and the number of domain records are fixed before activation.
Streams lease prepared records dynamically; each incarnation's limit is fixed at
lease creation. A domain limit alone grants no dedicated storage reservation; the
opt-in experiment in SI-05 supplies separate class reservations. Replacement
arenas require accounting for overlapping reservations. Representation limits are
private implementation choices, checked during preparation with layout arithmetic
and the root budget; there is no normative 32-bit slot ceiling.

Local mode, hierarchical quotas, magazines, owning chains, mutable splitting,
thaw/COW, automatic compaction, active reconfiguration, and specialized storage
are extensions. ABPS does not bound external queues, task counts, or all process
RSS; guarantee progress of arbitrary pipelines; or provide an end-to-end zero-copy
transport. Hardware surfaces and decoder-owned storage are outside the v0 model.

## Seven contribution rules

1. **No hidden allocation in steady-state hot paths.**
2. **No unsafe without a documented invariant and proof obligation.**
3. **No lock-free algorithm without Loom coverage.**
4. **No performance optimization without a before/after benchmark.**
5. **No platform-specific behavior in core semantics.**
6. **No implicit waiting, allocation fallback, or copying.**
7. **Safe Rust callers must be unable to cause UB, budget violation, or double
   reclamation.**

## Resource model

Four ledgers are canonical. Operations must name their effects on each; a reference
model must check them independently, not infer correctness from payload bytes.

| Ledger | Resource and legal changes |
| --- | --- |
| Reservation | Prepared backing and accounted metadata; constant until owner-side destruction, retained on abandonment. |
| Capacity | Per-class credits conserved across available, acquiring, issued, and returning states. |
| Admission | Domain headroom becomes pending charge, then live, then pending return, then refunded. |
| Ownership | Exclusive write ownership becomes immutable read rights; views add rights, drops remove them, only the final right permits return. |

Freeze and view creation change ownership only. Failed admission restores this
attempt's admission/capacity claims. Return completes ownership, publishes capacity,
then refunds origin admission. Domain reuse requires all old rights and claims to
end. Reference counts may increase: the ownership ledger records legal rights,
not a claim that their numerical sum is constant.

### MEM-01: Reservation units and the root bound

| Unit | Ownership and accounting |
| --- | --- |
| Region/slab | Physical reservation owned and charged once by the root. |
| Slot | Independently recyclable allocation unit; its full backing capacity is the domain charge. |
| View | Offset and length within an allocation; retains its charge without a new reservation. |
| Metadata | All ABPS-owned headers, tables, descriptors, helping records, and bookkeeping; charged to the root. |

At all times, including preparation and abandoned storage:

```text
reserved_backing_bytes(root) + metadata_bytes(root) <= root_limit
```

Cover ABPS-owned alignment padding, temporary preparation storage, and helper
metadata before allocating them. Idle slots remain reserved. A live slot may keep
its slab reserved at the root; it does not charge that entire slab again to every
domain. Root reservation and domain admission are different constraints, not
disjoint physical allocations to add together.

System allocator overhead, OS accounting, and application allocations are outside
this byte accounting and must be distinguished when observing RSS. A process-wide
bound also needs a bound on all roots, including replacements and abandoned roots.

### MEM-02: Domain admission and refunds

For each domain:

```text
admitted_bytes =
    pending_acquire_bytes + live_backing_bytes + pending_return_bytes

admitted_bytes <= domain_limit
```

These are abstract accounting categories; separate physical counters are not
required. Live charge belongs to a committed allocation until its return is
published, including a final drop stopped before publication. Pending return
means published retirement charge that has not yet been refunded.

| Transition | Required accounting effect |
| --- | --- |
| Admit a request | Atomically test and reserve the full selected slot charge before claiming a slot; concurrent requests cannot spend the same headroom. |
| Expose a successful lease | Move its charge from pending acquire to live without changing admitted bytes. |
| Publish final return | Move live charge to pending return; do not refund it merely because the reference count reached zero. |
| Complete return | Publish the class capacity, then refund the original domain charge exactly once. |
| Fail acquisition | Relinquish any slot claim and restore its class credit, then refund this attempt's charge before returning the error. |

A published class credit can be consumed before the old charge refund completes.
The retiring allocation's record must remain valid independently of slot reuse.
Its pending charge is conservative admission accounting, not another physical
reservation. Completing a drop requires both effects, as specified in PROG-03.

The operation retains a root lifetime claim throughout admission, rollback, and
return. Partial operations cannot report refunded resources they still own.
Helping unrelated operations may change global counters during a failed request;
rollback restores this request's resources, not a historical global snapshot.

### MEM-03: Origin, limits, and observability

An allocation has one immutable origin domain until final return. Moving handles
between tasks or threads does not move its charge. Clone and slice retain the full
charge: a 128-byte view of a 2 MiB slot still retains 2 MiB. Empty views also retain
their allocation. Explicit copies require admission for both source and destination
during their overlap; automatic copying or compaction is excluded.

A **limit** bounds admission. A **reservation** dedicates a resource. **Availability**
means a particular request can currently be served. The default policy promises
limits, not dedicated reservations or guaranteed availability. Another domain may
occupy the requested class while this domain remains below its byte limit. A slow consumer
cannot authorize growth beyond these bounds, but may affect other users of shared
class capacity.

Allocator-owned metadata is bounded; external queue entries and tasks require
their own limits even if a million handles share one backing allocation. Baseline
observability covers reservation, admission categories, slot use, and failures by
reason. Cross-counter samples must state their consistency; they are not implicitly
atomic snapshots. Exact consumer attribution requires a separate, explicitly costed
tracking mechanism.

Publish the selected class schedule and its rounding bound. A relation such as
`requested <= class_size < 2 * requested` applies only to a schedule and request
range that satisfies it. It does not bound total reserved bytes by logical payload
after slicing or preparation. Report rounding, retained backing, and idle storage
separately.

## Progress model

### PROG-01: Capacity credits and transitions

For each class of fixed capacity `N`, define nonnegative abstract counts:

```text
N = available_credits + acquiring_credits + issued_credits + returning_credits
```

Credits represent rights to class capacity, not a simultaneous count of physically
free leaf bits. A routing counter must not be exposed as exact `free_slots` merely
because it is near the top of a tree.

| Transition | Preconditions and effect |
| --- | --- |
| Reserve class capacity | Admission is registered and charged; move one available credit to acquiring. |
| Commit acquisition | One slot is exclusively claimed and its metadata is ready; move acquiring to issued before exposing the lease. |
| Abort acquisition | Release any concrete claim, then move acquiring back to available; finish MEM-02 rollback before returning. |
| Publish return | Final ownership has ended; publish a discoverable helpable record and move issued to returning. |
| Publish capacity | Make the slot safely reusable and its credit available exactly once; move returning to available. |

Non-final drops, freeze, and successful view creation do not change class credits.
Quota refunds and helper lifetime cleanup following capacity publication do not
create further class credits. They remain part of completing the return.

Each acquire can hold at most one class credit and its selected slot charge. A
stopped acquire may retain its own admission, but cannot prevent use of other
published, unreserved credits. Before return publication, a stopped final drop
still owns the obligation to publish; its issued credit and live charge remain.

The implementation must identify the linearization points or explicit staged
effects corresponding to these transitions. Independently sampled counters are
not sufficient evidence for the conservation law or an exhaustion decision.

### PROG-02: Acquisition and failure semantics

The core offers one synchronous, bounded `try_acquire(size)` attempt. Select the
smallest configured class that fits; do not silently try larger classes or allocate
outside prepared storage. Reject zero-sized requests as `InvalidSize`; unsupported
sizes and unrepresentable rounding are `TooLarge`. These validations have no
admission side effects.

| Result | Required meaning |
| --- | --- |
| Success | A unique mutable lease with one class credit and one origin charge. |
| `BudgetExceeded` | A validated admission observation shows the charge would exceed the domain limit, including pending charges. |
| `ClassDepleted` | One consistent observation establishes available, acquiring, and returning credits are all zero for the selected class. |
| `Contended` | The bounded attempt could not complete, or unfinished class-credit transitions prevent confirming depletion; usable capacity may exist. |
| `Closed` | The root or domain admission gate rejected this operation under LIFE-01. |
| `ReferenceLimit` | Creating the admission lifetime claim would exceed a checked ownership-count limit. |
| `TooLarge` / `InvalidSize` | The request is outside the supported size domain. |

After size validation, an error must have a valid observation or failed attempt
within the call. It need not represent the state at return, and competing failure
conditions have no fixed precedence. Do not replace these distinctions with an
undocumented `Exhausted`. Contention is not evidence of memory exhaustion.

Errors do not panic, wait, use heap fallback, or start an implicit retry loop.
Before returning an error, restore every resource acquired by this attempt.
Waiting, retries, eviction, deadlines, fairness, and disconnect policy belong to
the caller. They cannot revoke live ownership.

Exact `ClassDepleted` requires a coherent observation, not separate atomic loads.
This diagnostic guarantee can constrain the eventual class-state representation;
the reference mutex supplies a snapshot but proves no production snapshot protocol.

### PROG-03: Helping and completed-drop postconditions

Return publication is the point at which other participants can discover enough
stable state to finish the return without its original thread. Setting a local
flag or leaf bit alone does not establish discoverability. A publication protocol
must cover interruption at every intermediate step.

After publication, helpers can complete capacity publication and the origin refund.
Every effect occurs exactly once, including when two helpers race or the original
thread resumes. A stopped helper must not become a new exclusive owner that
everyone else must await. Stale helpers cannot access freed metadata or modify a
subsequent allocation after slot reuse.

Helping provides **system progress**, with bounded work per call:

- If no new returns are published, continuing applicable calls must finish the
  finite set of published returns without the publishing threads resuming.
- Applicable calls include acquisitions servicing that class or its outstanding
  domain refunds, and owner-side drain work after admission closes.
- A quota failure must not permanently bypass help needed to refund a published
  return. Temporary `Contended` is allowed; repeated failures alone do not count
  as useful progress.
- When a request remains admissible and an unreserved credit remains available,
  continuing attempts must eventually obtain a slot despite stopped peers.
  Under ongoing competition, system-wide progress does not promise success or
  fairness for a particular caller.

**On completion of the last buffer `Drop`, its slot has been safely returned,
its class credit published, and its origin charge refunded.** Another operation
may already have consumed the returned credit. Completing drop by merely queuing
work for a future helper is forbidden. Help concerns interrupted operations; it
does not weaken this completed-call postcondition. Non-final drop only relinquishes
its reference.

### PROG-04: Work bounds and algorithm acceptance

Prepared acquire, freeze, try-clone, try-slice, and buffer release perform no system
allocation/free, mapping changes, collection growth, or per-view heap allocation.
This includes errors, helping, and first return on a previously uninvolved thread.
No hidden TLS initialization or participant registration allocation is permitted.
Shared v0 is participantless: arbitrary existing Rust threads can receive and
return buffers without registering a participant or installing a magazine.

Single-buffer operations require a fixed bound on ABPS's own work and synchronization
attempts, independent of configured slot count and competing thread count. State
the bound for success, failure, rollback, and helping before accepting an algorithm.
Scanning all slots, an unbounded CAS loop, or waiting for one participant does not
satisfy it. Producing `k` descriptors costs O(k); explicitly copying or initializing
`n` bytes costs O(n).

System progress of helping does not by itself prove bounded completion of the
non-fallible final Drop. Prove that postcondition and its work bound separately;
Drop cannot return a contention error or leave its return for a future caller.

Drop also performs no blocking, syscalls, allocator trimming, payload zeroing,
compaction, external callbacks, or unbounded waking. Storage destruction belongs
to LIFE-02. ABPS does not guarantee wait-free platform atomics, page residency,
scheduler service, or a hard wall-clock bound. Rust's
[atomic portability contract](https://doc.rust-lang.org/std/sync/atomic/index.html#portability)
does not guarantee wait-free atomic primitives; count library work separately.

**The counter tree is an unaccepted candidate.** Fixed depth alone proves neither
exact accounting nor helping. A return stopped after making a leaf free but before
publishing a top credit can leave physical free space absent from that counter.
An existing credit may even be consumed to acquire that newly free slot. Zero at
the top therefore cannot alone justify `ClassDepleted`.

Before production Shared implementation, the algorithm decision here must include:

- Refinement of MEM-02 and PROG-01, linearization/effect points, all failure rollbacks,
  and a mapping from implementation state to credits and charges.
- Discoverability and progress with an acquire, publisher, or helper stopped at
  every step, including closing and participant/descriptor limits.
- Exactly-once effects: separate “increment credit” and “mark done” steps leave a
  loss/double-credit window unless a proven protocol resolves it.
- Metadata bounds, safe descriptor reuse, stale-helper/ABA protection, and explicit
  finite-counter limit behavior; never assume wraparound cannot occur.
- Memory-order proofs for producer publication and for final-reader completion
  before overwrite; plus the work bounds for each complete operation.

Small sequentially consistent state explorations discussed during design are
hypothesis checks only. They do not establish these obligations, Rust soundness,
weak-memory correctness, or production performance. No implementation-local
fallback may close a missing proof. If a requirement proves incompatible with a
candidate, revise the design explicitly before implementation.

## Ownership and safety

### OWN-01: Exclusive mutation and initialization

The ownership states are:

```text
free -> BufMut -> freeze(self) -> Buf -> final return -> free
```

Dropping `BufMut` directly also returns its allocation. Lifetimes are independent;
there is no arena reset that invalidates other live buffers. Freeze consumes the
mutable handle and leaves no writable aliases.

Maintain `filled_len <= initialized_len <= capacity`. Fresh leases start with
zero filled and initialized lengths even if old bytes remain in storage. Safe
payload reads, views, formatting, and I/O expose only filled bytes. Any explicitly
exposed initialized working region must first be established for the current lease.
Safe initialization helpers establish their own evidence; an unchecked assertion
of initialization requires an unsafe contract. Mere caller-supplied lengths cannot
make uninitialized memory readable.

Recycling does not zero-fill. Requested initialization and copies have visible
byte-proportional cost. Initialized padding is not automatically logical payload.

### OWN-02: Views and finite ownership counts

v0 uses fallible `try_clone` and `try_slice`, without a standard `Clone` promise.
Success adds one reference with no backing copy or charge change. A reached count
limit returns `ReferenceLimit`; a failed bounded concurrent attempt returns
`Contended`. Failure leaves ownership unchanged. Invalid or overflowing view
ranges return `InvalidRange`; views are restricted to filled bytes. Empty views
and an empty frozen buffer retain the original allocation.

No slot can be reused while any owner, view, or external I/O still retains it.
Refcounts cannot wrap, underflow, or authorize double return. Publish finite limits
and their checked behavior with the API. Generations are not mandatory fields;
introduce them only for a demonstrated protocol need with safe overflow behavior.

### OWN-03: Local safety proofs

Keep unsafe code and fields affecting its invariants inside a small private core.
Safe code that can violate those invariants belongs to the trusted boundary too.
Enable `#![deny(unsafe_op_in_unsafe_fn)]`. Every unsafe block or implementation
needs concrete obligations for the relevant provenance, range, initialization,
aliasing, lifetime, synchronization, and unwind behavior.

Prove disjoint slot access without creating an intermediate mutable reference to
the whole slab while neighboring slots are borrowed. `UnsafeCell` does not waive
reference uniqueness or race requirements. Prove both writes-before-consumer-reads
and final-reader-completion-before-reuse; a reference decrement alone is not a
complete reuse proof. A stopped or stale helper must also be covered.

An access that lets another thread free storage, such as a final refund or root
claim release, borrows only atomic words. Ending explicit reads/writes is not
sufficient: a live call's protected borrow must not cover non-atomic bytes that
the owner may deallocate, including padding in an aligned record. The terminal
borrow must exclude those bytes; see the [final-refund correction](docs/final-refund.md)
and the related [Rust lifetime discussion](https://github.com/rust-lang/rust/issues/55005).
Loom checks the atomic protocol but does not model raw storage borrow protectors.
Miri regressions exercise this obligation with many teardown races per seed.
Source or toolchain changes can change a seed's schedule; a directed control
must pass every selected unmodified baseline and detect the mutation under at
least one selected schedule. A surviving set remains unverified, and a timeout
or build failure never becomes a successful detection.

Never call user code, foreign destructors, allocation, formatting, logging, exporters, or
potentially panicking work while safety invariants are broken. Each unsafe change
must name the state transition it protects and include a regression that detects
its violation; a long generic SAFETY comment is insufficient.

Follow the Rust documentation on [module privacy](https://doc.rust-lang.org/stable/nomicon/working-with-unsafe.html),
[unwind safety](https://doc.rust-lang.org/nomicon/exception-safety.html),
[`MaybeUninit`](https://doc.rust-lang.org/std/mem/union.MaybeUninit.html),
[`UnsafeCell`](https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html), and
[final-reference synchronization](https://doc.rust-lang.org/nomicon/arc-mutex/arc-drop.html).
These references inform obligations; they do not prove ABPS's algorithm.

## Lifecycle model

### LIFE-01: Admission, close, and drain

One cold-path `StorageOwner` owns an arena independently of public pool/domain
handles. Buffers may outlive those handles. The root lifecycle is:

```text
Prepared -> Active -> Closing -> Drained -> Destroyed
                           \-> Abandoned (owner dropped before drain)
```

Preparation fixes storage, metadata capacity, and classes; failure cleans up
on the cold path without exposing handles. Activation opens admission. Root and
domain close operations are idempotent and prevent later registration through
their respective gates.

Acquisition registers a root lifetime claim through the root gate, then reserves
its charge through the origin domain gate, then claims class capacity. Each gate
decision is synchronized with that gate's close. A registration completed before
close may continue through subsequent stages; root close need not invalidate a
previous root registration. A domain closed before domain admission still rejects
that request. An operation registered through both gates may succeed after close.
This prevents storage destruction while an overlapping operation finishes or rolls
back.

Close preserves reads, view creation, and returns for existing buffers. It does
not revoke ownership or wait for drain. A closed domain's retained charges remain
valid and can keep it from draining. The root becomes Drained only when no public
handles, allocation ownership, admission/rollback operations, or helpers can still
access its state, and all returns and refunds are complete. The owner itself is
excluded from this outstanding set. Drain work can help published returns even
when new acquisition is closed; it cannot reclaim resources still owned by a
stopped pre-publication operation.

Domains lease preallocated records through a unique `DomainOwner` and separate
admission handles. Owner close or Drop closes that domain, even if handles remain.
Existing buffers remain readable. Its lifecycle is:

```text
Free -> Active -> Closing -> Drained -> Reusable -> Active (new identity)
                                      \-> Retired (generation exhausted)
```

Reuse requires no owner, handles, allocations, or pending operations/refunds;
`admitted == 0` alone is insufficient. A private checked generation distinguishes
incarnations; it never wraps. An exhausted record is permanently Retired and remains
visible in domain-capacity accounting. No generation permits reuse over an old
live buffer. Leasing fails with `DomainCapacityExhausted` if no record is available.
Limits cannot change during an incarnation. Cross-domain transfer is deferred.

### LIFE-02: Destruction, abandonment, and forgotten handles

An explicit owner-side `try_destroy` closes admission and attempts drain. If
outstanding access remains, it reports `Busy` and retains the owner; it never
forcibly frees storage. Actual region/metadata destruction occurs only on this
cold owner path after Drained.

Owner drop first closes admission and checks drain. If no outstanding access or
unfinished return remains, it reaches Drained and destroys storage on this cold
path. Otherwise it makes the arena Abandoned: retain its storage and root
reservation until process exit, even if every remaining handle later drops. Those
drops still complete their returns and refunds, but none inherits physical teardown.
Do not silently replace this policy with final-buffer deallocation or an unbounded
retirement queue.

Abandonment is a catastrophic-but-safe terminal condition. Expose an external
diagnostic snapshot with `abps_abandoned_roots`, `abps_abandoned_bytes`, root identity,
reservation, and outstanding ownership. Keep diagnostics bounded and usable after
owner destruction; adapters must surface the event. A nonzero root identity is an
opaque process-local value assigned once at preparation, never an address and
never reused after destruction. Exhaustion reports 0 (unassigned) rather than
wrapping; zero can recur and must not be treated as a unique root key. Domain
identities are scoped to their root: collectors use assigned root identity plus
DomainIdentity, and handle unassigned roots without merging their accounting.
These diagnostic values are not persistent or wire identifiers. There is no
implicit process reservation manager: applications bound aggregate roots,
including abandoned ones.
Forgetting an owner runs no destructor and produces no fictitious abandonment event.

A design such as an ordinary `Arc<PoolInner>` is not sufficient if its last buffer
reference would destroy the arena. Storage lifetime, accounting retirement, and
physical teardown need separate proofs, including suspended helpers.

Rust permits [suppressed destructors](https://doc.rust-lang.org/std/mem/fn.forget.html).
Forgetting a buffer retains its charge and prevents its slot from being reissued.
Forgetting a management handle can prevent drain. Forgetting the owner does not
execute close; its storage remains retained. None permits UAF, fictitious refunds,
or forced reuse. Application accounting must include retained and abandoned roots;
creating replacements cannot be presented as freeing their reservations.

## Interop contract

### IO-01: Borrowing, ingress, and foreign layouts

Provide borrowed filled slices, `AsRef<[u8]>`-style access, and `IoSlice`-style
descriptors in caller-provided or otherwise bounded storage. Constructing views and
descriptors does not concatenate payloads. One platform descriptor may cover
fewer bytes than one view (Windows `IoSlice`: `u32::MAX`). The limit applies to the
view's length, not its backing capacity. Infallible `as_io_slice` documents that
panic; `try_as_io_slice` returns `TooLarge`, and callers split views explicitly.
Creating either borrowed descriptor neither copies bytes nor adds ownership.
Owning conversions to foreign buffer types require source review and allocation
measurement: zero-copy does not imply zero-allocation. They are not implicitly
part of the prepared hot-path guarantee.

A generic [`std::io::Read`](https://doc.rust-lang.org/std/io/trait.Read.html)
adapter must explicitly initialize the working region before passing `&mut [u8]`.
A safe `Read` implementation may inspect those bytes; independently validate its
returned length before using it for unsafe access or committing filled data.

A Tokio ingress adapter may use [`ReadBuf`](https://docs.rs/tokio/latest/tokio/io/struct.ReadBuf.html)
and its filled/initialized distinction. It must transfer initialization evidence
correctly and commit only the actual filled range. Keep runtime-specific logic
outside the core.

The byte-buffer contract grants no codec-specific alignment, headroom, tailroom,
or padding guarantee. An adapter must validate and charge its layout requirements;
an arbitrary slice does not inherit a parent's foreign-API suitability.

### IO-02: Egress and completion ownership

Support borrowed vectored output with bounded descriptors and correct partial-write
handling. Descriptor lifetime does not substitute for backing ownership. Retain
the allocation until the external consumer can no longer read or write it.

An adapter must prove when kernel/device access ends. Submission return or future
cancellation alone is not proof. Completion records and intermediate queues must
be bounded; cancellation may transfer ownership to a completion path, never recycle
early. v0 includes no networking runtime or kernel zero-copy-send promise.

### IO-03: First vertical integration and pipeline headroom

Validate real Tokio TCP ingress, ABPS ownership, a bounded queue across workers,
a real consumer/egress, and reuse. Bound queue items and tasks separately from
payload admission. Record allocations remaining in the core, adapter, queue, and
runtime separately, plus exhaustion handling and close/drain behavior.

Budget the simultaneous working set:

```text
retained input + output under construction + queued output
+ in-flight I/O + explicit copy/transform headroom
```

The integration must exercise input occupying all available budget while a transform
needs output. Use explicit output headroom, full-step admission, or a documented
failure policy; do not wait indefinitely for memory held by that same step. ABPS
guarantees bounded admission, not deadlock freedom, acceptable video delay, or the
choice of consumer to disconnect.

## Possible extensions

Extensions must preserve the contracts above and expose their costs. Local mode
would use non-atomic ownership and `!Send + !Sync` handles. Hierarchical budgets
and cross-domain charge transfer need separate accounting and progress decisions.
Hard class reservations are authorized only for the SI-05 experiment below; a
multi-issuer extension still requires its own protocol. No parent-counter tree
is implied by the flat v0 model.

Magazines, owning chains, explicit detach/compaction, oversized storage, and async
waiting need bounded metadata and failure semantics. Security wiping requires an
explicit cost and execution location; its name does not authorize payload-sized
work in buffer Drop. Huge pages, NUMA, registered buffers, platform mapping, and
hardware surfaces need isolated backend/adapter contracts. Keep slot IDs,
descriptor layouts, storage traits, and refcount representation private.

## Reference backend scope

The authorized first spike is the repository's single, unpublished library crate with
`#![forbid(unsafe_code)]` in its reference module. It tests the four ledgers, bounded domain-record leasing,
fallible views, exact depletion, origin identity, and owner-side teardown. Payload
boxes are prepared once and move through mutable and immutable ownership without
payload copying. Root/slot identifiers and generations remain opaque.

This backend deliberately uses a mutex and `Arc` wrappers. Try operations use
`try_lock` and can return `Contended`; freeze, close, snapshots, and Drop can block.
Metadata wrappers allocate/free, and cold snapshots allocate. Its reservation
ledger charges an explicit metadata model, not all physical metadata or preparation
peaks. These are reference-only exceptions to MEM-01 and PROG-03/04, not a fallback,
an accepted Shared algorithm, a helping proof, or a latency/allocation claim.
No production lock-free, unsafe, Miri, or Loom evidence is implied by its tests.

The Tokio experiment uses twenty bounded subscriber queues. Queue overflow
disconnects the subscriber by default; `--overflow=skip` explicitly skips a whole
new frame before enqueue. Already accepted/in-flight ownership cannot be revoked.
Prepare all fan-out views before any enqueue, distinguish view errors from queue
overflow, and allow at most three application attempts on `Contended`, yielding
between them. Exhausted retries terminate ingress with a distinct reason.
Cancellation must finish task destruction before drain. Regular TCP writes borrow
memory only during the call; this does not generalize to registered/zero-copy I/O.

Report remaining allocations and fallibility ergonomics in the
[spike report](docs/reference-spike.md). Keep this canonical specification
in CONTRIBUTING during the spike; extraction to SPEC.md is a pre-release task.

## Berry: single-issuer experiment

The `berry-spike` feature authorizes a separate, unpublished backend under
`berry`; the safe reference remains the semantic oracle. This is a deliberate
execution-model change, not acceptance of the multi-issuer Shared algorithm above.
The following decisions specialize MEM/PROG/OWN/LIFE for this experiment:

- One worker creates and owns each root and all admission/control handles.
  `StorageOwner`, `Pool`, `DomainOwner`, and `Domain` are neither Send nor Sync.
  `BufMut` remains exclusive, Send, not Sync/Clone; `Buf` is Send + Sync. Buffer
  recipients need no registration, TLS state, or background return service.
- A root supports 1..=64 classes, each with 1..=usize::BITS slots. Each class has
  exactly one atomic free bitmap. Invalid configurations fail at preparation;
  there is no multi-word scan, counter tree, larger-class spill, or heap fallback.
- Roots and their aggregate reservation are bounded by the application. A stream
  stays on its selected root. Idle capacity in another root is unavailable;
  replacement and abandoned roots still consume the aggregate reservation.
- PROG-03 helping is intentionally absent. A stopped issuer stops admission on its
  worker. A stopped return retains its remaining charge/allocation/root claims
  until its own thread resumes, and can delay drain. Completed final Drop still
  publishes capacity, refunds the origin, and ends its lifetime claim. Merely
  enqueuing a future return is not completion. No buffering operation blocks.

### SI-01: Bitmap admission and publication

The sole issuer checks both lifecycle gates, pins the root, and reserves the full
class charge with one compare-exchange attempt against the domain admission word.
A failed CAS is Contended, even when only a concurrent remote refund changed the
word far below the limit; an observed insufficient limit is BudgetExceeded.
After admission, first load the bitmap and intersect the eligible mask. A positive
hint can be claimed immediately. A zero hint is not a depletion proof: confirm it
with exactly one Acquire fetch-or-zero, intersecting the same mask. A positive
confirmation is claimed; a zero confirmation returns exact ClassDepleted in
Unreserved mode or DomainClassDepleted in Reserved mode. An empty grant mask
requires no bitmap access because it grants no capacity. Budget error precedence
is unchanged, including for empty grants.

The confirming RMW reads the immediately preceding modification-order value and
is the empty observation for this attempt, not a promise about the state at return.
There is no other issuer with an acquiring credit; publication is one atomic
returning-to-available effect. Only this issuer removes bits, so a positive hint
cannot be stolen before trailing-zero selection and an Acquire fetch-and claim.
That claim pairs with release publication before overwrite. Neither stale loads
nor changing a load to Acquire replaces confirmation. Bounds/range checks precede
admission; each failed attempt rolls back its own resources. There is no retry.

The successful bit clear moves available to acquiring. Installing origin and
ownership metadata commits acquiring to issued before exposing the lease. A last
reader first finishes release/acquire refcount synchronization, then copies every
return operand out of the slot. Its bitmap fetch-or combines issued -> returning
-> available into one effect; there is no persistent returning credit. Until that
publication, a stopped final Drop still owns an issued credit and live charge.
After publication, its old charge is pending return even if the slot is reissued.
The returning thread never accesses reusable slot metadata again: it refunds the
saved domain, decrements that domain's allocation claims, and releases the root
claim last. The domain and root stay pinned through those actions, which borrow
only these atomic words (OWN-03). This permits reuse-before-refund without
letting an old return affect a new incarnation.

The abstract admission categories remain those of MEM-02. Physical counters are
admitted bytes and allocation claims; neither a bitmap nor a diagnostic sum is a
replacement byte budget. Release publishes domain completion before the issuer's
acquire observation authorizes record reuse. When the last domain control handle
ends, the issuer checks that record only: if drained, recycle it immediately;
otherwise enqueue it once in a prepared issuer-owned FIFO. Remote returns never
touch this FIFO. `poll_domains(max_records)` examines at most the smaller of its
argument and the entry-time FIFO length, reclaims drained records, and rotates
unfinished ones. It returns examined/reclaimed/pending counts. Forgotten ownership
can leave a candidate pending forever, without preventing other candidates' reuse.

`try_open_domain` only pops the prepared free stack. An empty stack with candidates
returns `DomainMaintenanceRequired`; an empty stack without candidates returns
`DomainCapacityExhausted`. Maintenance does not promise eventual availability.
Snapshots never run maintenance. The experiment's explicit record maintenance is
a specialization of LIFE-01, not a deferred payload return or refund. Pending
records do not independently pin storage; cold destruction still uses root claims.
FIFO metadata is reserved under MEM-01. Generations retire rather than wrap.

For Berry, the two loads in `DomainLedger::drained()` are justified by the
closed-domain transition, not by an atomic snapshot. The same sequential issuer
finishes or rolls back every admission before closing; no pending issuer commit
or new admission may increase either ledger until recycle. Both counters then
only decrease. Zero allocation claims acquired before zero admitted bytes permit
reuse of the record, even if an old return still owes its final root decrement:
that return must perform no further domain access. Mosaic instead uses its final
admitted-byte refund as the completion/lifetime edge; it has no allocation counter.

### SI-02: Ownership, storage, and work bounds

Prepare stable payload and metadata, with checked layouts and reservation for all
owned allocations and padding. Preparation borrows configuration rather than
retaining an unbounded input Vec. Allocation-system overhead is outside MEM-01.
One root lifetime claim covers an allocation through its final return; views do
not increment a global root count. Finite claims are checked by the sole issuer;
concurrent remote operations only decrement those claims. Only cold owner teardown
frees storage. A bounded independent diagnostics record can outlive that storage;
its own final observer release is cold work, never buffer work.

A slot-local checked CAS creates a view; failure leaves refcount unchanged. Freeze
consumes mutation rights without copying or allocating. Refcount decrement uses
release ordering and final-reader acquire synchronization. Safe reads expose only
filled bytes, with fresh filled/initialized lengths zero. No recycle zeroing or
unchecked safe initialization assertion is introduced.

The optional `bytes-interop` feature implements `bytes::BufMut` for the mutable
Berry lease, without an owning conversion to `Bytes`. Its writable chunk
stays within the slot and forbids safe de-initialization; checked cursor advancement
commits bytes initialized by the caller of that unsafe trait operation. Safe Tokio
`read_buf` adapters may use a length-limited cursor. Partial reads commit only their
filled prefix; generic `std::io::Read` still requires initialized writable memory.
The default core has neither a bytes nor a Tokio dependency.

Class lookup takes at most seven comparisons for the 64-class ceiling. Buffer
operations have no retry loops. Quota admission uses one CAS. Capacity acquisition
uses one bitmap RMW on a positive first hint, two on a positive confirmation, or
one on confirmed depletion; an empty grant needs none. Including root pin, quota,
commit or refund and root rollback, ordinary acquire uses at most five RMWs
(four on the fast successful path). Rollback after a bit claim adds three RMWs
instead of the successful allocation-claim commit, for at most seven total.
View creation uses one CAS, and nonfinal release one decrement. Final
release additionally performs one bitmap publication, one charge refund, one
domain-claim decrement, and one root-claim decrement, plus an acquire fence. Record
actual load/store/RMW bounds with the implementation, including rollback. Platform
atomics and scheduling still have no hard wall-clock guarantee.

Cheap capacity/domain metrics do not allocate or scan the domain/slot tables.
Full snapshots allocate on the cold path and are exact only at quiescence; samples
across independently changing counters are not atomic snapshots. Failure reasons
are returned to caller instrumentation without introducing global hot counters.

### SI-03: Acceptance and measurement

Model the actual atomic protocol with Loom, including publication before refund,
refcount races, stopped returns, close/drain, and root/domain completion. Use Miri
and semantic slot-boundary checks for unsafe storage. Compare both backends with
an independent ledger oracle. Allocation instrumentation must detect an intentional
positive control and observe zero alloc/free/realloc on prepared buffer paths,
including failures and the first return from an unrelated thread.

Separate load generation from server CPU resources. Compare shared-root reference,
root-per-worker reference, this backend, and budgeted Bytes with equal total class
capacity and policies. Multi-issuer access to one experimental root is unsupported,
not an error-rate measurement. Include skew/stranded capacity, remote final drops,
fan-out, small retained views, headroom, and monitor cost. This feature remains an
experiment; its tests do not establish the original multi-issuer helping contract.

### SI-04: Pipeline admission and measurement adapters

The TCP experiment reports a cold `PipelinePlan`: per-root classes, domain limits,
ingress/output headroom, conservative queue and in-flight backing bounds, and
separate adapter metadata/scratch bounds. Independent subscribers can retain
different frames; their queues must not be assumed to share every backing. These
estimates grant no availability guarantee; SI-05 reservations are separately explicit.

After a valid header, reserve available subscriber queue permits before acquiring
payload. Prepare views only for those permits, before publishing any view.
Disconnect remains the default: a full queue disconnects its subscriber; an
unrecoverable admission error stops that ingress with its exact reason. Explicit
Skip also handles BudgetExceeded, ClassDepleted, DomainClassDepleted, and Contended
after at most three adapter attempts: discard that frame and continue. Unread payload is drained via
a prepared scratch buffer of at most 16 KiB per stream. EOF, error, or cancellation
within a frame terminates the connection. Closed and ReferenceLimit are not memory
overload skips. Partial views/permits/leases are released by RAII. No core retry,
waiting, allocation fallback, or forced reclaim is introduced.

Writers default to one frame per write batch. An explicit batch of up to eight
already queued frames uses at most sixteen borrowed descriptors, without waiting
to coalesce arrivals. Account the batch as in-flight ownership, handle partial
writes, and drop completed frames before finishing the remaining batch.

Pacing runs separately from client I/O and server execution. Per-stream bounded
source queues prevent a blocked connection from stopping other streams' offered
schedule. Distinguish pacing misses, source queue refusals, source write failures,
ingress discards, subscriber drops, and delivery; all remain in the offered-load
denominator. Size sequences are per-stream and reproducibly seeded, independent
of routing. Native timings, allocation instrumentation, and tracing are separate
runs. Include working-set, concurrent-view, and domain-churn cases, not only
immediate reuse of one hot slot. CPU affinity is not a hardware-isolation promise.

### SI-05: Hard class reservations (reference and Berry)

`Config::capacity_policy` is fixed at preparation: `Unreserved` is the default;
`Reserved` dedicates class slots to domain incarnations. The two policies never
mix inside one root. `try_open_domain(limit)` is valid only for Unreserved roots;
`try_open_reserved_domain(limit, &[ClassReservation { capacity, slots }])` only
for Reserved roots. A policy mismatch is `InvalidConfiguration`. Grants name
existing classes in strictly increasing order with positive counts; an empty
grant is valid and admits no slots. No grant silently spills into another class.

Opening preflights every grant and commits all or none. Insufficient unassigned
class capacity is `ReservationUnavailable`, without consuming a domain record,
root claims, or any partial grant. Slot rights are distinct from byte admission:
limits still charge full backing and pending operations under MEM-02. An idle
reservation cannot be borrowed. In Reserved mode the selected class's unavailable
domain capacity (including an omitted class) is `DomainClassDepleted`; Unreserved
roots retain `ClassDepleted`. These observations do not guarantee future admission.

The single issuer maintains the partition `all bits = unassigned bits + disjoint
domain masks`. Unassigned bits are physically free. A checked, prepared
domain-count by class-count mask matrix exists only in Reserved roots and is
charged as metadata under MEM-01. Acquisition selects `free_bitmap & domain_mask`
with the same single admission CAS and SI-01 confirmation/claim protocol; remote
return remains unchanged.
Grant selection is bounded by configured class count and slots per class. The
safe reference may scan its prepared records under its existing scope exception.

Grants are immutable until domain recycle: closed admission, no control handles,
and all allocation claims and admission drained. Only then does issuer maintenance
return the masks to unassigned capacity, before recycling or retiring the record.
An interrupted refund or forgotten handle pins the old incarnation and its grants.
The final buffer drop does not change grants or defer slot publication/refund.
Reserved `poll_domains(n)` examines at most n entry-time candidates and releases
at most `n * class_count` masks. Adapter convenience admission inspects at most
32 records per call; any further retry is explicit caller work. Cold snapshots
separate physical free, assigned, and unassigned capacity, and domain eligibility.

Private slot metadata may be aligned to 64 bytes to separate independent refcounts.
Layout alignment and padding must be included before root admission. This is a
measured layout choice, not a portable promise about hardware cache-line size;
it neither changes the atomic protocol nor removes shared-backing contention.

### SI-06: Explicit TCP freshness and matched capacity

The reserved TCP profile divides each root's class slots evenly across its streams,
assigning remainders by stream ID. Compare identical grants, total backing, byte
limits, queue bounds, and overload policy in reference, Berry, and budgeted
Bytes. PipelinePlan reports per-domain grants and retention/headroom bounds; a
domain can exhaust its own grant even though another stream is isolated.

`max-frame-age-ms` is optional and disabled by default. The initial explicit
freshness profile uses 100 ms, queue 8, write batch at most 8 already-ready frames,
and Berry read_buf ingress. Its server deadline starts when the complete
frame is ready to publish. Check it before every I/O attempt, even on a ready
socket. Expiry terminates the subscriber connection and releases its writer and
queue ownership; a partial frame cannot be skipped on that connection. A write
completion is not proof of timely network delivery. Cancellation retains the
existing completion-ownership rules.

Clients separately count delivery, timely delivery/bytes, and expired delivery
using planned arrival timestamps; expired deliveries remain in offered-load and
latency results. `send-buffer-bytes` is an explicit adapter socket setting, absent
by default; report requested and actual values. Core semantics remain OS-agnostic.

### SI-07: Bounded diagnostic sampling

Berry `Pool::domain_metrics_page(start, dst)` examines at most `dst.len()`
consecutive prepared domain records, including free records. It returns written,
next_index, and done; start equal to domain_count is complete, a larger start is
InvalidRange, and an empty destination does not advance. Only the written prefix
changes. Samples include incarnation, phase, limit, owner, control handles, admitted
bytes, and allocation claims. `Domain::class_grants_into(dst)` writes one entry per
class in Reserved mode, or zero entries in Unreserved mode. A short Reserved
destination is InvalidSize before any write. Neither operation allocates, scans
slots, changes accounting, nor invokes maintenance. Work is O(destination length)
for the page and O(class count) for grants. Caller-owned output storage is prepared
outside the measured path and is not part of the arena reservation.

Each record is an observation, not an atomic multi-counter snapshot. Pages can
observe different domain incarnations; consumers must retain index and generation.
Identity remains opaque and scoped to its root; collectors keep that root context.
Default DomainMetrics values initialize caller scratch and are not observations.
No diagnostic result authorizes admission or reclaim. A page does not derive
live/pending charge by a hidden slot scan. Full snapshots remain explicit allocating
cold diagnostics with O(domains * classes + slots) work/storage in Reserved mode,
exact only at quiescence. Routine collectors process and reuse bounded pages,
rather than rebuilding the full snapshot in an unbounded collection.

### SI-08: Typed pipeline resource plan

The TCP adapter prepares one typed PipelinePlan before storage, listeners, and
runtimes. Backend classes, domain limits/grants, and reports use that same plan.
`--class-slots=64,64` sets total slots in each of the two classes; roots and domains
receive quotient/remainder shares in their existing deterministic order.
`--domain-limit-bytes=N` overrides the per-domain limit. Without it, retain the
previous `(sum of class capacities) * (64 / workers)` default independently of
configured class slots. All layout and envelope arithmetic is checked.

`--capacity-check=report` is the default: report deficits and permit explicit
overload. `require-envelope` rejects an insufficient plan before resource creation.
For the current TCP pipeline each stream's conservative envelope is
`E = 1 + fanout * (queue_capacity + writer_batch)` backing claims. Each Reserved
class grant must cover E; Unreserved classes must cover the root's summed E.
Each domain limit must cover `E * max_class_capacity`. The one ingress position
covers pending admission; stage occupancy includes destructors and pending refunds
until final Drop completes, not merely until a queue removes an item or the bitmap
publishes a slot. An input/output transform must add its output headroom; the
current TCP adapter performs no such transform. These bounds rely on ownership
remaining inside the stated pipeline stages, not arbitrary external retention.

Both modes reject invalid layouts, including Berry classes exceeding
usize::BITS slots per root. No automatic root growth, counter tree, larger-class
spill, grant borrowing, or byte-limit increase is introduced. A covered envelope
is a static feasibility check, not a promise of successful CAS, scheduler progress,
completed interrupted returns, or timely delivery. Baselines use equal configured
slots, byte limits, grant policy, overload, and freshness.

### SI-09: Transactional linear grant resolution

Berry opening resolves strictly increasing grants against sorted classes
once, using fixed stack scratch for at most 64 private class indices. Work is
O(C + G + sum(requested slots)), including mask selection. Preserve validation and
error order: validate/resolve, Active gate, all grant availability, domain record,
root claims, then commit. Mosaic and the reference oracle report this same order
for one sequential history; only the maintenance outcome is normalized, because
the reference has no pending FIFO. Before commit no rights or records change.
After commit starts there are no fallible operations, allocations, or external
callbacks.
Errors preserve this request's claims and every grant partition. Empty grants
remain valid. Remote Drop and issuer maintenance keep their SI-01/05 protocol;
no grant atomics or extra work is moved onto returners. Reference resolution stays
independent for differential tests.

### SI-10: Transactional batch reference creation

`Buf::try_clone_into(&self, dst: &mut [Option<Buf>])` fills an initially empty
destination with identical immutable views. A nonempty element is OutputNotEmpty
before changing anything. Empty output succeeds without synchronization. For K > 0,
Berry performs checked `old + K <= reference_limit` followed by one CAS;
overflow/limit failure is ReferenceLimit, failed CAS is Contended. On Err the
destination and this operation's ownership effects are unchanged. Work is O(K)
with no allocations, callbacks, or internal retry. During construction,
`refcount = live handles + reserved batch references`. A private guard owns the
unmaterialized references. Unwind clears the constructed prefix and refunds the
remaining batch references while the borrowed original still prevents final return.
On success all references belong to output handles. The ordinary final-return
protocol is unchanged; scalar clone/slice retain their one-attempt semantics.
Reference supplies the same observable operation under its existing mutex scope.

At observable and unwind boundaries, reserved batch rights equal the guard's
`pending` count. During one materialization, a right transfers through a bounded
non-unwinding interval from that count to the installed handle and completed-prefix
count. It remains an owned right during this interval; no additional reference is
created. Destination bounds and the positive-pending assertion precede transfer.
The interval copies only concrete pointer/index/range fields, replaces a known
None destination and advances an index already proved below its slice length.
There is no callback, allocation or fallible operation. Tests inject unwind at
the legal boundaries; a mutation that inserts a panic inside this interval must
expose the resulting lost right. It is a negative control of this proof obligation,
not an assertion that such a panic is possible in the current implementation.

OWN-03 applies across generations: prior readers' Release decrements (including
batch cleanup) precede the last release's Acquire, then Release bitmap publication
and the next issuer's Acquire claim. Only then may `references.begin()` reset the
counter for the next allocation. Old returns use saved operands and cannot access
that reinitialized counter. Visibility models must observe the new generation
before thread join or any extra synchronization with the old returners.

TCP `--fanout-api=scalar|batch` defaults to scalar. Batch first obtains queue permits,
then prepares K-1 new views for K recipients and transfers the original to the last
recipient, before any publication. At most three explicit adapter attempts on
Contended apply to a batch, with yields between attempts. Partial preparations are
never published. This reduces producer reference creation; it does not eliminate
same-backing contention or the per-owner release cost. Promote batch to default
only in a separate decision supported by dedicated-host measurements.

## Mosaic: segmented single-issuer experiment

The optional `mosaic-spike` / `mosaic` backend is a separate
SI-11..15 experiment. The reference and Berry (SI-01..10) implementation remain
independent baselines. Controls are worker-local, immutable buffers cross threads;
there is no participant registration, helping, quota tree, capacity borrowing, spill, hidden
retry, or background retirement. None of these specializations authorizes a change
to the original multi-issuer Shared contract.

### SI-11: Positive charge protects allocation lifetime

Every allocation carries a positive full-slot charge, including an empty frozen
buffer. `admitted = pending_acquire + live + pending_return <= domain_limit`.
Reserve with one checked CAS before claiming capacity. Rollback publishes any
claimed slot before refunding. The last owner performs release/acquire refcount
synchronization, copies all return operands, publishes its bitmap bit with Release,
and refunds admitted with Release as its LAST storage access, borrowing only the
admitted word: the 64-byte-aligned ledger has non-atomic padding (OWN-03). Never
reread reusable slot metadata after publication. Completed Drop has three RMWs;
there is no root or allocation-count RMW. A stopped return pins its remaining
positive charge. Equivalently, admitted is
the sum of the positive capacities of all allocations whose admission/ownership/
return has not completed its final refund, including guards and published returns.
Assertions of equality use quiescence or deliberately paused transitions; ordinary
diagnostic loads do not provide a coherent decomposition.

Control counts are issuer-local and separate; `control_handle_limit` includes the
storage owner, pools and domain controls, not buffers. A closed domain recycles
only without owner/handles and after an Acquire zero admitted observation. The
sole issuer cannot concurrently admit while making this decision. A closed root
is destroyed only with the storage owner as sole control and Acquire zero in
EVERY domain (never a possibly overflowing sum). Positive outstanding charges
then rule out all pending, issued and returning allocations. This is an explicit
specialization of LIFE-01, not a claim that admitted alone suffices in Shared.
Refund release sequences and issuer program order must cover prior incarnations.
Forgetting ownership pins resources; early owner Drop abandons storage permanently
and publishes bounded external diagnostics. Later returns cannot inherit teardown.

### SI-12: Flat words and exact depleted observations

Each of at most 64 classes has 1..=256 atomic bitmap words. Configuration may lower
`max_bitmap_words_per_class` (default/ceiling 256); unused tail bits stay zero.
Only the issuer clears bits; returns only add their saved bit. Grants do not change
during acquisition. Start at an issuer-local cursor, traverse eligible words once
with loads, and claim a found bit with an Acquire fetch-and. Keep the winning word
as cursor. This is locality-oriented reuse, not round-robin allocation or a
fairness guarantee among words, slots or domains. If that pass is empty, confirm
with one Acquire fetch-or-zero per
eligible word. A positive confirmation can be claimed; all-zero confirmations
return exact ClassDepleted or DomainClassDepleted, then refund this request.
Exact means an empty witness during the attempt, not emptiness at function return.
A return into an already confirmed word can legally precede the error response.
There is no persistent returning capacity credit: until publication it is issued,
and publication moves it directly to available, independently of pending refund.

The proof uses monotonic eligible bits during confirmation and the RMW rule that
its read observes the immediately preceding modification. All-zero confirmations
must have a common empty witness; independent stale loads are insufficient. Prove
this for the production weak-memory protocol and test it with Loom. Do not weaken
the diagnostic error to make a candidate pass. Budget error precedence remains.
For W eligible words: <=2W probes, <=W+2 RMWs for an ordinary result; rollback after
claim adds at most two RMWs. Lookup uses <=7 comparisons. Final return stays three
RMWs regardless of W. These bound library work, not machine steps or wall time.

### SI-13: Prepared sparse class grants

Let S be total slots and D domain capacity. Reserved roots prepare S mask nodes
and min(D,S) GrantSet rows, each with one direct entry/cursor per class. A node
contains a nonempty mask for one class word. Empty grants consume no row. Each
active node owns at least one distinct slot right, so nodes <= assigned slots <=S;
nonempty domains <=min(D,S). With a free domain record and enough unassigned rights,
these prepared pools suffice, without a new metadata-exhaustion error.

All valid rights partition into unassigned and disjoint immutable domain grants.
Opening resolves sorted requests once, preflights rights/record/controls and commits
without failure or allocation, choosing lowest unassigned bits. Debug assertions
check cached free-row/node sufficiency before the first mutation. This relies on
the valid partition invariant; it does not promise rollback from corrupted private
state or arbitrary injected panics inside the infallible commit. Each class list is
bounded by W words. Recycle returns nodes, row and rights only after SI-11 drain.
Remote Drop never touches grants. poll_domains(n) examines <=n entry-time records
and <=n*C*W masks; adapters use n<=32. Charge arrays, free stacks and all padding.

### SI-14: Borrowed views and bounded batch construction

Buf::try_view(range) returns BufView<'_>, a checked borrowed initialized byte range
with AsRef<[u8]>, as_io_slice and try_as_io_slice under IO-01's descriptor bounds.
Copy/Clone never touch refcounts; lifetime cannot
exceed its owner. Keep one owner per processing branch and bounded borrowed I/O
descriptors. Independent ownership still requires checked try_clone/try_slice.
Batch validates destination first: K=0 returns immediately, K=1 uses scalar clone,
K>=2 reserves once under the SI-10 unwind guard. Errors leave output unchanged.
Scalar fan-out remains default. SI-14 adds no ownership groups or implicit copies;
the separately enabled BR-01..06 experiment below explicitly permits local branches.

New diagnostics expose admitted and controls, not invented root/allocation counts.
Bitmap pages inspect a caller-bounded consecutive range, without maintenance or
an exact snapshot claim. Full allocating snapshots remain explicitly cold.

Diagnostic work is separate from admission: a domain page is O(page length),
including free records; it does not traverse grant lists. All class grants cost
O(classes) in Berry and O(classes + visited mask nodes) in Mosaic. Unpublished
slots cannot distinguish live ownership from a final return before publication.
Mosaic drain examines up to all prepared domains; both backends' try_destroy also
perform an O(domains) close pass. Physical teardown cost depends on the allocator.
None of these cold operations belongs in a latency-sensitive admission loop.
Accounted arena reservation is neither process RSS nor total process virtual
address space. Lazy can defer first touch; ZeroPayload establishes neither future
residency nor immunity to later page faults.

### SI-15: Independent offered load and checked resource plans

PipelinePlan accepts sorted class capacities and corresponding slot counts through
--class-capacities and --class-slots. Preserve historical defaults. Traffic sizes
are independent of configured classes: legacy profiles retain historical sizes;
synthetic mixed-frames uses 95% 32..96 KiB and 5% 512 KiB..2 MiB, seed 1 by default,
with a reported sequence hash. Boundary traffic targets 64 KiB, 128 KiB and 2 MiB.
Geometric classes are opt-in and never silently increase quotas or borrow rights.
Report requested versus charged bytes and equal-budget counterfactuals.

The selected layout target is 64 streams, two roots, 20 subscribers, queue 8 and
writer batch 8: E=321, 10272 slots/class/root, 161 words on 64-bit. Historical frame
classes require 41.38 GiB backing plus metadata. The layout test validates this without
allocating it; full execution requires an appropriately provisioned host. A covered
envelope is not a progress, successful CAS, or delivery guarantee. Optional read_buf
preserves initialization/slot boundaries; generic Read still needs initialized
writable bytes. Compare equal initialization, capacity and overload policies.

### SI-16: Bounded adapter supervision and recovery

The TCP experiment uses cancellable setup with one overall 30-second deadline,
worker completion messages and a two-second drain deadline, not participant
barriers. Errors, panics and absent peers cancel the cohort. Stop admission before
cancelling ingress, then release task/queue/control ownership and attempt physical
teardown on the issuer. A timeout or Busy is not evidence of destruction. Never
join an unconfirmed worker without a deadline; its root remains charged.

A process-lived adapter supervisor prepares a fixed root-record registry. Reserve
both a record and the complete planned root reservation BEFORE preparation.
Preparing, active, draining, abandoned and unconfirmed roots consume these bounds.
Only confirmed arena destruction followed by release of all adapter-owned diagnostic
handles refunds a record. Generation-tagged completion cannot refund another root.
Abandoned roots remain charged permanently; unconfirmed roots remain charged until
actual destruction is confirmed. Expose copied snapshots, not diagnostic ownership.
These cold supervisor records/locks never participate in buffer Drop. Reference
and Bytes reservation numbers remain explicitly modeled, not physical bounds.

Defaults cover the initial typed resource plan only. Extra root records and byte
headroom for overlap must be configured explicitly. At most one automatic
replacement attempt per logical worker over the supervisor lifetime is allowed.
Attempt failure does not restart the loop. Replacement admits NEW work only on
new endpoints: old connections close, losses remain visible, no reconnect/replay.
The TCP cohort is replaced together when remote-return workers cannot drain. The
recovery example also exercises permanent retention and rejection before prepare.

An issuer maintenance timer runs every 10 ms, examines at most 32 pending records
per tick and skips missed ticks. Idle ingress does not stop maintenance. Snapshots
and domain opening never perform an implicit scan. Contended permits an explicit
bounded retry; depleted resources need a state change, Closed stops admission,
DomainMaintenanceRequired requests maintenance, and Busy retains teardown authority.

### SI-17: Explicit payload preparation

Mosaic Config::preparation defaults to Lazy. ZeroPayload performs an explicit
O(backing_bytes) write pass over every checked payload range before activation,
using private volatile aligned word writes and byte prefix/tail writes. Metadata,
padding, reservation size and allocation count do not change. No whole-arena
mutable reference is created. This is preparation cost, never acquire/recycle/Drop
work. Fresh leases still expose filled_len = initialized_len = 0; stale data is not
made public by preparation. This does not promise resident pages, no future page
faults, zero-on-release security, or hard real-time latency.

## Local branch experiment

The opt-in `local-branches-spike` feature extends Mosaic storage only. Ordinary
Buf, Berry and the safe reference remain independent baselines.
Config::local_branches defaults to None. LocalBranchConfig prepares a fixed
max_branches_per_slot B and a separate local_reference_limit L. Both are positive
and at most isize::MAX; layout arithmetic and the complete root reservation are
checked before allocation. Each slot has B private 64-byte-aligned records, fully
charged as metadata including padding. This placement is an experimental layout,
not a portable cache-line isolation guarantee. No records are acquired dynamically.

### BR-01: Conserved branch rights

BufMut::freeze_branches_into(self, dst) consumes exclusive mutable ownership and
produces K immutable Branch handles into caller-prepared Option slots. Check errors
in order: OutputNotEmpty, BranchCapacityExceeded (including disabled configuration),
ReferenceLimit against the ordinary slot reference limit. K=0 consumes/releases the
lease without requiring branch configuration. Every error returns the unchanged
BufMut through FreezeBranchesError::into_parts and leaves dst unchanged.

For K>0, while exclusively owned and before publication, replace the single slot
right with K rights using a Relaxed store. Install the private construction guard
while it still owns the source's one right, then transition from source-owned to
batch-reserved to committed. In batch-reserved state,
`global_references = pending_rights + staged_family + installed_families`.
A staged family owns its global right before installation into the output. Each
right becomes exactly one branch; cancellation or unwind clears the staged family
and built output prefix and releases all remaining rights. No callbacks, allocation,
or fallible work may intervene in the transfer of rights into the guard/handles.
The global counter counts branch rights, including construction and pending
release rights, rather than all local handles. Ordinary Buf semantics do not change.

### BR-02: Local ownership and migration

Branch is Send, !Sync and not Clone. Branch::into_local transfers its one right to
a LocalBuf family. LocalBuf is !Send + !Sync; try_clone/try_slice change only the
family's Cell counter, check L without wrapping, and cannot return Contended.
Invalid ranges precede reference-limit errors. Borrowed views create no rights.
LocalBuf::try_into_branch succeeds only at local count one; on failure it returns
the same LocalBuf. A synchronized transfer of the unique Branch may move the family
to a different thread. No independent shared Buf conversion or dynamic branching
of an already published family is provided. Payload is immutable after freeze.

### BR-03: Complete local and global return

Local count 1->0 ends all access to that branch record BEFORE the one global
decrement. Unmaterialized guard rights have no local handles. The existing SI-11
global release/Acquire fence, bitmap publication and final admission refund remain
unchanged. Never touch a slot/branch record after bit publication, or storage after
refund. A stopped family release can retain its global right/charge; completed Drop
must finish its applicable effects. No branch scan is needed for final return.

### BR-04: Fixed metadata and bounded work

Immutable descriptors are initialized during prepare. Only exclusive slot ownership
resets a record's counter; only that branch's current thread changes its local counter. A record is not independently reused: its next
incarnation starts after the whole slot is reclaimed. Freeze/rollback is O(K),
local clone/slice/drop is O(1), and family completion uses the unchanged O(1)
shared return. No TLS, participant registration, hot allocation, retries, copying,
deferred cleanup, or additional work on ordinary Buf acquire/Drop is introduced.

### BR-05: Lifetimes and suppressed destructors

Every family retains one global right until its last local owner disappears.
Forget pins that right, the full origin charge and prepared record indefinitely.
Close preserves reads and local view creation; owner-side teardown requires the
existing complete drain. Owner abandonment cannot move destruction to a branch.
Preserve initialization, disjoint-slot access and exact origin identity on reuse.

### BR-06: Diagnostics and experimental acceptance

SlotSnapshot::references counts global rights, not local handles. Cross-thread
diagnostics must never read a family's non-atomic counter. Keep metadata cost and
retained backing visible without adding instrumentation atomics to the hot path.

TCP --fanout-api=local-branches is explicit and requires the feature and Mosaic
backend. Queue permits precede acquisition; all branches are built before enqueue.
Queues carry Send Branch handles; writers bind them inside their existing LocalSet.
Keep default scalar, bounded batching, deadlines, overload and cancellation intact.

Model actual global and local transitions with Loom, including unique migration,
stopped releases and reuse before refund; use Miri for the private storage boundary.
Compare scalar shared, batch shared, borrowed and local branches at matched offered
work, fan-out and retention. Include churn, last Drops and complete frame cycles.
Churn improvements do not imply faster one-handle-per-consumer TCP. Publish failures,
tail samples, metadata and drain; dedicated-host evidence is required for promotion.

## Accounting and fan-out hardening

AP-01..09 refine the existing issuer experiments. They do not authorize helping,
multi-issuer admission, quota trees, or a new reclamation policy.

### AP-01: One sequential issuer

Exactly one sequential executor changes admission, controls and grants. Control
handles remain !Send + !Sync, including cloned handles on the same thread. No user
callbacks, logging, formatting or reentrant user code run inside a partial
transition. Remote owners change only the documented atomic protocol.
Mosaic buffer and branch descriptors carry a private remote-access authority,
separate from issuer RootPtr. It exposes no issuer control, domain-state, grant,
cursor or teardown methods. Its typed field projections do not create a reference
to the enclosing issuer Root or DomainRecord. New remote methods must extend the
transitive access audit and retain the ownership/lifetime proof for every field.
Issuer-side snapshots cannot overlap slot issuance or domain recycling. Only the
issuer writes slot origin; remote freeze, reference changes and publication use
atomics. A sampled bitmap followed by an origin read therefore cannot race an
origin rewrite, even when a remote return publishes between those reads.

### AP-02: Publication ends slot access

Copy all return operands before publishing a free bit. Publication permits immediate
reuse; neither slot metadata nor a branch sidecar may be accessed afterwards. The
Mosaic final refund is the last storage access and borrows only the admitted word.
Test reuse before old refund, including reuse of the same sidecar, using the
production transitions.

### AP-03: Lifetime is proved by drain

Positive admitted charge protects a pending acquire, live allocation or unfinished
return. Domain recycle and root teardown use the actual drain predicates, never
diagnostic samples or sums of sampled counters. Berry retains its
separate claims/allocation counters. Both must prove teardown before joining a
returner, without relying on thread join for synchronization.

### AP-04: Completion and interruption

A completed final Drop publishes capacity and refunds its charge. A stopped return
or forgotten owner may retain rights indefinitely; these experiments do not help
it. Cancellation and abandonment never force reclamation. Normal adapter shutdown
stops admission, cancels and drains tasks/queues, performs bounded maintenance and
attempts owner-side destruction. Busy, timeout and abandonment retain supervisor
charges until confirmed destruction and diagnostic release.

### AP-05: Honest diagnostic samples

Experimental allocating snapshots report independently observed values, not a
consistent multi-counter snapshot. Class fields are observed_available_slots and
observed_unpublished_slots; domain fields are observed_admitted_bytes and
observed_unpublished_backing_bytes. Unpublished includes a final return before its
bit publication, even if its refcount is already zero. Never classify admitted
minus unpublished as a pending-return phase. Phase partitions remain available in
the exact safe reference model. Do not add hot-path atomics or read another
thread's Cell to improve diagnostics. Adapter-local contention counters distinguish
admission, scalar reference creation and batch reference creation; their sum is
the aggregate contention count.

### AP-06: Compact local families

One global right covers each local family, including unfinished family release.
Only local count 1->0 releases that right, exactly once. Prepared branch records
hold the local counter and immutable root/slot/payload/capacity/limit descriptor;
reuse initializes only the counter. LocalBuf holds a record pointer and byte range.
Copy the return operands before releasing the right and never touch the record
afterwards. Charge descriptor layout and padding; keep ordinary Buf unchanged.

### AP-07: Explicit worker fan-out and queue credits

The opt-in TCP --fanout-api=worker-branches mode requires Mosaic storage and
local-branches-spike. Its first version uses the existing routing: all subscribers
of a stream reside on one writer worker. Scalar and local-branches remain baselines.
Prepare one sidecar per slot and local reference limit at least the fan-out.

Each subscriber has Q prepared queue credits with this conservation law:
free + reserved_ingress + mailbox + local_queue = Q. Ingress acquires credits
without waiting BEFORE payload admission. One stream mailbox has capacity F*Q;
every bundle holds at least one credit, one Branch, a fixed recipient array,
generation, header and original deadline. Full after successful preflight is an
adapter invariant failure, not a reason to allocate or spill. Closed is handled
as cancellation. Worker-local distribution creates K-1 local views and moves the
original into K bounded queues. Process at most eight bundles before yielding.

Release a queue credit only after ownership transfers to a bounded writer batch;
on cancellation drop its payload ownership before its credit. Stale-generation
messages release their own ownership/credits, never a new generation's resources.
FIFO, Skip/Disconnect policy, eight-frame maximum write batch and deadline checks
before every write attempt remain unchanged. Mailbox occupancy is a subset of
queue credits, so the payload envelope stays 1 + F*(Q + write_batch); mailbox and
credit metadata are modeled separately. Adapter semaphore/channel costs are not
covered by the core's zero-allocation proof. Report dispatched recipient rights,
local enqueues, cancellations before forwarding and final delivery losses.

### AP-08: Preserve bounded attempts and exact errors

Admission keeps one checked CAS and explicit Contended; no hidden retry, larger
class fallback, borrowing, TLS or deferred refund. Mosaic empty search retains
both bounded passes and confirming RMWs. Replacing modulo with a wrap cursor must
preserve visit order, masks, exact depleted proof and <=2W probes.

### AP-09: Comparable evidence

Compare equal workload, quotas, retention, overload and freshness policies. Keep
all rejected views and partially served frames. Separate primitive work, preparation,
materialization, nonfinal Drop, family completion, global final Drop, dispatch and
full frame cycles. Reuse scratch storage outside measured repeats; report overhead
controls without subtracting them. Use deadline/cancellation-bounded coordination.
Physical-core affinity is default; SMT is a separate experiment. Record actual
build flags and source/binary identities, calibrated iterations and at least seven
interleaved repetitions. TCP reports frames per write, syscall/CPU evidence and
queue-credit cost independently of allocator microbenchmarks. VM results are
exploratory; promotion requires a dedicated host and no >5% ordinary-path or p99
regression under comparable policies.

### AP-10: Fixed-arena delivery profiles and local freshness

An explicit fixed-arena TCP profile may increase queue descriptors while preserving
class slots, grants and domain byte limits. It uses `capacity-check=report` and
reports the unchanged conservative SI-08 envelope and every deficit. It grants no
availability guarantee. `require-envelope` and ordinary defaults are unchanged.
Queue metadata and socket buffers remain separate from arena reservation. The
experimental delivery profile targets per-healthy-subscriber p99 <= 5 ms under
its stated offered load; this policy tradeoff does not relax AP-09 for ordinary
paths or constitute a production latency guarantee.

`queue-backend=local` is opt-in, restricted to Mosaic scalar/batch fan-out with
`return=local`. Prepared queues and all their access stay on the same worker;
their handles are !Send + !Sync. `freshness-priority=off|oldest` defaults to off.
Oldest requires the local queue and explicit Skip. `freshness-min-age-ms` defaults
to zero (any age qualifies), accepts 0..3600000, and is meaningful only with
oldest. Reject unsupported combinations before creating resources.

After a valid header, a full local queue may evict at most its oldest queued
whole frame if its server-ready age reaches the configured threshold. Complete
that ownership's Drop before admitting replacement payload. No writer-batch or
partially written frame is eligible; surviving frames retain FIFO order. Eviction
is irreversible even if replacement admission, input, view construction or commit
fails. Count evictions and failed replacements separately, including cancellation.
This policy requires an application that permits whole-frame loss; it does not
understand codec dependencies or retract bytes already accepted by TCP. SI-06
checks before every write attempt remain unchanged.

For each prepared queue, free + ingress_reserved + queued + transferring +
releasing = Q. A dequeue retains its credit until ownership enters a bounded
writer batch. Cancellation/eviction finishes payload Drop before recycling that
credit. Guards restore intermediate rights on unwind. Never retain a queue borrow
across await, payload Drop or wake. Queue operations do bounded work without growth,
locks, payload copies or hidden retries; shutdown drains at most Q entries.
Record occupancy high-water marks, queue age and cancellations without new core
atomics. Delivery reports retain planned arrivals, all loss categories and each
subscriber's latency. A freshness eviction is part of undelivered enqueued work,
not an excuse to remove that work from the denominator.

## Validation policy

Introduce validation with the implementation it checks. No unsafe memory work
without focused Miri coverage; no lock-free algorithm without Loom coverage.
Shared ownership requires concurrency models even if stress runs pass.

### Contract-linked scenarios

| Scenario | Required evidence |
| --- | --- |
| Failure or suspension at every admission/rollback step | MEM-02, PROG-01: no lost charge, duplicated claim, or budget overshoot; failed calls restore their own resources. |
| Suspension at every publication/help/refund step | PROG-03: applicable callers finish published work without the stopped participant; final Drop's postcondition holds. |
| Two helpers race; original publisher resumes; stale helper runs after reuse | PROG-03, OWN-03: exactly-once effects, protected metadata, no mutation of the new allocation. |
| Final drop races with acquire, close, and drain | PROG-01, LIFE-01/02: no early reuse/destruction or lost capacity; gate outcomes follow registration order. |
| Many tiny/empty views, parent dropped first, count limits | MEM-03, OWN-02: full retained charge, valid bytes, checked limits, exactly one final return. |
| Forget buffer/handle/owner, then close; drop owner before drain | LIFE-02: retained resources stay accounted, no forced reclaim, no teardown in a later buffer drop. |
| First return from a new thread; contended and failed paths | PROG-04: no hidden allocation, registration growth, waiting, or unbounded retry. |
| Full input budget with output needed; slow/dead consumer and cancellation | IO-02/03: explicit overload outcome, bounded queues, no impossible wait or premature I/O reuse. |
| Initialization, neighboring live slots, publication, and repeated address reuse | OWN-01/03: valid ranges, no stale/uninitialized reads, disjoint references, correct happens-before edges. |

Also cover zero and maximum sizes, class boundaries and neighbors, `usize::MAX`,
range overflow, empty freeze, direct mutable drop, nested slices, and auto traits.
Verify every counter's finite-limit behavior; test generation limits if introduced.

Use an independent model for generated operation sequences and check bytes, charges,
credits, and ownership after each transition. Preserve minimized failures as
regressions. Model-check interrupted operations and liveness cycles as well as
successful completions: finite stress runs are not progress proofs.

Loom must execute the production synchronization/state-transition protocol through
instrumented primitives; a similar toy algorithm or synchronization hidden in
uninstrumented dependencies is insufficient. State model bounds and limitations.
Miri and ASan do not automatically enforce semantic slot boundaries inside a larger
slab allocation: add range-model checks and canaries alongside those tools.

Instrument all prepared paths for allocations and frees. Give the harness a
positive control with an observable intentional allocation, kept outside the
production measurement. Record violations and assert afterwards; never panic or
unwind inside [`GlobalAlloc` hooks](https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html).
Keep harness allocations out of measured regions.

### Executable verification program

The verification registry in `verification/matrix.json` maps the existing invariant
IDs to executable selectors, features, exploration profiles and intentional-retention
exceptions. [SAFETY_CASE.md](SAFETY_CASE.md) records local proof obligations and
remaining evidence gaps; it is an evidence index, not a competing specification.
The safe reference and both experimental backends share generated traces where
their specified behaviors coincide. Sequential traces require exact errors;
concurrent protocols allow only their documented transient outcomes. Differences
in maintenance, root counters and metadata size are normalized explicitly.

Test-only admission injection runs the real transition after its RAII guard is
installed. A return stopped after publication retains its remaining claims under
SI-01/11 and AP-04; tests must not invent rollback or helping. These exceptions do
not weaken the original multi-issuer PROG-03 target. Persistent fuzz iterations
must drain normally; intentional forget/abandonment runs in separate finite tests.

The ABPS-001..032 review matrix binds scenarios to these existing invariants; it
does not introduce a second behavioral contract. A registered selector or detector
is not execution evidence. Each scenario records its precondition, independent
oracle, backend applicability and negative controls. A control may support several
scenarios for the same transition; it does not establish mutation sensitivity of
every individual test. Missing scenarios or empty detector mappings fail registry
validation. Mutation survivors require review; build failures and timeouts are
incomplete evidence, never killed mutants.

Execution evidence is qualified by source identity, Cargo target, resolved features,
compiler, platform, tool and reference-acquire variant. Resolve selectors in that
same configuration and compare complete test names; substring matches across
targets are not evidence. Both required Loom variants must execute on one source
and compatible build configuration. Record doctest and compile-fail executions,
not only successful compilation. Explicitly declared empty feature-gated targets
are compile evidence only. Historical reports without this identity remain
historical and must not silently satisfy a new campaign.

Publish required campaign steps before running them. Completion requires every
step, an unchanged source, and no failed, incomplete or unsupported command.
An earlier failure cannot be overwritten by later success. Sanitizer findings and
failed test summaries remain failures even on exit code zero. Mandatory registry
and inventory validation must also run under optimized Python. Keep failed and
timed-out mutation logs; only a named detector failure after its unmodified
baseline passed counts as a killed directed mutation.

Concrete issuer histories retain pending domains until an explicit maintenance
action. Preserve the normalized reference replay as a separate comparison, not
an implicit poll in an adversarial history. Replay uses stable logical IDs and a
versioned concrete action trace; shrinking rejects missing dependencies rather
than retargeting handles. Predict expected outcomes from independent ownership,
charge and grant rights, never observed production counters. Structural partition
audits run only in tests with bounded, cycle-detecting traversal. Quiescent snapshots
can check exact ledgers; concurrent samples cannot identify an atomic whole-Drop
transition. Staged-return models distinguish allocation incarnation from slot ID.
Queue-credit tests exercise actual adapter transitions, including batch transfer
and cancellation, independently of payload admission. They are native/sanitizer
evidence, not a Loom model of Tokio internals.

Counterexample shrinking must preserve backend, violated postcondition, operation
and logical ownership identities. Diagnostic wording or the first panic line is
not a defect identity. Preserve an unidentified panic without shrinking. Short
campaigns use deterministic pressure histories to establish that the intended
boundary states were reached, alongside generated histories. Test partial I/O,
wakeups, EOF/errors and cancellation against the real adapters, including exact
wire prefixes and unaffected neighboring ownership. Work-bound checks count
protocol visits; elapsed-time tests do not establish an algorithmic bound.

Native, Loom, Miri, sanitizers and coverage are separate compilations. Small Loom
models explore without permutation/duration/preemption limits; bounded models
report their actual limits. A timeout, branch-limit failure, empty selector,
unavailable tool or incomplete instrumentation is never a passing verification.
Ordinary Miri and leak-sanitizer runs retain leak checking. Leak suppression is
limited to the exact registered intentional-retention tests. TSan cannot validate
atomic fences. For OWN-03, SI-02/11 and BR-03, its instrumented build replaces only
the final reference Acquire fence with an Acquire load of that reference word;
the decrement remains Release. At 1->0 no live or reserved reference can increment
the word, and the slot is not published yet, so the load observes that zero in the
prior readers' RMW release sequences. It precedes publication and final refund.
Ordinary builds retain the fence. This equivalent synchronization edge follows
Rust Arc's sanitizer approach, not an AcqRel upgrade on every decrement.

The private `abps_tsan` cfg is selected from the target's sanitizer configuration
by the build script, with no public feature or runtime switch. Loom executes both
compiled variants separately (forcing this cfg only for the load model). Both
need no-join reuse/drain regressions and negative controls that remove their final
Acquire edge. A TSan pass applies to its load variant; it does not test the native
fence's instrumentation. Record the actual compiled mode, reject sanitizer reports
even on exit code zero, and preserve historical failed evidence. Unexplained
reports remain unresolved, never blanket-suppressed.
MSan requires instrumented dependencies and std. Keep tool versions pinned.

New unsafe transitions require a regression and a safety-case entry; changed
synchronization requires a production-protocol model. Critical transition coverage
and sensitive negative controls gate acceptance; line percentages are diagnostic,
not a substitute. Preserve minimized traces/corpora, source identities, commands,
seeds and incomplete outcomes. Production promotion also requires an independent
unsafe review and dedicated-host performance evidence; CI configuration alone is
not evidence that a campaign ran.

### Intended CI and evidence

These are production validation objectives, not configured GitHub jobs. The
published checkout contains verification scripts and a registry, but no GitHub
Actions workflows. Run the [verification commands](docs/verification.md) locally
for the explicit feature matrix, Loom variants, Miri with exact retention
exceptions, coverage and sanitizers. Historical reports may mention workflows
from their local source snapshots; they do not establish current CI availability.
Each result needs its source, configuration and execution record. A future
configured job would still not be a completed remote run or production approval.

| Frequency | Coverage |
| --- | --- |
| Each implementation change | Formatting, Clippy, deterministic and short property tests, target builds. |
| Pull requests | Focused multi-seed Miri, bounded Loom models, fuzz corpus regressions, supported sanitizers, API compatibility against an established baseline. |
| Scheduled runs | Longer property/fuzz runs, adversarial soak, available cross-target execution. |
| Dedicated performance host | Microbenchmarks, pipeline workload, allocation counts, counters, measured regression thresholds. |

Publish exact commands, features, toolchains, seeds, model bounds, and unsupported
configurations with each harness. `cargo fmt --check`, `cargo clippy`,
`cargo test`, and pinned nightly Miri commands exercise the current crate's
applicable suites. Nightly verification tools do not change the stable shipping
baseline; an available command alone is not execution evidence.

[Miri](https://github.com/rust-lang/miri), [Loom](https://docs.rs/loom/latest/loom/),
[proptest](https://docs.rs/proptest/latest/proptest/), and
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) provide complementary evidence,
not a soundness proof. Use supported Address, Thread, Leak, and suitably instrumented
Memory sanitizer configurations according to the
[Rust sanitizer documentation](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html).

For documentation changes, validate cross-document contracts, invariant references,
links/anchors, fences, and whitespace. Run code checks only when applicable and
report what actually ran.

## Performance and optimization

Measure acquire, failed acquire, release, freeze, try-clone, try-slice, and
acquire/copy/drop, plus a real packet/frame pipeline with size distributions,
keyframe bursts, fan-out, slow consumers, and cross-thread handoff.

The primary baseline is **Bytes + a correct byte budget + bounded queues** under
the same workload, class/retention assumptions, and overload policy. A `Vec`
microbenchmark alone does not establish product value.

The reference [performance audit](docs/performance.md) documents the runnable
timing, allocation and tracing harnesses and their evidence boundaries. It does
not establish production Shared progress or change the reference-only exceptions.

Report offered and accepted load, rejected and overdue work, useful throughput,
end-to-end latency, and p50/p95/p99/p99.9/p99.99 with adequate samples. Include
reservation, admission categories, logical bytes, metadata, fragmentation,
allocation/free counts, and RSS over soak runs. Record cycles, instructions, cache
and branch misses, and atomic counts when available.

A stalled load generator must not erase bad samples: state the arrival schedule,
missed arrivals, and treatment of
[coordinated omission](https://github.com/HdrHistogram/HdrHistogram#corrected-vs-raw-value-recording-calls).
Do not report success-only latency as the complete offered workload.

Use a dedicated repeatable host with recorded CPU, governor, affinity, NUMA,
compiler, and kernel. Set thresholds from measured noise; shared CI runners do not
gate latency. Keep raw results and parameters. Optimize algorithm and copy count
before batching, cache layout, compiler hints, or custom SIMD; every optimization
needs before/after evidence and preserved invariants.

## Portability

The shipping baseline is stable Rust with `std` and portable storage prepared
through standard Rust allocation primitives. Record an MSRV with the crate.
Preserve ownership, budget, and error semantics across supported targets; do not
claim identical atomic hardware behavior or performance. State required atomic
widths with the accepted Shared protocol and reject unsupported configurations
explicitly instead of adding a hidden mutex fallback.

Keep OS-specific `cfg` at backend boundaries. Use checked `usize` arithmetic for
address sizes; validate fixed-width IDs and limits. Test pointer-width and byte-order
assumptions. Optional CPU routines require baseline support and feature dispatch;
`target-cpu=native` is a recorded local choice, not the distribution default.

Publish four independent evidence columns for every target: **builds**, **tests
pass**, **concurrency validated**, and **performance characterized**. A compile-only
target has only build evidence. The imported reports contain historical Linux
x86_64 GNU executions for the experimental backends; upstream's final-refund
review also reports Windows x86_64 Miri observations. Each claim applies only to
its recorded source, tool and target, not an entire platform matrix. Production
acceptance remains open. Reference mutex race tests are not an accepted Shared
concurrency proof or performance evidence.

| Target | Initial validation objective |
| --- | --- |
| Linux x86_64 GNU | First workload and performance characterization; build, tests, and concurrency checks. |
| Linux x86_64 musl; Linux aarch64 GNU | Build and tests; record concurrency and performance evidence separately. |
| Windows x86_64 MSVC; macOS x86_64 and aarch64 | Build and tests; record concurrency and performance evidence separately. |
| Windows aarch64 MSVC | Compile-only initially. |
| A 32-bit target and a big-endian target with required atomics | Compile-only initially; arithmetic/byte-order execution where available. |

Record native versus emulated execution and gaps.
[cross](https://github.com/cross-rs/cross) is an optional tool, not evidence that
these targets have already been validated.

## Contribution and review workflow

Inspect current files, instructions, and working state; preserve unrelated edits.
Keep changes focused on the requested behavior and shared causes.

Each design or implementation change must identify affected invariant IDs, state
transitions, ownership/accounting effects, work bounds, errors, and compatibility.
Unsafe changes also require the local proof and transition regression. Report
actual checks and unverified cases; attach before/after evidence for performance
claims.

Agents and contributors must not resolve unspecified ownership, accounting,
progress, or shutdown semantics by inventing an implementation-local policy.
Missing semantics or failed proof obligations require an explicit design decision
here before dependent implementation. Future ADRs must be linked from this
canonical contract with their adopted status.

Changes in this revision are explicit: Shared is the sole v0 execution model;
work bounds replace wall-clock implications; accounting limits do not grant
availability; helping covers interrupted returns while completed-drop reclamation
remains mandatory; owned storage uses the stated abandonment policy. Preserve
soundness, hard accounting, and the ban on hidden fallback.

Do not weaken a requirement to make code or tests pass. Justify any contract change,
update all affected documents, and distinguish requirements, candidate mechanisms,
performed checks, and verified behavior.
