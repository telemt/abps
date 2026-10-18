# Resources, ownership and lifetime

ABPS is a resource-accounting system that hands out byte buffers. Its useful
property is control over how much state a pipeline may retain, including work
that is still acquiring or returning a buffer. This guide explains the
[canonical contract](../CONTRIBUTING.md); it adds no new invariant IDs.

## Four resource ledgers

| Resource | What holds it | What releases it | Contract |
| --- | --- | --- | --- |
| Reservation | Prepared payload and ABPS-owned metadata, including padding | Owner-side storage destruction; separate diagnostics may survive | MEM-01, LIFE-02 |
| Capacity | One right to issue a particular class slot | Completed capacity publication by the final return or rollback | PROG-01/02, SI-01/12 |
| Admission | The full capacity of each allocation charged to its origin domain | Exactly one refund after capacity publication | MEM-02/03, SI-11 |
| Ownership | Exclusive mutable rights, shared references or local family rights | Drop of each right; only the final right starts slot return | OWN-01/02/03, SI-10, BR-01/03 |

```text
accounted backing + metadata <= root_limit
admitted = pending_acquire + live + pending_return <= domain_limit
class capacity = available + acquiring + issued + returning
```

The class equation names abstract credit states. Implementations need not store
four counters. In Berry/Mosaic, bitmap publication completes the slot-capacity
return even if the old allocation's charge is still pending refund. That old
charge must not also be counted as a second occupied slot. The reference confines
acquiring/returning credit transitions to its mutex critical section, so its
snapshots observe those two fields as zero.

Reservation and admission are constraints on different resources, not two physical
allocations to add together. A free slot remains reserved at its root. An empty
view still retains the allocation's full domain charge. Cloning or transferring
ownership neither copies the payload nor changes its origin.

A **limit** bounds admission. A **class grant** partitions eligible slots under
`CapacityPolicy::Reserved`. **Availability** means this request can acquire an
eligible slot with its required budget. A domain below its limit can still have
no available slot; idle reserved rights cannot be borrowed by a neighbor. Empty
reserved grants are valid and grant no capacity (SI-05/09/13).

## Execution models

The default Reference is a safe mutex-based oracle. Its wrapper allocations,
blocking and modeled metadata are explicit exceptions. Berry and Mosaic use one
sequential issuer per root: control handles are `!Send + !Sync`, while buffers
can return from other threads. Multiple roots have independent issuers.

Berry has one bitmap word per class. Mosaic has multiple words per class and
sparse grant lists; segmentation does not permit competing issuers. Only the
issuer clears eligible bits. Remote returners only publish them, which is a
necessary premise of the bitmap claim and depleted-observation proofs (AP-01,
SI-01/12). A call cannot retain its search hint across another issuer operation.

## Ownership and initialization

The usual path is `acquire -> fill -> freeze -> view/clone -> send -> drop`.
`BufMut` is exclusive; safe reads expose only the filled initialized prefix:

```text
filled_len <= initialized_len <= backing_capacity
```

Reuse resets both lengths. ZeroPayload preparation writes payload during the cold
phase but does not make a newly leased buffer logically filled or initialized.
Lazy preparation may defer page faults; neither policy locks pages in RAM.

An owning `try_clone` or `try_slice` reserves a checked reference. Mosaic's
`try_view` creates a borrowed `BufView` tied to its owner, without a refcount change.
Batch cloning retains its source while a guard owns unmaterialized references;
handles plus pending rights must account for the reserved count (SI-10).

With `local-branches-spike`, each uniquely transferable `Branch` represents one
global right. On a worker, its LocalBuf family creates local owning views with
its own checked Cell counter. The last local right finishes every sidecar access
before releasing the global right. LocalBuf is `!Send + !Sync`; transfer back to
Branch requires unique local ownership. A sidecar cannot outlive its backing right
or be reused while an old family survives (BR-01..05, AP-06).

Queue credits are a separate adapter resource. A queued frame retains both payload
ownership and its subscriber credit. Transfer into a bounded writer batch ends
queue occupancy; cancellation drops payload ownership before refunding the credit.
These rules do not make external queues or tasks part of the core's memory bound.

## Publication, completion and destruction

The following sequence illustrates Mosaic's allowed overlap. Berry also permits
slot reuse before accounting completion, but releases its root claim last.

```mermaid
sequenceDiagram
    participant R as Remote returner
    participant I as Issuer / storage owner
    R->>R: Final reference release; acquire prior readers
    R->>R: Capture old origin and return operands
    R->>I: Release-publish the free bitmap bit
    I->>I: May claim and initialize the slot again
    R->>R: Release-refund old admitted word
    I->>I: Once closed and drained, acquire completion and destroy
    Note over R,I: Returner's stack frame may still exist after destruction
```

After publication, the old return never reads the reusable slot or its sidecar.
Its saved positive charge still pins the old domain. Reissuing the slot cannot
redirect that refund to the new origin. Any new allocation also prevents root
destruction until its own return completes.

| Boundary | Berry | Mosaic |
| --- | --- | --- |
| Allocation lifetime | Domain charge, allocation claim and root claim | Positive full-slot domain charge |
| After slot publication | Refund admitted, decrement allocation claim, release root claim | Refund admitted |
| Terminal borrowed state | Atomic admitted/allocation words and transparent atomic Claims | Atomic admitted word only |
| Root drain | Closed owner observes only its root claim with Acquire | Closed owner is the only control and observes every ledger at zero with Acquire |
| Physical destruction | Owner-side cold operation | Owner-side cold operation |

The terminal operation must also satisfy Rust's borrow obligations. A reference
to a padded record can remain protected for the returner's call even after its
last explicit read/write. Another thread freeing that record in this interval
violates that protection. The [final-refund correction](final-refund.md) narrows
terminal borrows to atomic words; an ordering proof alone does not cover this
storage-layer obligation (OWN-03, LIFE-02, AP-02/03).

## Domain and root lifecycle

Streams dynamically lease prepared domain records. Closing stops new admission;
it does not revoke live buffers. Domain reuse waits for controls and all old
allocation/return obligations, including published-but-unrefunded returns. Grants
remain pinned until recycle, and domain generations retire rather than wrap.

Berry/Mosaic place eligible closed records in an issuer-maintained FIFO when
returns remain outstanding. `poll_domains(n)` examines at most its bounded number
of candidates. Snapshots and opens never perform an implicit maintenance scan;
remote Drop never maintains grants. The reference models equivalent drain without
that FIFO, which explains its distinct maintenance outcome.

`try_destroy` closes admission and may return Busy. Owner Drop with outstanding
access abandons storage; subsequent remote drops do not inherit deallocation.
Abandoned arenas remain retained until process exit. Forgetting ownership can
prevent drain forever. Neither a deadline nor a diagnostic sample authorizes
forced reclaim (LIFE-01/02).

Diagnostics are independent of arena storage. Their observers can outlive the
owner and free the separate diagnostic record on their last release. Abandonment
samples retain event-time accounting; they are not a live outstanding-charge feed.

## Identities and observations

A nonzero `root_identity` is opaque and assigned once during preparation across
the backends' shared identity allocator. It is not a heap address, a persistent ID
or a wire format. Exhaustion returns `0` without wrapping: zero is unassigned and
can recur, so it cannot key a unique-root record. Collectors must handle that case
without merging unrelated roots. For assigned roots, domain attribution uses
`(root_identity, DomainIdentity)`, because domain identities are root-scoped.

Current Berry/Mosaic `observed_*` fields are independent samples. For example,
unpublished capacity includes a final reference already at zero before bitmap
publication. Samples do not distinguish all ownership/return phases, supply a
coherent snapshot, run maintenance or authorize reclamation (SI-07, AP-05).

## Errors and progress

| Result | Meaning and caller responsibility |
| --- | --- |
| `Contended` | A bounded admission/reference attempt lost a concurrent update. A refund or clone can cause it far below the limit; the caller chooses whether and how to retry. |
| `BudgetExceeded` | This attempt observed insufficient byte headroom, including pending-return charge. Slot availability does not imply budget availability. |
| `ClassDepleted` / `DomainClassDepleted` | The attempt witnessed no eligible capacity. Berry confirms a zero hint with an RMW; Mosaic's confirming pass has a common empty witness under monotone publication. This does not promise emptiness at response time. |
| `DomainMaintenanceRequired` | Explicit bounded issuer maintenance may recover records; it does not guarantee that a pending domain has drained. |
| `ReferenceLimit` | The requested ownership/control rights exceed the applicable checked limit. Failure adds no rights. |

For domain opening, validation/resolution precedes the Active gate, grant
availability, domain-record availability and root/control claims, then an
infallible commit. The sequential reference oracle uses the same error precedence;
its absence of a pending FIFO is normalized separately (SI-09/13).

No hidden retry, wait or heap fallback is allowed in Berry/Mosaic hot paths.
Preparation, diagnostics and teardown have separate cold-work contracts. A
completed Drop finishes capacity publication and refund; a stopped return is not
helped and may retain its remaining resources. Bounded library work is not a
wait-free hardware guarantee or a hard elapsed-time bound. The original Shared
helping requirements are a separate, unimplemented target.

## I/O boundaries

`try_as_io_slice` creates a borrowed descriptor without copying or adding ownership.
On Windows a single descriptor can cover at most `u32::MAX` bytes; a larger view
returns TooLarge. `as_io_slice` retains std's panic for that case. The test is on
the view's length, not its backing capacity. Callers can split views explicitly;
there is no implicit allocation of a descriptor chain. This applies to Reference,
Berry and Mosaic Buf, and Mosaic BufView, Branch and LocalBuf (IO-01).

Generic Read adapters initialize the region before exposing `&mut [u8]`; optional
bytes interop preserves the initialized/filled distinction. Short writes retain
ownership for the remaining descriptors. Cancellation must end actual I/O access
before reclaim, and payload ownership alone does not bound kernel buffers,
registered I/O, external queues or tasks (IO-01..03).
