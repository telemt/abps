# ABPS safety case

**Status: experimental implementations; production admission remains open.**
[CONTRIBUTING.md](CONTRIBUTING.md) is the normative contract. This document connects
local arguments to executable checks, not a certificate of soundness. Berry is a
single issuer with one bitmap word per class; Mosaic uses segmented bitmaps with
the same sole-issuer boundary. Neither implements the original helping target.

[The verification registry](verification/matrix.json) names invariant IDs, selectors,
features and applicable tools. [The unsafe-site inventory](verification/unsafe-sites.json)
identifies each lexical unsafe block, unsafe function and unsafe impl in the
trusted storage modules, with a local argument below and registry checks. Run
`python3 scripts/verification_inventory.py` to detect drift. Its lexical inventory
is a review aid, not a Rust parser; inspect macro expansions/dependencies separately.
Updating the inventory does not discharge an obligation by itself.

## Storage and ownership obligations

### layout

MEM-01, OWN-03. Plan checks every product, Layout extension, alignment and root
limit before allocation. RootPtr accessors derive pointers from the original arena
allocation, with checked indices into initialized typed records. The caller must
hold a live control, allocation claim (Berry), or positive admitted charge (Mosaic).
No intermediate mutable reference covers the whole arena. Disjoint slot payloads
can therefore coexist with live references in neighboring slots. Preparation/layout,
neighbor canary, checked boundary and Miri tests cover these obligations. Layout
errors must occur before attempting oversized allocation; tests do not allocate
`usize::MAX` bytes.

### preparation

MEM-01, OWN-01/03, SI-17, BR-04. Preparation owns the unexposed arena exclusively,
initializes all records and immutable descriptors before activation, and unwinds
allocation failure without exposing partial records. Payload initialization is
explicit; ZeroPayload writes only checked payload regions and does not initialize
a future lease's logical prefix. Branch descriptors retain arena-derived pointers
but no independent deallocation rights. Layout/zero-payload/branch-size tests and
Miri enforce alignment and neighboring metadata boundaries. System allocation
failure coverage remains distinct from injected admission failure coverage.

### admission

MEM-02, SI-01/11, LIFE-01. Only the issuer modifies control Cells, origin and free
record tables. Clearing a bit acquires exclusive access after the old readers'
release chain. A guard owns every intermediate claim; test-only faults after pin,
charge, bit, origin and references initialization execute this actual path. Error
and unwind restore the attempt's bit/charge/claim; resetting references is permitted
only while the slot is exclusive. Closing stops new admission; old controls and
pending refunds prevent identity/grant reuse. Generations retire instead of wrapping.
No callback or foreign destructor runs inside an incomplete production transition.

### ownership

OWN-01/02/03, SI-10, AP-01/02. Lease Send/Sync relies on immutable layout plus the
slot's positive reference count, not shared access to issuer Cells. BufMut is Send,
not Sync/Clone; it alone writes the slot. freeze consumes it. Safe reads expose
only the filled initialized prefix; `initialize_to` establishes initialization,
and `put_slice` checks capacity before copying. The bytes BufMut implementation
checks range independently; its unsafe caller must initialize bytes before
advance_mut. Tests never invoke unsafe APIs with invalid initialization promises.

Buf and borrowed views retain the source lifetime and checked subrange. A clone's
source remains live throughout increment; no test fabricates cloning from a dead
handle. Batch rights are reserved once; the original remains borrowed through
construction/rollback. Last release acquires all readers before saving the return
descriptor and publishing capacity. The old return may coexist with a new origin,
but accesses only saved stable operands afterwards. Neighbor canaries, shared
traces, batch-unwind tests, type assertions, Miri and actual protocol Loom models
cover these transitions; all raw pointer operations are excluded from Loom builds.

### remote

OWN-03, AP-01/02, SI-11. Mosaic Lease and branch descriptors carry the private
`RemotePtr` authority from [raw/remote.rs](src/mosaic/raw/remote.rs). Its pointer
and layout projection are private to that child module. It exposes only immutable
class/layout properties, slot metadata protected by ownership, atomic bitmap and
ledger references, and the explicitly family-owned branch records. It offers no
issuer Root, DomainRecord, controls, cursors, grants or destruction accessor.
Projections never form &Root or &DomainRecord; the ledger projection excludes
concurrently changed issuer Cells. The authority remains one pointer; positive
charge, publication and final-refund obligations still determine its lifetime.

The remote call graph is Lease::{slot,capacity,origin,data,try_clone,begin_return},
BufMut payload/freeze operations, Buf view/scalar/batch creation and Lease Drop.
Branch construction uses the checked sidecar projection; FamilyRight finishes
record access before reconstructing one Lease. Final return captures bitmap,
ledger, bit and capacity before publication; PublishedReturn has no slot access,
borrows only the admitted word, and its final refund is the last arena access.
No remote operation uses RootPtr.
Compiler checks first build this boundary successfully, then require eight precise
access/privacy rejections. Raw paused-return integration checks metadata and
diagnostics at refs=0, after publication, after cross-domain reuse and after old
refund, before joining the returner. Miri covers these actual projections and
branch/payload reuse. The compiler boundary constrains available safe methods;
it does not authorize arbitrary new unsafe conversions or prove pointer validity.

### teardown

LIFE-02, AP-03/04. Berry's root claim is released last; Mosaic's positive charge is
refunded last. The return path performs no later storage access. Only the issuer
owner destroys storage after Acquire observations establish complete drain. Domain
grants remain pinned through pending returns. Owner drop before drain deliberately
abandons the arena; subsequent drops never acquire teardown work. mem::forget can
retain resources forever. Existing no-join teardown and reuse-before-refund tests
check the boundary without crediting thread join as the synchronization proof.

The last access is also a Rust borrow obligation (OWN-03). The owner may free the
arena while the returner's frame is still live, and a protected reference to
non-atomic bytes would outlive that deallocation. Mosaic's 64-byte-aligned ledger
has 56 padding bytes: refunding through `&DomainLedger` was UB in both Stacked and
Tree Borrows. Each PublishedReturn therefore holds only `&AtomicUsize` words, and
Berry's `Claims` is `repr(transparent)`, so neither correctness depends on layout.
Miri sees the window only when a schedule preempts the returner inside it (about
1% of races), so three regressions run hundreds of races per seed with a
persistent returner: ordinary Buf, branch families and Berry claims. In a local
Miri sample (nightly, Windows x86_64) the ordinary Buf regression exposed the
old refund on all 16 seeds; the adversarial review found it with the earlier
single-race teardown tests on none of 64 seeds. Per-seed detection is weaker for
the shorter Berry claim window and for the branch root's longer drain pass, and
an unrelated source edit moves every seed's schedule: after one such edit 128
Berry races caught the padded control on 7 of 16 seeds, and 128 branch races
on the shared two-record fixture on 4. Berry now runs 512 races (15 of 16), and
the branch regression races a one-slot, one-record root 256 times (15 of 16).
Negative controls restore the padded-ledger borrow in either backend; each passes
its baseline under seeds 1..=3 and counts as killed when one seed exposes it, so
the verdict does not hinge on one schedule. Berry's root-claim edge has its own
production-protocol Loom model; it kills Relaxed release and load of the claim.

### diagnostics

LIFE-02. Diagnostics Send/Sync shares only immutable data and atomics in a separate
record. Checked references protect record access; the final cold observer uses
release/acquire synchronization before deallocation. Arena destruction publishes
its result and relinquishes its diagnostic right. Observers can outlive the owner;
abandoned roots remain observable. The intentional-retention allowlist is exact
and finite. Ordinary Miri/ASan checks keep leak detection enabled.

### observations

AP-01/05. A snapshot runs on the sole issuer, excluding concurrent origin writes,
record recycle or acquire. Remote returns do not write origin or control Cells.
Loads of bitmap, references, frozen and admitted are independently timed: no
cross-counter equality is inferred during concurrency. Diagnostics never inspect
another thread's local-family Cell. Tests check independent bounds while returners
run, then exact ledgers only after quiescence and reuse under a new domain identity.

### families

BR-01..05, AP-06. Branch Send requires exactly one local owner; LocalBuf families
are !Send/!Sync. Only their current thread touches the Cell. Every nonempty family
retains one global right; construction guards retain unmaterialized rights. Last
local release copies the immutable descriptor before releasing that global right,
and never reads the sidecar afterwards. Slot reuse reinitializes counters only
after all families have released their rights. Unique migration uses synchronized
handoff; a borrowed view prevents moving its owner. Construction-error and prefix
unwind tests, the family oracle, no-join protocol models and Miri cover this argument.

BranchBuild is installed with the source's single right before the exclusive
1-to-K store. Its SourceOwned, BatchReserved and Committed phases make cancellation
explicit. During construction the global count equals pending rights plus the
optional staged family plus installed families. A staged family is guarded before
output installation. Bounds/phase checks precede each transfer; only non-panicking
scalar/Option assignments separate the counter transition and guard update.
`br01_source_reserved_staged_and_installed_boundaries_conserve_rights` checks
normal cancellation and unwind at every complete boundary for K=1,2,3 in both
grant policies, followed by immediate same-slot/new-domain-generation reuse.
This is a private protocol hardening, not a reproduced public unwind defect.

## Atomic edges and progress

| Edge / ordering | Obligation | Executable evidence |
| --- | --- | --- |
| Checked admission CAS, Relaxed | Sole issuer increments; concurrent refunds may cause Contended; failure owns no new charge. | Admission/refund Loom; guarded rollback; common ledger traces. |
| Checked reference CAS, Relaxed | A live source retains one right; overflow cannot publish rights. | clone/nonfinal-drop and batch Loom; limit/unwind tests. |
| Reference decrement Release + final Acquire fence (TSan: Acquire load) | Every earlier reader finishes before reuse. The final load precedes bitmap publication and cannot see a new allocation. | Both actual variants in no-join reader/overwrite and branch-reuse Loom; missing-fence and Relaxed-load mutations; Miri/native reuse. |
| Bitmap publication Release, claim Acquire | Publication ends all old slot access; only issuer clears bits. | Two-return conservation, masked grants, return/reuse tests. |
| Confirming Acquire RMW | Read modification-order predecessor; Mosaic's confirming pass has an empty witness under monotone remote publication. | Berry stale-hint and Mosaic hidden-publication regressions; depletion negative control. |
| Berry domain/root completion Release, drain Acquire | No premature domain reuse or root destroy; root release is last and borrows only the claim word. | Identity/drain Loom, last-claim teardown Loom with Relaxed release/load controls, owner teardown without join, schedule-dense Miri teardown. |
| Mosaic refund Release, drain Acquire | Positive charge pins storage until the final access, which borrows only the admitted word; all ledgers must drain. | Multi-domain zero-observation Loom, raw no-join teardown, schedule-dense Miri teardown for Buf and branch families with a padded-ledger control. |
| Frozen Release / diagnostics Acquire | Diagnostic observation only; payload transfer still requires synchronized ownership handoff. | Remote-freeze snapshot and mutable-handoff tests. |
| Exclusive references begin / branch begin, Relaxed | No published right exists during initialization. | Admission faults, branch construction and rollback. |
| Diagnostic reference release/acquire and status publication | Observers retain their own record; status does not authorize arena access. | Diagnostics after destroy/abandonment and Miri. |

For OWN-03, SI-02/11 and BR-03, the final 1->0 RMW leaves no live handle or
unmaterialized right that could increment the reference word. Only the issuer can
reinitialize it, after acquiring the not-yet-published free bit. Consequently an
Acquire load sequenced after that RMW observes its zero (atomic coherence), which
belongs to the RMW release sequences of all prior readers and batch rollbacks.
The load supplies the same required happens-before edges as the ordinary Acquire
fence, before publication, refund and owner-side destruction. Neither variant
reads slot/sidecar metadata after publication or storage after final refund.

Rust Arc uses this load approach because TSan does not instrument atomic fences.
The build script detects the target's `CARGO_CFG_SANITIZE=thread` and enables the
private `abps_tsan` cfg. Native builds retain the fence and Release decrements;
only final release in instrumented builds adds a load in place of the fence.
The cfg probe reports the actual compiled variant and target sanitizer. Loom
forces each variant in its own build and runs the same production transitions;
it still excludes unsafe arena storage. The TSan result applies to the load
variant, not to the native fence. Neither is a whole-program soundness proof.

SI-10's `pending` counter is observed at complete materialization boundaries. The
concrete transfer checks the output index and positive pending count first. Its
remaining operations copy pointer/usize fields, clone a concrete Range<usize>,
replace None and advance a bounded prefix; none can unwind. Between decrement
and prefix completion the execution frame carries the transferring right. There
is no callback or test failpoint in this interval. Boundary tests check exact
live-plus-pending conservation, both ordinary guard rollback and unwind for every
prefix of N=0..3. A deliberately inserted panic tests the sensitivity of that
argument rather than demonstrating a panic in the unmodified implementation.

The generation models compose the actual batch/scalar reservation, final release,
publication, next claim and next `begin()` for Berry and Mosaic. Old readers write
Relaxed markers; assertions on the next generation precede join and additional
diagnostic synchronization. Host-only coverage flags are written after assertions
and read only after exploration, so an unvisited successful-reuse branch cannot
pass and those flags cannot provide the modeled visibility edge. A separate model
retains a live clone source while its peer drops; it never clones from zero.

Berry's two-counter drain is used only after the sequential issuer has completed
or rolled back every admission and closed its gate. Until record reuse, both
counters are monotone decreasing. The two-allocation model checks this actual
predicate before join, then re-admits into the same record while old returns may
still owe root decrements. Those decrements cannot touch its new ledger. Raw
retirement tests additionally exercise valid Closed errors and deferred Retired
transition with two remote buffers. Mosaic has a different final-refund predicate;
the raw boundary tests apply there, not Berry's allocation-counter proof.

An interrupted return is not helped in these experiments. Before publication it
withholds the slot; afterwards it can withhold charge/domain completion even while
the slot is reused. Completed Drop must finish its applicable effects. Tests assert
these distinct states, not eventual completion of a stopped participant. Bounded
source work does not prove wait-free atomics or a wall-clock latency bound.

## Evidence and open admission requirements

The contract-verification layer adds these executable obligations. Registration
is distinct from a completed campaign; results retain the source and tool bounds.

| Obligation | Regression / detector | Remaining limitation |
| --- | --- | --- |
| SI-10: source, constructed handles and pending rights conserve references | `abps_batch_boundaries_conserve_handles_pending_and_source`; injected handoff-unwind mutation | Exact observations occur at complete, nonconcurrent boundaries. |
| OWN-03: batch/scalar readers precede a reinitialized slot | `generations::batch_release_chain_reinitializes_one_slot_without_join`, `live_clone_source_survives_peer_release_and_next_generation`; missing begin/final acquire and Relaxed publication controls | Finite protocol compositions; raw arena access is covered separately. |
| LIFE-01/02: closed-domain completion gates reuse and retirement | `closed_two_allocation_drain_gates_record_reuse_before_join`, `abps_generation_retirement_waits_for_closed_domain_returns`; omitted allocation-gate control | Requires the single sequential issuer and no outstanding admission commit. |
| LIFE-02, OWN-03: the final refund/claim release borrows only atomic words before owner teardown | `si11_final_refund_races_teardown_across_many_schedules`, `br03_final_family_release_races_teardown_across_many_schedules`, `life02_final_claim_release_races_teardown_across_many_schedules`; padded-ledger controls | Miri samples schedules per seed: detection is probabilistic per seed, deterministic per seed and source. |
| LIFE-02, AP-03: Berry's teardown observation acquires every earlier return | `life02_last_claim_observation_acquires_every_prior_return_before_destroy`; Relaxed claim release/load controls | Protocol model without raw storage. |
| MEM-02, LIFE-02, SI-05: rollback followed by close/remote return/reopen preserves the neighbor and pinned grants | `abps_fault_close_remote_return_reopen_preserves_neighbor` in both raw backends; existing admission/refund/generation controls | The experiments do not help stopped returns. |
| OWN-01, MEM-02, LIFE-02: boundary histories retain exact error outcomes, payload and generation isolation | `abps_boundary_histories_preserve_errors_isolation_and_recycling`, independent rights oracle | Exact ledger comparisons occur in sequential/quiescent states. |
| SI-12, AP-08: first-pass success and empty-mask confirmation stay within the search bound | `abps_search_success_and_masked_failure_have_bounded_work`; redundant-pass mutation | Counts protocol visits, not machine instructions or elapsed latency. |
| IO-01/02, SI-04, BR-06: partial I/O, wakeups and failures preserve bytes and release owned resources | `abps_scripted_ingress_errors_wakeups_and_cancellation_preserve_prefix`, `abps_scripted_writer_error_cleanup_preserves_exact_wire_prefix` | Actual adapters with scripted I/O; not a model of kernel completion ownership. |
| AP-09: a reduced counterexample retains its violated postcondition and owners | `abps_shrinking_preserves_defect_identity_not_panic_text`; substituted-defect mutation | Unexpected panics retain the original trace without shrinking. |
| AP-09: execution evidence cannot mix targets, variants or revisions, and cannot discard failures | `EvidenceRegressions` under ordinary/optimized Python, exact discovery/execution matching, directed baseline checks | Tool success does not establish whole-program soundness. |

Evidence schema 1 records qualified executions and required campaign steps. Older
unqualified archives remain historical; they cannot automatically satisfy this
new matching contract. Doctests and compile-fail cases are executions; declared
empty targets only establish compilation. Failed mutation logs survive campaign
failure, and a named detector must pass unmodified before its mutation is counted.

The next-wave ABPS-001..032 checklist is registered in `verification/matrix.json`
alongside exact selectors, applicability, preconditions, oracles and detectors.
These review IDs refer to existing normative invariants; registration alone does
not claim a completed campaign. [The conservation report](docs/rights-verification-2026-10-10.md)
records the actual source, runs, limitations and negative-control outcomes.

The independent concrete oracle keeps logical identities stable during replay and
shrinking. Its issuer histories do not insert maintenance. The legacy normalized
reference comparison remains separate; reference and issuer recycling are not
silently equated. Pure negative controls reject orphan rights, invalid budgets,
early recycling and duplicated/missing FIFO membership. A staged-return ledger
allows an old charge and a new allocation in one physical slot simultaneously.
Raw regressions exercise that same publication/refund window, including lack of
budget, closure and reuse of a single domain record.

The Mosaic partition observer copies prepared records only from issuer-side tests.
Its independent bounded walker checks node/row ownership, cycles, class membership,
nonzero masks, valid tail bits and exact disjoint coverage, including pending
returns. Deliberately corrupted samples test this observer's sensitivity. It does
not reuse production assignment/release traversal. Berry checks the equivalent
mask partition without introducing sparse metadata.

Small production-protocol Loom tests cover all batch prefixes N=1..3 and the
depleted witness on either side of first confirmation. Scheduling channels select
semantic windows; separate no-join models test visibility without those channels.
Actual adapter tests track per-subscriber credit tokens through ingress, mailbox,
local delivery and bounded writer batches, including cancellation. Tokio internals
remain outside Loom coverage. Historical TSan/fence qualifications still apply.

Use [the verification guide](docs/verification.md) and the runner's `results.json`.
The [2026-10-10 local report](docs/verification-2026-10-10.md) includes successful
campaigns, mutation timeouts and the unsuppressed TSan finding with its control.
Each run records source identity, exact command/features, seeds, tool versions,
exploration limits, logs and completion status. A successful bounded Loom run is
not exhaustive. A model excludes raw storage; Miri does not cover every execution;
ASan cannot infer semantic slot boundaries; TSan does not model atomic fences;
MSan requires all code instrumented. No passing result removes these limitations.

Before production, require all critical registered transitions checked, every
mutation survivor explained or covered, no unexplained sanitizer findings or
resource growth, a long campaign for the release source, dedicated-host performance
comparison and independent unsafe review. These external/release activities remain
open until evidence for that exact source is attached. Configured CI is not executed CI.


## Grant, drain and sidecar evidence obligations

SI-05/SI-13: [the shared logical slot-set oracle](src/test_support/grant_histories.rs)
assigns integer slot identities to domain incarnations. It predicts lowest free
rights without bitmap words or node allocation. Every complete action is checked
against the dense Berry representation or the independent Mosaic node/row audit.
The existing concrete public histories retain general interleavings and shrinking;
these new generated cycles shrink whole, fully drained incarnations so references
cannot bind to a replacement owner. Cached pool sufficiency assertions precede
all opening mutation; corrupted private structures are not a rollback contract.

SI-11: raw root teardown and [the Loom model](src/mosaic/protocol/tests.rs) now call
the same `root_drained` predicate. Three records include a charged final record;
two returns address the same or different domains. Relaxed reader markers must be
visible before join if drain succeeds. Host-only coverage flags ensure the success
branch was explored without supplying model synchronization. Berry retains its
separate claim/allocation proof and existing paused-return/no-join tests. Its
raw `try_destroy`, owner Drop and last-claim Loom model share one predicate,
`Claims::only_owner`, applied to a single Acquire load; Drop also reports
abandoned claims from that same observation.

BR-03/04: review includes the complete [storage implementation](src/mosaic/raw/branches.rs),
[local protocol](src/mosaic/protocol/branches.rs), [Loom compositions](src/mosaic/protocol/tests/local_branches.rs)
and [storage regressions](src/mosaic/raw/branches/tests.rs). A local family protects
its immutable descriptor through one global right; its last release completes all
record access before global release. The strengthened visibility model reacquires
before join, then executes complete global/admitted/local transitions with 1, 2,
then 1 families on the same records. Actual raw storage checks initialization and
payload between incarnations. No generation counter is needed while all old rights
block reuse; forgetting a family intentionally prevents this transition forever.

MEM-01: wide arithmetic checks every typed range for alignment, nonoverlap and
arena bounds separately from production Layout operations. Configuration fuzzing
shares the public oracle with native tests and caps actual preparation at 4 MiB.
Large accepted plans are layout-only evidence, not allocation feasibility tests.

SI-12/AP-05: an exact depleted result witnesses emptiness during the attempt;
publication into an already confirmed word may precede the error response. Masked
cursor models preserve other grants. Diagnostic observations cannot distinguish a
live slot from a final release before publication, and do not prove RSS residency.
The observability probe measures grant traversal, pages, busy teardown and mixed
admission separately; its bounded channel and clocks are measurement overhead.
Local smoke is a harness check, not production throughput or tail-latency evidence.
