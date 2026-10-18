# Instructions for agents working on ABPS

Read [CONTRIBUTING.md](CONTRIBUTING.md) before design or implementation changes.
It is the canonical behavioral and engineering contract; its invariant IDs are
the references for reviews and tests. [README.md](README.md) summarizes scope and
status. This file makes the contract operational and does not define competing
semantics.

## Project direction

Preserve this priority:

```text
soundness -> bounded resources -> bounded hot-path work and reclamation
          -> throughput -> ergonomics
```

Optimize predictable resource consumption before minimum average allocation
latency. v0 is Shared CPU byte buffers with fixed storage/classes and flat domains.
Local, hierarchical quotas, magazines, owning chains, and specialized storage are
extensions. Type names remain illustrative; the specified behavior is normative.

The authorized `abps-reference` spike is a safe executable model. Apply the explicit
[reference-only exceptions](CONTRIBUTING.md#reference-backend-scope); its mutex,
wrapper allocations, and modeled metadata are not a production fallback. Do not
extend these exceptions to a future Shared backend or claim helping/latency proof.

The authorized Berry backend (`berry-spike`) is a separate experiment governed by
[SI-01/02/03](CONTRIBUTING.md#berry-single-issuer-experiment). Admission/control handles
are worker-local; buffer ownership crosses threads. A one-word class bitmap and
prepared ownership records replace the reference mutex/wrappers. This experiment
has no helping: a stopped return can retain its remaining claims, but a completed
Drop must finish capacity publication and refund without blocking or allocation.
Do not extend that explicit exception to the original multi-issuer Shared target.
Preserve the reference oracle and its module-level `forbid(unsafe_code)`; keep new
unsafe private and test the actual atomic protocol with Loom and storage with Miri.
Berry depleted errors require SI-01's confirming RMW after a zero load hint; only
an empty grant mask proves absence without touching the bitmap. Preserve budget
precedence and report Berry's own claim/rollback costs, not Mosaic's RMW formula.

The accepted SI-04 adapters keep allocation admission separate from queue policy.
Disconnect is default; explicit Skip drains rejected frames without losing framing.
Queue permits precede payload acquisition, and writer batches are bounded to eight
frames with no coalescing wait. Domain maintenance is issuer-side and explicit:
never scan the domain table from try_open_domain or a snapshot, and never defer
payload refunds to maintenance. Optional bytes interop must preserve initialization
and slot boundaries without adding Tokio to the core. Benchmark pacing, client I/O,
and server resources separately; retain every loss category in offered-load results.

SI-05 adds opt-in hard class reservations within one root, without borrowing.
Keep the default unreserved policy; reject mixing open methods. Grants commit
transactionally and remain pinned until complete domain recycle, including pending
refunds and forgotten ownership. Charge the prepared mask matrix and layout padding.
Do not add atomics or grant maintenance to remote Drop. Adapter maintenance examines
at most 32 records per call. SI-06 freshness is explicit: check deadlines before
every write attempt, close on expiry, and report late deliveries rather than hiding
them. Compare equivalent reservation, overload, and freshness policies in baselines.

SI-07 diagnostics examine a caller-bounded consecutive page, including free records;
never scan for enough active records, infer exact snapshots, or run maintenance.
SI-08 adapters consume one typed resource plan; report is default and strict envelope
validation precedes all server resources. Slots and byte limits are independent;
stage occupancy includes unfinished Drop/refund. SI-09 resolves grants once with
bounded stack scratch and preserves transactional opening/error order. SI-10 batch
cloning reserves references with one CAS, materializes under an unwind guard, and
leaves destinations unchanged on errors. Keep scalar fan-out as default; batch is
explicit. Test the actual batch protocol with Loom, storage with Miri, and retain
the reference oracle. No new atomics or grant work belong on remote final Drop.

SI-11..15 authorize the separate Mosaic backend (`mosaic-spike`). Positive
admitted charge protects storage through the final refund; control counts stay
issuer-local. Never access storage after that refund or a reusable slot after bit
publication. Final refunds and claim releases borrow only atomic words, never an
aligned record with padding (OWN-03); keep their schedule-dense Miri regressions
and multi-seed negative controls.
Exact depleted errors require the confirming RMW pass and its proof.
Keep sparse grants prepared, disjoint and pinned through full drain; no grant work
belongs on remote return. Borrowed views do not create ownership. Preserve Berry
and the safe reference as baselines. Test actual production transitions with Loom,
unsafe storage with Miri, and report bounds/measurement limitations explicitly.

SI-16 puts recovery in the adapter supervisor. Charge roots before prepare, retain
abandoned/unconfirmed charges, refund only confirmed teardown including diagnostic
release, and reject stale generation messages. Replacement has fixed root/byte
headroom and a lifetime attempt cap; it serves new work without replay. Worker
errors must cancel deadline-bounded coordination. Maintain pending domains on a
10 ms issuer tick with at most 32 examinations and skip missed ticks. SI-17 permits
explicit ZeroPayload preparation only; keep new lease initialization at zero and
never add work to acquire, recycle or remote return.

BR-01..06 authorize the opt-in `local-branches-spike` experiment in Mosaic
storage. Prepare and charge fixed per-slot branch records. Branch is uniquely
transferable; LocalBuf families are !Send + !Sync and change only their own Cell.
The last local owner completes record access before releasing one global right.
Construct branches only from exclusive BufMut, guard all unmaterialized rights,
and return the source unchanged on validation errors. Never read local counters
from global diagnostics or add work to ordinary Buf Drop. Preserve scalar default,
the existing baselines, queue policy and complete accounting on cancellation.

AP-01..09 harden these experiments: diagnostic samples must not invent transition
phases; compact local records keep immutable descriptors and end record access
before global release. Worker fan-out is explicit and uses prepared per-subscriber
credits across ingress, mailbox and local queues. Transfer to a bounded writer
batch before refunding a queue credit; cancellation drops ownership first. Preserve
FIFO, generation identity and deadlines. Do not weaken exact depleted observations
when removing search modulo. Separate primitive, full-cycle and TCP evidence, keep
all losses, record build identities and use physical cores by default. Stage proof
and diagnostic checks before adapter or layout optimization.

## Seven mandatory rules

AP-10 allows an explicit fixed-arena TCP profile with `capacity-check=report`.
Changing queue depth must not silently increase slots, grants or byte limits.
The opt-in local queue stays worker-local and is restricted to Mosaic scalar/batch
fan-out. Oldest-first freshness requires explicit Skip and may evict only a whole
queued frame after its configured minimum age; complete its Drop before replacement
admission. Never evict writer-batch or partially written frames. Preserve FIFO for
survivors, the queue credit equation, bounded cancellation, and SI-06 deadlines.
Report irreversible evictions, failed replacements and queue metadata separately.
Keep defaults unchanged; the profile's 5 ms healthy-subscriber p99 target does not
weaken AP-09 or establish a production latency guarantee.

1. **No hidden allocation in steady-state hot paths.** Include metadata,
   collections, per-view objects, system frees, mapping changes, helping, errors,
   and first return from a previously uninvolved thread.
2. **No unsafe without a documented invariant and proof obligation.** Keep the
   trusted core private, enable `#![deny(unsafe_op_in_unsafe_fn)]`, and explain the
   actual transition, bounds, initialization, aliases, lifetime, and synchronization.
3. **No lock-free algorithm without Loom coverage.** Model the production protocol
   with instrumented synchronization; stress or a similar toy model is insufficient.
4. **No performance optimization without a before/after benchmark.** Preserve
   comparable workload and overload policy; report failures, tail latency,
   allocations, and retention. Use a dedicated performance host.
5. **No platform-specific behavior in core semantics.** Preserve stable-Rust
   ownership, accounting, and errors; isolate backend details and report target
   evidence separately.
6. **No implicit waiting, allocation fallback, or copying.** Acquisition performs
   one bounded attempt. Retry, wait, eviction, compaction, and runtime policy must
   be explicit; they cannot revoke retained ownership.
7. **Safe Rust callers must be unable to cause UB, budget violation, or double
   reclamation.** Cover initialization, aliasing, overflow, unwind, helpers, and
   cross-thread reuse without relying on eventual destructor execution.

## Apply the complete contract

- **MEM-01/02/03:** separate root reservation, domain admission, payload, metadata,
  and RSS. Charge full slots and pending operations; preserve origin across handle
  transfer and views. A byte limit grants neither dedicated storage nor availability;
  explicit SI-05 class grants are a separate partition of capacity rights.
  External queues and tasks need independent bounds.
- **PROG-01/02:** credits are not exact physical free-slot counts. `ClassDepleted`
  requires the specified consistent observation; unfinished transitions or failed
  attempts use `Contended`. Failed acquire restores its own resources even when
  helping changes other counters.
- **PROG-03:** published returns must be discoverable and helpable without their
  original thread. Prove exactly-once effects and stale-helper safety. Completed
  final Drop must have returned the slot, published its credit, and refunded its
  charge; queuing future cleanup does not satisfy the contract.
- **PROG-04:** bound work for success, failure, rollback, and helping. No unbounded
  scan, CAS loop, or wait on one participant. The counter tree is not accepted;
  fixed depth and sequentially consistent explorations do not prove its protocol.
- **OWN-01/02/03:** keep mutable ownership exclusive, freeze consuming, filled and
  initialized lengths distinct, and views within filled bytes. Use checked
  fallible reference creation. Prove disjoint slot access, publication, and reuse;
  never borrow an entire slab mutably over neighboring live references.
- **LIFE-01/02:** root/domain close follows admission registration. Drain includes
  handles, operations, and helpers. Physical teardown is owner-side cold work.
  Owner drop with outstanding access or unfinished returns closes and abandons
  storage until process exit; later buffer drops cannot inherit teardown.
  Forgetting ownership never permits forced reclaim.
  Dynamic domains lease prepared records; owner close stops admission, old handles
  prevent reuse, and generations never wrap. Account retired records explicitly.
  Abandonment must remain observable outside the owner, without unbounded logs.
- **IO-01/02/03:** generic `Read` needs initialized writable bytes and independently
  checked lengths. Bound descriptors and completion ownership; cancellation does
  not by itself end kernel/device access. Test real ingress/queue/consumer/reuse
  and input/output headroom.

Do not add a background retirement mechanism, TLS growth, malloc fallback, new
meaning of exhaustion, or accounting migration to make an implementation fit.
Agents must not resolve unspecified ownership, accounting, progress, or shutdown
semantics by inventing an implementation-local policy. Record the missing decision
in the canonical contract before dependent code; explicitly justify normative
changes and synchronize all affected documents.

For every unsafe patch, name the invariant ID and state transition, provide its
local proof obligations, and add a regression that detects a violation of that
transition. A generic SAFETY paragraph or passing stress run is not a substitute.

## Working and reporting

- Use Berry / `berry` and Mosaic / `mosaic` consistently in code, features, CLI,
  tools and current documentation. Both use single-issuer admission; Mosaic has
  segmented class bitmaps. Do not restore old-name aliases. Preserve invariant
  IDs and historical evidence names/hashes; see the canonical
  [backend names](CONTRIBUTING.md#backend-names).
- Inspect current files, applicable instructions, and repository status. Preserve
  unrelated edits and avoid incidental cleanup or dependency changes.
- Fix shared invariant violations across affected paths instead of adding one
  caller's workaround or weakening a check.
- Inspect manifests and harnesses before selecting commands. The repository root
  is one unpublished library package: implementation in `src/`, integration tests
  in `tests/`, and runnable examples in `examples/`. This checkout has verification
  scripts but no GitHub Actions workflows. Historical local/CI configuration and
  production validation remain separate evidence; do not claim unexecuted jobs.
- Introduce deterministic accounting/lifecycle tests, independent model/property
  tests, focused Miri, and production-protocol Loom as applicable. Include
  interrupted returns/helpers, stale reuse, close races, forget, and allocation
  harness positive controls. Apply fuzz, sanitizers, soak, and target checks
  according to CONTRIBUTING.
- For documentation changes, validate cross-document contracts, invariant IDs,
  links/anchors, fences, and whitespace. Keep English canonical and identifiers
  exact.
- Report actual changes, checks, results, and material gaps. Distinguish specified
  behavior, candidate algorithms, model evidence, and verified implementation.
  Never present planned CI, target coverage, or benchmark results as completed.

## Executable verification

Maintain `verification/matrix.json` and [SAFETY_CASE.md](SAFETY_CASE.md) when changing
unsafe transitions or synchronization. Use the shared trace/oracle for equivalent
backend behavior; never accept spurious sequential `Contended` to hide a mismatch.
Test-only failpoints must use real guarded transitions and compile out of ordinary
builds. Never inject user callbacks into incomplete invariants. Report actual Loom
bounds, reject empty selectors, and separate bounded/incomplete/unsupported results.
Keep intentional-retention tests out of persistent fuzzing and ordinary leak checks.
Do not suppress TSan races or strengthen production atomics merely to satisfy TSan.
The accepted TSan variant replaces only the final reference Acquire fence with an
Acquire load before slot publication. Keep Release decrements and the ordinary
fence path; test both actual variants with Loom and sensitive negative controls.
Verify the target sanitizer/cfg selection and treat TSan warnings as failures even
on exit code zero. Include build.rs in source identities and disposable test trees.
Preserve historical evidence and run mutation tools only in disposable source trees.

ABPS-001..032 are verification-review scenario IDs, not additional normative
invariants. Keep their registry mappings, preconditions, applicability and detectors
complete. The concrete rights oracle must remain independent of backend atomics
and traversal. Issuer histories perform maintenance only at explicit trace steps;
retain the old normalized reference comparison separately. Never rebind a missing
handle/domain ID while shrinking. Record the concrete replay, failing step/outcome
and source identity, not only a seed. Keep queue credits separate from writer batch
ownership and include actual adapter tests/sources in sanitizer groups/build hashes.
Test staged publication/refund without inventing an atomic whole-Drop snapshot.

Qualify execution evidence by source, Cargo target, resolved features, compiler,
platform, tool and synchronization variant. Discovery alone and legacy unqualified
records cannot pass a scenario. Require all planned steps and variants; preserve
failures across subsequent successes. Run harness tests under ordinary and
optimized Python. Retain failed mutation artifacts and check the unmodified named
detector before mutation. Shrinking must preserve the violated postcondition and
logical owners; unknown panics retain their original traces. Use neutral engineering
names for verification directions and reports.

For generation-transition verification, retain the live source throughout clone
and batch reservation. A hint must not cross a second issuer operation. Separate
controlled accounting observations from visibility models: observer barriers or
thread join must not supply the synchronization being tested. Exercise next-slot
initialization and assert that successful reuse was actually explored. Treat the
batch materialization interval as an explicit non-unwinding proof obligation;
test-only accessors must disappear from ordinary builds. Berry's closed-domain
drain relies on monotone counters and no pending admission/commit; do not transfer
its two-counter argument to Mosaic's final-refund lifetime protocol.
