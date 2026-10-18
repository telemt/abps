# Mosaic branch construction and review verification — 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

The reviewed cursor behavior is reproducible and is the explicit SI-12 policy:
keep the winning word, including the winning sparse grant node. It is not a
round-robin fairness contract. No public memory-safety or accounting failure was
reproduced. This change strengthens private branch construction and the evidence
for its transitions; it does not change admission, reference limits, return
ordering, bitmap search, public APIs or arena layout.

## Disposition of the review

| Concern | Verified contract and action |
| --- | --- |
| Cursor retains the same word | SI-12 requires winning-word reuse. New regression frees slots in both an earlier word and the winning word, checks repeated reuse, then exhausts/wraps all three words including a one-bit tail under both policies. All valid slots remain reachable exactly once. Round-robin would be a separate policy/performance change and needs representative contention measurements. |
| Branch construction depends on non-unwinding transfers | No reachable public panic was reproduced in the old transfer interval. The guard now exists while it owns the source right, transitions to BatchReserved, guards a staged family, and commits only after all output entries are installed. Normal cancellation and unwind are tested at every complete boundary. |
| Internal decrements lack diagnostics | Added debug assertions to local-reference release and issuer control unpin before subtraction; refund checks the scalar returned by its existing Release RMW. Nothing accesses arena storage after final refund. These diagnose violated private preconditions; they do not promise recovery from corrupted state. |
| Publication/reuse/refund | Existing actual-protocol Loom models and raw paused-return regressions are rerun. The old charge pins its domain/arena through refund, and cannot change the new origin, references or payload. Visibility models omit observer barriers and join synchronization; controlled accounting tests deliberately use pauses. |
| Admission rollback and FIFO recycle | Existing tests cover Err and unwind at Charged/Bit/Origin/Ready, same-slot/domain reincarnation, all configured domain records, every return permutation and bounded poll(1). They are rerun rather than duplicated. |

The adjacent review's proposed retries and stronger counters were also checked
against the canonical contract. A failed single admission/reference CAS may
return Contended, including during a refund or concurrent clone. Diagnostics
cloning is checked and fallible too. BudgetExceeded includes pending-return
charge; a published bit does not imply refunded budget. These are specified
bounded-attempt outcomes, not leaked resources. No hidden retry/fetch_update loop,
refund-before-publication or blanket AcqRel substitution was introduced. Relaxed
count reservations are not payload publication; the release/acquire chains remain
covered by the actual protocol models.

Empty reserved grants are explicitly valid (SI-13): they consume a domain record,
no grant row/nodes, grant no capacity, and can close/recycle normally. Budget
precedence is preserved. Snapshots remain independent observations and never drive
reclamation. Existing partition, differential-history, snapshot and lifetime tests
cover these paths. Debug assertions alone are not the release safety argument:
checked public inputs, single issuer, ownership guards and lifetime barriers are.

## Construction invariant

BR-01 now names SourceOwned, BatchReserved and Committed. The guard first owns the
source's single global right. An exclusive store replaces it with K rights; only
non-panicking scalar assignments connect that store to the installed guard state.
During batch construction:

```text
global references = pending rights + staged family + installed families
```

Sidecar initialization and transfer into a staged family have checked bounds and
no intervening fallible work. The staged family remains owned if construction
stops before output installation. On cancellation, staged/installed families drop
and every unmaterialized right is released exactly once. Only Committed leaves
ownership in caller output. All state is on the construction stack; descriptors,
local handles, atomics and remote final Drop retain their existing representation.

The new regression runs 72 deterministic cancellation/unwind histories: K=1,2,3,
both grant policies, all SourceOwned/BatchReserved/staged/installed boundaries.
Every history checks references, payload, admitted bytes, bitmap availability and
control rights, then closes/reopens the sole domain record and reuses the sole
slot with fresh initialization and origin. Existing prefix rollback tests remain.
The new negative controls remove either the source right or the staged family's
release; the named detector must fail while its unmodified baseline passes.
No callback/failpoint is placed inside an incomplete transfer invariant.

## Evidence

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

Accepted source digest:
`404810774e8ce23bd8544770edf8a736c5577c6306846391bdb5d1bd8d2d5e6f`. Starting tree:
`1c11b7d89e2ada28dca758a0be5353ff831d5a0825352fd28638552a52b1447e`. There was no Git
metadata; a source copy and unified patch preserve the baseline. The summary
(`evidence/branch-construction-verification-2026-10-10.json`) and compressed logs
(`evidence/branch-construction-verification-2026-10-10.raw.json.gz`) retain commands,
compilers, feature resolution, source identities, bounds, mutation baselines and
failures. The initial mutation-tool lookup failed because the installed executable was
absent from PATH; this attempt is excluded, retained, and followed by a complete run
with the installed tools explicitly selected.

Rust 1.96.0 and nightly-2026-10-03 ran on x86_64-unknown-linux-gnu.
Counts below are executions, including repeated configurations, not distinct tests.

| Campaign | Completed result |
| --- | --- |
| Native feature matrix | 1,153 tests + 73 doctests + one synchronization-configuration probe = 1,227 executions; fmt and Clippy passed across seven configurations. |
| Release storage | 90 tests with both backends, branches and bytes interop; validates behavior with debug assertions disabled. |
| Loom | 40 models under each final-reference synchronization variant + two configuration probes = 82 executions. |
| Focused Miri | 12 selected tests across seven groups, two alias models and three seeds = 72 executions. |
| ASan | 129 tests + one instrumentation probe = 130 executions. |
| TSan | 129 tests + one instrumentation probe = 130 executions. |
| Property histories | 36 executions; case setting 1,000, public operation bound 256, seed 424242; includes independent sparse-grant histories. |
| Directed mutations | 16/16 killed by their named detectors; every unmodified baseline passed. Includes lost source/pending/staged rights, removed synchronization and early refund/recycle. |
| Allocation probes | Five passed, including local branches and worker fan-out; zero unexpected hot or first-remote allocations/frees/reallocations, with positive controls. |
| Verification infrastructure | 42 Python tests under ordinary execution and 42 under optimized execution; 87 unsafe inventory sites checked; all 104 registry checks and 32 review mappings have applicable final-source evidence. |

## Construction cost

Both revisions were built before timing, with identical release features
`local-branches-spike,bytes-interop`. All verification workers had completed before
24 interleaved probe runs: materialization/full cycle, one/four branch workers,
three repeats, both source versions. Issuer and workers were pinned to distinct
physical cores. Each run served all 64,000 offered frames, recorded zero rejected
views, used identical reservation layout, and drained successfully.

The existing `branches_probe --case=materialize --mode=local` includes a timer pair
per frame and surrounding fan-out work; it is not isolated instruction timing.

| Branches | Before: mean ns/frame across repeats | After: mean ns/frame across repeats |
| --- | --- | --- |
| 1 | 54.1–56.6 | 60.2–64.6 |
| 4 | 94.1–95.7 | 102.9–103.8 |

The median increase in this local sample is approximately 6.1 ns/frame for one
branch and 8.1 ns/frame for four. This is a measured hardening cost, not an
optimization claim. Full-cycle means were 759–869 versus 800–896 ns/frame for one
worker and 979–1,150 versus 895–1,448 for four; the spread prevents a stable
full-cycle regression estimate on this shared host. Raw results retain the worker
latency distributions, all offered/accepted counts, binary hashes and CPU mapping.
No round-robin candidate was benchmarked or adopted.

The report distinguishes finite-model exploration from unsafe storage execution.
Loom does not compile raw storage; the small critical models are exhaustive within
their model and other models use preemption bound 2, branch limit 10000, without
permutation or duration caps. TSan tests the explicit final Acquire-load variant;
Loom also tests the ordinary Acquire-fence variant. Miri uses strict provenance,
Stacked and Tree Borrows with seeds 1,17,42 on the selected lifecycle groups.
No new full coverage, fuzz, MSan, cross-target, long soak or independent external
unsafe audit is claimed. Local performance samples do not establish a production
regression gate or a hard latency bound.
