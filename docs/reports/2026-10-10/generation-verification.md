# Batch rights and slot generations, 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

This change makes the review's remaining proof obligations executable. It adds
checks for batch construction, reuse of one slot, and closed-domain completion.
The tested histories did not reproduce a violation of the existing production
protocol. Public APIs, memory orderings, allocation paths and dependencies are
unchanged; the only added protocol method exposes pending batch rights under
`cfg(test)`.

## Invariants and detectors

| Contract | Executable obligation |
| --- | --- |
| SI-10, OWN-02/03 | For N=0..3, every completed construction prefix satisfies `references = source + constructed handles + pending rights`. Ordinary guard destruction and unwind restore the source, destination and charge. A deliberately inserted panic detects a violation of the concrete handoff's no-unwind requirement. |
| OWN-03, SI-10 | Scalar and partially materialized batch rights flow through two remote releases, final Acquire, bitmap publication, next acquisition and `references.begin()`. The new generation observes both old reader markers before joining either thread. |
| OWN-02/03 | A retained source can attempt cloning while its peer releases. The slot remains unpublished while that source is live; the scenario never manufactures a clone from a zero count. |
| SI-01 | A positive hint remains valid only within the same issuer operation while returners add bits. A depleted result witnesses the confirming RMW and can be followed by publication before the caller's next observation. |
| LIFE-01/02 | Berry's closed two-allocation ledger cannot be reused before both counters drain. Subsequent old root decrements cannot alter the new allocation's ledger. |
| LIFE-01/02, SI-05 | Both backends retain a Closing record at generation `u64::MAX` until two remote allocations return. Closed acquisition attempts preserve accounting; bounded maintenance retires the record exactly once under reserved and unreserved policies. |

The batch transfer interval is documented in the canonical contract and storage
code. Bounds and pending-right checks occur before the transfer; concrete pointer,
integer and `Range<usize>` copies, replacement of a known empty destination, and a
bounded prefix increment cannot unwind. The executing frame carries the right
between pending decrement and completion. No new runtime guard, callback or
failpoint was added to that interval.

The shared Loom scenarios call each backend's actual protocol. Successful reuse
must be reached during exploration: host-only coverage flags are written after
assertions and inspected only after exploration. They supply no synchronization
inside the model. The separate Berry drain proof assumes closed admission and
monotone counters; it is not imposed on Mosaic's final-refund lifetime protocol.

The [safety case](../../../SAFETY_CASE.md), [canonical contract](../../../CONTRIBUTING.md),
[verification guide](../../verification.md) and [registry](../../../verification/matrix.json)
are synchronized. The registry now contains 90 selectors and 41 negative-control
entries. Those counts describe registration, not explored schedules or executions.
The unsafe inventory still has 79 unchanged sites; two line references moved with
new proof comments.

## Validation

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The summary (`evidence/generation-verification-2026-10-10.json`) and compressed commands
and logs (`evidence/generation-verification-2026-10-10.raw.json.gz`) record eight
successful campaigns for source identity
`0e66ace5458541ba2e46c8c0e6c6dab80da0b816ad6a650841e4f5b540a285fc`. The checkout has no
Git metadata; changes were reviewed against a file baseline.

| Check | Actual result |
| --- | --- |
| Native | Seven feature configurations, format and Clippy with warnings denied; 1,093 native executions, 73 doctests and one compiled-configuration probe. |
| Loom | 38 model tests for each of fence and Acquire-load, plus two configuration probes. Per variant: 62 finite model invocations without a preemption cap and 11 with a bound of two. Every new model is in the first group; the 10,000-branch guard was not reached. |
| Miri | Four new raw tests under strict provenance, Stacked Borrows and Tree Borrows, seeds 1/17/42: 24 executions. |
| ASan / TSan | 114 selected tests and one compiled-configuration probe per sanitizer; no diagnostics. Existing intentional-retention exclusions are recorded. |
| Directed mutations | All 49 controls detected by their named regressions, with an independently passing unmodified baseline. Thirteen controls were added for handoff, initialization, publication, final Acquire and drain. Build failures, aborts and timeouts do not count as detections. |
| Allocation probes | Five probes passed, including positive controls for allocation detection. |
| Registry / reporting | All 90 selectors resolve and all checks referenced by the 32 review rows have qualified execution evidence. Reporting tests: 39 ordinary and 39 optimized Python executions; the optimized unsafe inventory check passed. |

Development attempts are archived separately. A shared Cargo target used while
calibrating mutations reused stale artifacts; those results are excluded entirely.
The accepted mutation campaign builds each control in its own target directory.
Missing-tool setup and campaigns superseded by refreshed inventory line references
also supply no evidence for the final source identity.

## Reproduction and limits

Use new output directories with `scripts/verify.py`: native profile `pr`, Loom
profile `smoke`, ASan/TSan profile `pr`, and allocation, registry and directed
mutation suites. The archive retains exact commands and `run-focused-miri.py` for
the four-test Miri selection; adjust its workspace, build and output paths when
reproducing elsewhere. The ordinary Miri CLI selects the broader registered suite.

This is Linux x86_64 evidence with Rust 1.96.0 and `nightly-2026-10-03`. Loom checks
finite protocol models, excluding raw arena storage. Miri checks the four selected
storage scenarios. TSan uses the Acquire-load variant; native execution uses the
fence, and both variants have Loom coverage. Interrupted-return and abandonment
semantics are preserved.

No new MSan, fuzz, coverage, automatic-mutant, long-soak, cross-target, performance
or independent unsafe-review result is claimed. These checks strengthen the
argument for the tested transitions; they are not a production-readiness or general
soundness proof.
