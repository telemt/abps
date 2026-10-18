# Adversarial lifecycle and rights conservation, 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

The ABPS-001..032 review is implemented as an executable verification matrix for
Berry and Mosaic. It reuses existing regressions where they already express the
contract, and adds independent oracles, targeted histories and mutation controls
where they do not. Production allocation, reference counting, return ordering,
public API and root dependencies are unchanged.

## What the new checks establish

The [registry](../../../verification/matrix.json) links all 32 review scenarios to existing
MEM/OWN/LIFE/SI/BR/AP invariants, preconditions, precise test selectors, backend
applicability and negative controls. Its 72 check entries and 26 detector entries
are not counts of distinct tests or explored concurrent schedules. Berry has no
Mosaic branch, segmented grant-node or ZeroPayload API; those entries explicitly
record that difference rather than inventing a common interface.

The [concrete oracle](../../../tests/support/rights.rs) predicts retained allocations,
byte admission, reference rights, domain records and pending maintenance from
logical ownership. It does not call production protocol helpers or use observed
refcounts to predict outcomes. Quiescent observations check every result, payload,
initialized prefix, grant count and domain incarnation against that model. The
legacy reference comparison remains intact; new issuer histories never insert a
maintenance call to hide a pending record.

The new [adversarial suite](../../../tests/adversarial.rs) targets last-slot, reference
limit, closing-domain, pending-maintenance and grant-pressure states. Fixed
histories assert non-vacuous reachability independently of random generation.
Stable IDs survive JSON replay and dependency-preserving shrinking: removing a
producer cannot silently redirect an operation to another handle. Failed histories
retain their original and minimized actions, backend, step, expected/observed
result, configuration, seed and source digest. Existing binary corpora retain their
old decoder; `adversarial_rights` is an additional fuzz target.

Metamorphic checks cover all 24 drop orders of four owners, batch splitting,
snapshot insertion, `poll_domains(0)`, composed writes and successful acquisition
prefixes under a larger budget. The last property uses fresh pools: histories that
accept additional requests need not have the same subsequent slot availability.

Private raw-storage tests exercise error and unwind at every existing admission
stage for both classes and reservation policies while neighboring allocations
remain live. Reused payload starts with zero filled/initialized length, and safe
initialization cannot expose an earlier `0xA5` pattern. A one-slot/one-record
history distinguishes publication from refund, tests both sufficient and exhausted
budget during that interval, and forbids record reuse until old accounting ends.

Mosaic's [partition observer](../../../src/mosaic/raw/tests/partitions.rs) independently
walks copied nodes and rows with bounded visited sets. It checks exact coverage of
valid bits, disjoint grants, tail masks, class membership, cursors, free lists and
absence of lost/duplicated nodes or rows. Tests reach the legal node/row capacity,
exercise fragmented multi-class grants, and hold an old published return across
maintenance. Corrupted copies demonstrate that the observer rejects cycles,
overlap and duplicate ownership. Berry checks its simpler mask partition.

Loom runs the actual batch protocol for every materialization prefix of N=1..3
while another owner drops. A new segmented bitmap model selects publication on
either side of the first confirming RMW. A late publication into an already
confirmed word may coexist with a valid earlier depleted witness. Separate
no-join models still test memory visibility; scheduling channels in the witness
test are not presented as a visibility proof.

The real worker-fanout adapter now has a separate credit oracle for queue capacities
1/2/8, fan-out 1/2/20, and cancellation at each of five stages. Dequeue retains the
credit; transfer into a bounded writer batch refunds it while payload ownership
continues. Every step checks credits, FIFO and retained backing. Existing stale
generation and payload-before-permit cancellation tests remain in the suite.

## Campaign evidence

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The machine-readable record (`evidence/rights-verification-2026-10-10.json`) and
compressed commands/logs (`evidence/rights-verification-2026-10-10.raw.json.gz`) record
final and intermediate outcomes separately. Accepted campaigns use source digest
`a7630cfe60ba9213c54a81f4bfc7e73b7b62d3b05c380f91a0a8b3137b7af968`. The runner now
hashes adapter/example sources too and rejects changes to that identity during a
campaign. This checkout has no Git metadata; comparison used a file-by-file baseline and
SHA-256 identities.

| Check | Result and scope |
| --- | --- |
| Native | Seven feature configurations, format and Clippy with warnings denied; 1,035 native executions plus one compiled-configuration probe, and 73 doctest executions. An additional 18-doctest campaign records the compile-fail contract names explicitly. |
| Long properties | Passed with seed 424242, 10,000 generated cases per property and an upper bound of 1,000 operations per history. Includes the new concrete oracle, existing reference/backend differential model and local-family model. Explicit optimized test build; ordinary native tests remain debug builds. |
| Loom | 31 model tests per reference-acquire variant, plus two configuration probes: 64 executions. Both completed with no preemption/permutation/duration cap; the 10,000-branch guard was not exceeded. A test can invoke several finite models; 31 is not a schedule count. |
| ASan / TSan / MSan | Each passed 103 selected executions plus one compiled-configuration probe. Includes actual raw storage, generated histories, branches, snapshots, native stress and worker-fanout adapter tests. No sanitizer reports. TSan selects Acquire-load; native/ASan/MSan select the fence. |
| Miri | 194 executions passed under strict provenance with Stacked Borrows and Tree Borrows, seed 1. Five focused publication/refund and no-join teardown cases also passed under both models with seeds 17 and 42: 20 additional executions. Ordinary leak checks stay enabled; only exact registered intentional-retention tests use separate `ignore-leaks` runs. |
| Directed mutations | All 34 controls detected: 14 existing and 20 new. Includes lost publication/refund/reference/claim/credit, stale initialization, repeated generation, overlapping grants and Relaxed publication under both reference-acquire variants. No timeout, build failure or abort counted as a detection. |
| Fuzz | Four independent bounded smoke campaigns completed: 23,753 ownership, 17,446 segmented-boundary, 102,578 local-branch and 11,760 concrete-rights inputs; each stopped after 21 seconds. No failure artifacts. |
| Allocations | Five existing probes and their allocation-positive controls passed. Prepared core and measured synchronous worker dispatch regions retained zero allocation/free/reallocation results. This excludes task creation, sockets and scheduling. |
| Registry / harness | All 72 selectors resolved; all check selectors referenced by the 32 review rows have successful native or Loom execution evidence. Twenty-six runner tests passed. The lexical unsafe inventory remains 79 sites. |

Merged native line coverage is 95.71% (4,199/4,387 lines), with 95.16% region
coverage. Berry/Mosaic protocol line coverage is 100%/99.28%; raw storage is
94.89%/95.52%. Local-reference protocol coverage is 100%; branch storage is 91.71%,
with its uncovered lines in formatting methods. The remaining Mosaic protocol
line is the `Contended` CAS failure mapping, exercised by separate Loom models.
JSON, LCOV and missing-line output are retained. These are native coverage numbers,
not branch coverage or a proof that every unsafe execution has been explored.

The intermediate grant mutation exposed a detector problem: the corrupted arena
panicked again during cleanup and aborted the process. That result was recorded as
incomplete, not a killed mutation. The revised detector exercises the real mask
assignment protocol on plain cells without an arena destructor; the separate
arena lifecycle regression is retained. Other development artifacts, including a
missing optional-tool PATH, interrupted debug property run and Miri run spanning
source edits, are preserved and excluded from accepted final evidence.

## Reproduction and remaining limits

Use the pinned versions and commands in the [verification guide](../../verification.md).
The registry and runner distinguish registered scenarios, resolved selectors and
actual successful test executions. A compact reproduction starts with:

```sh
python3 scripts/verify.py --suite native --output verification/artifacts/rights-native
python3 scripts/verify.py --suite properties --profile nightly --output verification/artifacts/rights-properties
python3 scripts/verify.py --suite loom --profile release --output verification/artifacts/rights-loom
python3 scripts/verify.py --suite miri --profile smoke --output verification/artifacts/rights-miri
python3 scripts/verify.py --suite mutations --mutation-scope directed --output verification/artifacts/rights-mutations
```

Output directories must be new; `--build-root` can reuse separate tool targets on
an executable filesystem. Full commands, feature flags, tool versions and limits
are in the archived results. The additional compile-fail and focused Miri campaigns
record their exact Cargo commands there as well.

This is local Linux x86_64 evidence. Loom covers atomic protocol compositions,
not raw arena storage or Tokio internals. Miri and sanitizers cover selected
executions, and TSan checks the Acquire-load variant rather than native fences.
The sequential oracle is not an arbitrary concurrent-history linearizability
checker. Interrupted returns still retain pending accounting; these tests do not
introduce helping, forced reclaim or a new shutdown policy. Long soak,
cross-target execution, a new automatic-mutant campaign, dedicated-host performance
and independent unsafe review were not performed in this change. No production
readiness claim follows from the review checklist or coverage percentage.
