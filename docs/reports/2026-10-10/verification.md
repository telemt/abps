# Verification program: local evidence, 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

Berry and Mosaic now have an executable verification registry, shared trace
decoders/oracles, failure injection, bounded stress, tool runners and scheduled CI.
The canonical behavior remains in CONTRIBUTING; [SAFETY_CASE](../../../SAFETY_CASE.md)
connects its invariants to local arguments and checks. This is experimental Linux
x86_64 evidence, not production admission or cross-platform validation.

The implementation adds test-only error/unwind points after guarded acquisition
stages. They disappear from ordinary builds. The shipped atomic ordering, ownership,
accounting, interrupted-return and abandonment protocols are unchanged. Generated
traces compare independently modeled capacity, admission, ownership and payload
against the safe reference, Berry and Mosaic, with explicit maintenance barriers.
Corpus replay and the fuzz targets execute those same decoders and oracles.

## Completed local checks

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The machine-readable record (`evidence/verification-2026-10-10.json`) preserves
commands, tool versions, limits, source digests and links to local logs. Counts below
are executions across configurations, not counts of distinct tests. Source digests
differ across development runs; the record lists them separately. Later additions were
tests and harness checks, not changes to the atomic protocol.

| Check | Evidence |
| --- | --- |
| Native | Seven explicit feature configurations; formatting, Clippy with warnings denied, 928 test executions and 73 doctest executions. |
| Registry | 42 nonempty selectors tied to canonical invariants; five allocation probes; 79 lexical unsafe sites indexed. |
| Properties | Five property tests, each with 10,000 cases, up to 1,000 operations, seed 424242; common backend traces and local-family oracle. |
| Loom | All 30 protocol models completed without preemption/permutation/duration caps. The 10,000-branch guard was not exceeded. Exhaustive only within these finite models; raw storage is excluded. |
| Miri | Strict provenance, seeds 1/17/42, Stacked Borrows and Tree Borrows; 462 executions. Ordinary tests retain leak checking; the ten exact intentional-retention selectors run separately. |
| ASan / MSan | 76 executions each, std rebuilt with instrumentation; MSan also instruments cc-rs native helpers with target-specific Clang flags. |
| Allocation probes | Reference positive control and Berry/Mosaic/branch/worker-fanout probes completed. Prepared paths assert zero allocations, frees and reallocations in their defined measurement regions. |
| Fuzz | Three targets, 21 seconds each, 300,550 total inputs, no crashes. This is smoke evidence, not the configured hour-long campaign. |
| Stress | Release-mode 10,000 successful allocation rounds per case, 1/2/4/8/16 returners, two slot layouts, 100 domains and bounded queues; separate three-worker family reuse; complete drain. About 28 seconds, not a long soak. |
| Mutations | Ten directed negative controls caught. Component campaigns caught all 40 grant mutations and 11 local-reference mutations, with no survivors, timeouts or unviable builds. |
| Criterion | Functional benchmark smoke completed; no timing regression claim on this host. |
| Runner / CI files | Fourteen runner tests, unsafe-inventory validation, YAML parsing and document-link checks. Remote CI has not been run here. |

The merged native source-file line coverage is 95.26%. Berry/Mosaic protocol files
are 100%/99.27%; their raw storage files are 94.34%/95.30%; branch protocol is 100%
and branch storage is 91.71%. Inline unit-test code can contribute to file coverage.
These percentages are diagnostics, not proof that every unsafe transition was
exercised. Missing-line reports remain part of the artifact set. Coverage found
missing branch I/O/origin checks and grant preflight cases; both now have regressions.

The initial automatic mutation run applied every mutation to the whole test suite
and finished with 46 detected mutations and five timeouts.
Some mutated search/release operations hung unrelated lifecycle tests after another
test had already detected the violation. Those timeouts are retained as unsuccessful
evidence. Component mutation campaigns now use independently bounded class-access
monitors and direct local-reference checks. Full unmutated lifecycle tests remain
separate; this does not weaken the production progress contract.

## TSan finding remains visible

The original TSan campaign is **failed**. Test assertions completed, but TSan
reported races in raw branch teardown before worker join and in native stress
(slot/sidecar reuse and payload writes). Its model/property/snapshot groups passed.
No report is suppressed or relabeled as success.

The relevant edge is a nonfinal reader's Release decrement followed by the final
reader's Acquire fence, capacity publication/refund, and issuer Acquire observation.
Rust documents that [TSan does not support atomic fences](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html#threadsanitizer).
In a separate source/target copy, changing only `References::release`'s decrement
from Release to AcqRel in both backends made all five TSan groups pass. That stronger
ordering is a diagnostic control and is **not shipped**. It supports the missing-
fence-instrumentation explanation; it does not independently prove soundness or
replace the unchanged protocol's Loom/Miri evidence. Review this disposition before
using TSan as a release gate; the committed runner keeps the original result red.

## Remaining release evidence

Scheduled deep/weekly campaigns, hour-long fuzz/soak, dedicated-host performance
comparisons, other target architectures and independent unsafe review remain open.
Configured workflows and an unsafe-site inventory are not evidence of their execution
or review. Use the [verification guide](../../verification.md) to reproduce each scope and
retain failed/incomplete outcomes alongside successful ones.
