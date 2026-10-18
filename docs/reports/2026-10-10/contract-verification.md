# Contract verification, 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

This change strengthens the tests and the evidence used to accept them. It adds
accounting/lifecycle histories, partial-I/O and cancellation cases, deterministic
search-work bounds, and stricter counterexample shrinking. The production
allocation protocol, unsafe implementation, public API and dependencies remain
unchanged.

## Reproduced reporting defects

The original reporting code could credit a selector using a matching name from
another binary or configuration, accept one synchronization variant for a row
requiring both, or complete an empty/previously unsuccessful campaign. Sanitizer
diagnostics followed by a zero exit code could be classified as success, and
registry assertions disappeared under optimized Python. A further regression
showed that running one member of a registered group could satisfy the whole
group. The failing reproductions are retained in the evidence archive.

The runner now qualifies executions by source, Cargo target, resolved features,
compiler, platform, tool, build configuration and reference-acquire variant. It
discovers the whole target, records the selected and completed names, and requires
every member of a registered group. Required steps are declared before execution;
earlier failures, missing work and source changes prevent successful completion.
Sanitizer diagnostics and failed test summaries override a zero exit code. Registry
and inventory validation remain enabled under `python -O`.

Directed mutations first run their exact named test against the unmodified source.
Only a compiled mutation that fails that test counts as detected. Ambiguous patch
anchors, timeouts, build failures and process aborts retain separate outcomes and
logs. One existing supervisor mutation had an ambiguous anchor; it now targets
the refund guard specifically. The supervisor itself did not change.

## New executable obligations

| Contract | Scenario and independent observation |
| --- | --- |
| MEM-02, LIFE-02, SI-05 | Each admission fault stage is followed by close, remote final return, explicit maintenance and record reuse under both policies. Charge/claims return correctly, the new incarnation starts uninitialized, and a neighboring live allocation remains intact. |
| OWN-01/02, PROG-01, LIFE-02 | Concrete histories reach budget, reference, class-word and grant boundaries. Exact sequential errors, tiny/empty views, invalid ranges, pinned old ownership and recycle are compared with the rights oracle. |
| IO-01/03, SI-04, AP-06 | Scripted readers/writers drive the real adapter helpers through partial prefixes, pending/wakeup, EOF, errors and cancellation. Tests check wire bytes, framing, completed frames, queue cleanup and unaffected neighboring ownership. |
| PROG-04, SI-11/12 | Mosaic search counts word visits for successful and empty masked paths, multiple cursors and prepared sizes. A redundant-pass mutation is rejected without relying on timing. |
| Verification contract | Shrinking preserves backend, postcondition, operation and logical-owner identity. An unrelated panic with the same text cannot replace the original defect; unknown panics keep the original trace. |

The [registry](../../../verification/matrix.json),
[safety case](../../../SAFETY_CASE.md) and
[verification guide](../../verification.md) connect these scenarios to the existing
invariants. There are 79 registered selectors and 28 negative-control entries;
these are registration counts, not counts of explored executions. Every check
referenced by the 32 review rows has qualified execution evidence in this campaign.

## Accepted campaign

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The summary (`evidence/contract-verification-2026-10-10.json`) and compressed records
and logs (`evidence/contract-verification-2026-10-10.raw.json.gz`) share source identity
`23de9747ce6bf24b2c488c844a2d8f0418c16baf651bbd51f920ba391ee4bcb8`. All eleven accepted
campaigns finished with that unchanged identity. This checkout has no Git metadata;
review used a file baseline and SHA-256 identities.

| Check | Actual result and scope |
| --- | --- |
| Native | Seven feature configurations; format and Clippy with warnings denied; 1,077 native test executions, one compiled-configuration probe and 73 doctest executions. |
| Reporting infrastructure | 39 tests passed normally and 39 under optimized Python. The optimized inventory check passed with 79 unchanged lexical unsafe sites. |
| Properties | 32 test executions; generated properties use 128 cases, up to 250 operations per history, seed 424242. Includes the independent rights oracle and existing differential/local-family models. |
| Loom | 31 model tests for each of the fence and Acquire-load variants, plus two configuration probes. Each variant completed 52 finite model invocations without a preemption cap and 11 with a bound of two; the 10,000-branch guard was not exceeded. These are model invocations, not schedule counts. |
| Miri | Five focused scenarios under strict provenance, both Stacked Borrows and Tree Borrows, seeds 1/17/42: 30 executions. Covers fault/close/reopen for both backends, bounded search, boundary histories and defect-preserving shrinking. |
| ASan / TSan / MSan | 110 selected tests plus one compiled-configuration probe per sanitizer; no reports. Covers raw storage, model histories, branches, snapshots, native stress and actual adapter tests. MSan used an optimized build with instrumented std/dependencies and native helpers; the selection was unchanged. |
| Directed mutations | All 36 detected by their named tests, each with a passing unmodified baseline. Includes the new redundant-search-pass and substituted-defect mutations. No timeout/build failure/abort counted as a detection. |
| Fuzz | Four bounded runs with seed 424242: 24,706 ownership, 23,029 segmented-boundary, 147,083 local-branch and 11,355 concrete-rights inputs. Requested 20 seconds per target; each reported completion after 21 seconds, without failure artifacts. |
| Allocations | Five probes passed, including allocation-positive controls. Prepared experimental core and measured synchronous dispatch regions retained their zero-allocation/free/reallocation checks. The allocating reference remains a control. |
| Registry | All 79 selectors resolved. Registration and successful scenario execution remain separate evidence. |

Counts include repeat execution under different features, models and seeds; they
are not distinct scenario counts. Stable compilation used Rust 1.96.0;
instrumented compilation used `nightly-2026-10-03`. Exact commands, toolchain
output, selections, full target catalogs and logs are archived.

Development runs are retained separately, including the initial failing reporting
regressions, the ambiguous mutation anchor, setup/build failures and campaigns
interrupted or superseded by reporting changes. None supplies missing evidence
for the final source identity.

## Reproduction and limits

Run from the repository root with new output directories:

```sh
python3 -B scripts/test_verify.py
python3 -B -O scripts/test_verify.py
python3 -B -O scripts/verification_inventory.py
python3 -B scripts/verify.py --suite native --profile pr --output verification/artifacts/contracts-native
python3 -B scripts/verify.py --suite properties --profile pr --seed 424242 --output verification/artifacts/contracts-properties
python3 -B scripts/verify.py --suite loom --profile smoke --output verification/artifacts/contracts-loom
python3 -B scripts/verify.py --suite mutations --mutation-scope directed --profile smoke --output verification/artifacts/contracts-mutations
```

The guide lists sanitizer, fuzz, allocation and registry commands. A reusable build
filesystem can be selected with `--build-root`. The archive also includes the exact
`run-accepted-miri.py` and `run-accepted-msan.py` drivers for the focused Miri and
optimized MSan selections, with their required-step records. Adjust their workspace,
output and build paths when reproducing on another machine. The ordinary Miri CLI
runs the broader registered selection.

This is local Linux x86_64 evidence. Loom executes the atomic protocol, excluding
raw arena storage and Tokio internals; some models are preemption-bounded. Miri
covers the five selected scenarios, not the whole crate in this final campaign.
TSan checks the Acquire-load variant; ordinary native execution retains the fence.
The sequential oracle is not a general concurrent linearizability checker, and
the structural failure key is not a proof that two failures have the same root
cause. Completed tests preserve the existing interrupted-return and abandonment
contracts.

No new coverage measurement, automatic-mutant campaign, long soak, cross-target
execution, dedicated-host performance result or independent unsafe review is
claimed. The results provide reproducible evidence for the tested transitions;
they do not establish production readiness or a general soundness proof.
