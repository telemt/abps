# Mosaic remote authority and return verification — 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

The review follow-up makes the remote buffer boundary explicit in the type used by
Lease and branch descriptors, and strengthens the integration checks around
publication, reuse, refund, diagnostics and domain reincarnation. No allocator UAF
or accounting defect was reproduced. Public APIs, atomic orderings and arena
layouts are unchanged; the private authority and its regression gates are new.

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

Accepted evidence uses source digest
`1c11b7d89e2ada28dca758a0be5353ff831d5a0825352fd28638552a52b1447e`. The preserved
starting tree is `ff9b94f3151187c099cbb3c7276636833e0b87961f6752f707ba7a59aaa5add1`.
There was no Git metadata. The summary
(`evidence/remote-authority-verification-2026-10-10.json`) records qualified executions,
bounds, compiler identities and exclusions. The compressed archive
(`evidence/remote-authority-verification-2026-10-10.raw.json.gz`) contains 1,058 files,
including logs, drivers, compiler rejection diagnostics, mutation outcomes, probe
results and the baseline diff. Its checksum is recorded in the summary. This report is
outside the runner's source digest.

## Structural access boundary

Mosaic Lease and BranchRecord now hold the private
[RemotePtr](../../../src/mosaic/raw/remote.rs), rather than issuer RootPtr. It exposes
immutable properties, owned slot metadata and atomic word/ledger access. The arena
pointer and Plan projection are private to the child module. Remote calls cannot
obtain Root or DomainRecord, access controls/cursors/grants, or perform teardown.
Ledger projection creates a reference to the atomic field directly, excluding the
enclosing record's issuer Cells. Family records remain accessible only under their
documented construction/local-ownership obligations.

The transitive audit covers Lease clone, slot/origin/data access, return preparation,
BufMut writes and freeze, Buf scalar/batch/view operations, and local-family final
release. Return operands are captured before bit publication. PublishedReturn
still has no slot reference; final refund remains the last arena access. The
[safety case](../../../SAFETY_CASE.md#remote) records field, aliasing, lifetime and
synchronization obligations. The authority occupies one pointer, Lease remains
two words, and the matched probe fixtures retain identical reservation layouts.

The [compiler gate](../../../scripts/check_remote_authority.py) first builds the valid
boundary, then requires eight exact failures for Root, DomainRecord, pin, cursor,
destroy, drain, raw pointer and private Plan access. It runs as a required native
verification step. Setup errors, unrelated diagnostics, extra errors or a missing
build result cannot satisfy the gate. Its diagnostic parser also has regression
coverage under ordinary and optimized Python. These checks constrain available
methods; pointer validity still depends on the documented unsafe obligations.

## Lifecycle and rollback checks

| Contract | Executed regression |
| --- | --- |
| AP-01/02/03/05, SI-11, OWN-03 | `ap01_remote_authority_publish_reuse_refund_preserves_new_metadata` moves BufMut to a worker, exercises write/freeze/batch/slice/view access, stops at zero references before publication and again after publication, then reuses the slot in another domain. Old refund must preserve new origin, three references, immutable ownership and payload. Recycling, destruction and diagnostics are checked before joining the worker. |
| SI-11, AP-02/03 | The existing exhaustive `si11_reuse_before_old_refund_keeps_old_charge_pinned` Loom composition now retains three new-generation rights while old refund completes and verifies the ledgers/reference state before join. Both final-reference synchronization variants run. Reader visibility remains separately covered by the existing models without observer barriers. |
| MEM-02, OWN-03, LIFE-01/02 | The shared `abps_fault_close_remote_return_reopen_preserves_neighbor` history now covers both Err and unwind at every guarded admission stage, followed by close, remote return, maintenance and a new domain incarnation. Existing all-class admission tests retain all four stages and both grant modes. |
| LIFE-01/02, SI-05/13 | New shared `abps_all_records_close_remote_permutations_and_one_record_poll` fills configured domain and slot capacity, closes every domain while buffers remain live, and tests every return permutation with poll budget one. Each ready record must be reached within a FIFO cycle, exactly one record recycles, neighboring payload survives, and all records reopen with advanced generations. Native fixtures use four records; Miri uses three. |
| AP-05, LIFE-02 | The paused integration test samples references=0 with an unpublished bit, then available capacity with old admitted charge. It checks fresh metadata after reuse and owner diagnostics after destruction. Observations are exact only at these controlled boundaries; ordinary snapshots remain independently timed samples. |

Close is legal with outstanding ownership. **Recycle and destroy**, rather than
close itself, must wait for complete refund and the required control drain. A
published slot does not prove its old domain is drained. Domain generation
exhaustion, grant reassignment barriers and initialization/view contracts remain
in the executed native matrix and applicable sanitizer selections.

The actual [branch storage](../../../src/mosaic/raw/branches.rs) and
[branch protocol](../../../src/mosaic/protocol/branches.rs) are included in the audit.
Branch descriptor preparation, local-family origin reads and last-family release
use the restricted authority. Focused Miri includes sidecar reincarnation, paused
local/global return and every branch construction unwind prefix.

## Results and bounds

Native validation used Rust 1.96.0; instrumented validation used
`nightly-2026-10-03` on `x86_64-unknown-linux-gnu`. Counts are executions across
configurations, aliases and seeds, not distinct scenarios.

| Campaign | Completed evidence |
| --- | --- |
| Native | 1,147 native tests, 73 doctests and one synchronization-configuration probe; fmt and Clippy across all seven supported native feature configurations. |
| Compiler boundary | Valid baseline plus all eight required access/privacy rejections. |
| Loom | 40 tests for each of fence and Acquire-load variants: 80 executions plus two configuration probes. Critical finite models are exhaustive; older models include preemption bound 2. Branch guard 10,000, with no permutation/duration cap. |
| Focused Miri | Eleven tests across eight selector groups, strict provenance, default and Tree Borrows, seeds 1/17/42: 66 executions. Ordinary leak checking stays enabled. |
| ASan / TSan | Each passed 127 test executions and one configuration probe. TSan validates the existing Acquire-load variant; native retains the Acquire fence. |
| Properties | 36 test executions; 1,000 generated cases, public trace bound 256 operations, seed 424242. |
| Fuzz | Five targets, 20-second budgets each, 316,888 aggregate executions. |
| Directed mutations | All 14 selected refcount/return controls compiled and failed their named detector; every unmodified detector baseline passed. This is a focused subset of the larger registry. |
| Allocation checks | All five prepared-path probes passed with their existing positive controls. |
| Infrastructure | 42 reporting tests in each Python mode; inventory checks account for 87 lexical unsafe sites, including the new remote module. |

All 102 registry checks and all 32 review checklist mappings have qualified
execution evidence on this source. This means each mapping has an applicable
execution; it does not assert that every tool ran every registered check.

## Local depleted-path measurements

The existing `mosaic_probe` was built for the saved baseline and current source
with identical features. Builds completed before measurement. The probe fills
the class and leaves byte-budget headroom, so the measured error is confirmed
class depletion rather than BudgetExceeded. Preparation and teardown are outside
the measured loop; every fixture confirms full drain.

Thirty-six interleaved local runs cover 1, 32 and 256 words, unreserved/reserved
policies, three repetitions and both versions, with 10,000 measured attempts per
run. Source, compiler, binary checksums, CPU affinity, commands and full histograms
are retained in the summary. The reported work bounds are protocol formulas,
separately covered by counted-search tests, rather than measured machine steps.

At 256 words, observed p50 ranges were:

| Policy | Baseline | Current |
| --- | --- | --- |
| Unreserved | 611–621 ns | 591–601 ns |
| Reserved | 892–912 ns | 902 ns |

These shared-host samples do not establish an improvement or regression. Current
maximum samples reached about 1.6 ms and p99.99 remains insufficiently sampled.
The declared ordinary bound is 512 probes and 258 RMWs for 256 eligible words;
it is not a wall-clock limit. README explicitly separates bounded work from
cache coherence, scheduling and page faults.

## Reproduction and limits

Use new output directories with the [verification runner](../../verification.md).

```sh
python3 scripts/verify.py --suite native --output verification/artifacts/remote-native
python3 scripts/verify.py --suite loom --output verification/artifacts/remote-loom
python3 scripts/verify.py --suite properties --profile pr --cases 1000 --operations 256 --seed 424242 --output verification/artifacts/remote-properties
python3 scripts/verify.py --suite asan --output verification/artifacts/remote-asan
python3 scripts/verify.py --suite tsan --output verification/artifacts/remote-tsan
cargo run --locked --release --features local-branches-spike,bytes-interop --example mosaic_probe -- --case=empty --words=256 --capacity-policy=reserved --iterations=10000
```

The archive includes `run-accepted-miri.py`, `run-accepted-mutations.py` and
`run-scans.py` with the exact focused selections and commands. Replace their
artifact paths with new directories when replaying. The full Miri and mutation
runner suites are broader than this campaign.

Development corrections remain visible in excluded logs: the compiler harness
initially omitted manifest-declared example files; the FIFO fixture incorrectly
equated logical buffer numbers with physical record indexes; optional tool
executables were initially absent from PATH. These were corrected before accepted
runs. A later safety-case wording change invalidated the earlier source digest;
all accepted campaigns were rerun on the final digest rather than promoted.

No new coverage, MSan, automatic mutation, cross-target validation, long soak,
dedicated-host benchmark or independent external unsafe audit was completed.
Loom excludes unsafe arena storage; Miri covers the selected executions. Stopped
returns may retain charge and abandonment retains storage under the existing
contract. The evidence improves the experimental safety case and does not declare
production readiness.
