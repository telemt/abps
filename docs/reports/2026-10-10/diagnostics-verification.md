# Diagnostics lifetime and preparation rollback — 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout. This report includes local-only `tests/issuer_diagnostics.rs`,
> `tests/prepare_failures.rs` and `scripts/check-diagnostics-mutations.py`;
> those files and their registry additions are absent from upstream `668d2f3`.

The audit identified missing execution evidence around the separate diagnostic
allocation and failed preparation. This change adds regression tests and directed
negative controls for those boundaries in Berry and Mosaic. The tests did not
reproduce a production lifetime or accounting defect. Production source, public
APIs, layout, memory orderings and bounded-attempt error semantics are unchanged.

## Executable obligations

| Invariant | Test and independent observation |
| --- | --- |
| LIFE-02, OWN-03: an observer retains its diagnostic record independently of arena storage | Two remote observers clone, sample and drop while the owner destroys the arena, alternating explicit `try_destroy()` and owner Drop. Identity and abandonment fields remain valid; after all competitors join, cloning must succeed and destruction must be observable. The last observer may drop on another thread. |
| LIFE-02: abandonment remains observable after the last buffer returns | A separate test destroys the owner with a live buffer, verifies the remote payload/return, and retains diagnostic identity, abandoned bytes and event-time admitted charge. It does not expect later buffer drops to reclaim the abandoned arena. |
| MEM-01: failed prepare exposes no allocation and releases its partial storage | A separate integration-test binary fails allocation one and allocation two of the real `prepare()`. It checks `AllocationFailed`, the exact allocation attempts and physical frees, then immediately prepares successfully and exercises payload ownership. |
| LIFE-02: arena and diagnostics have separate destruction barriers | The allocator observer checks allocation sizes against public reservation, exact deallocation layouts, arena free on both teardown paths, no diagnostic free while either observer survives, and exactly one diagnostic free after the last remote observer drops. |
| OWN-03: diagnostics may cross threads and be shared | The existing trait-contract test now requires `Diagnostics: Send + Sync` for both backends. |

`tests/issuer_diagnostics.rs` uses safe public APIs. The allocation detector in
`tests/prepare_failures.rs` is isolated from other tests in its binary. A const,
destructor-free thread-local flag arms only the synchronous prepare call; harness
and worker-startup allocations cannot consume its failpoint. The hooks use two
fixed atomic records, forward the original pointers/layouts to System, and never
allocate, log, lock or panic. Numeric pointer addresses are compared only. Clearing
an address at its first free avoids confusing later unrelated address reuse with
another free of the observed allocation. Assertions run outside the hooks.

The physical-free checks retain observers in `ManuallyDrop` until their lifetime
assertions pass. A deliberately premature-free mutation therefore fails a named
assertion before a freed record is read or dangling observers are dropped during
unwind. These precautions belong to the test harness, not the production API.

## Mutation sensitivity

`scripts/check-diagnostics-mutations.py` applies each mutation in a disposable
source tree and a fresh target directory. Its unmodified named detector must pass
before the mutation is accepted as detected. Build failures, crashes and timeouts
do not count as a killed mutation.

| Mutation, applied to each backend | Required failure |
| --- | --- |
| Omit diagnostic reference increment | A remaining observer loses its physical lifetime right. |
| Free the diagnostic record on a nonfinal release | Owner/observer release frees storage while another observer still exists. |
| Omit arena cleanup after diagnostic allocation failure | The failed prepare leaves the arena allocation outstanding. |

All six mutations were detected by `mem01_prepare_and_diagnostic_allocations`,
with six successful unmodified baselines. The new checks are registered against
MEM-01/LIFE-02/OWN-03 and mapped to the diagnostic and preparation unsafe sites.
The production unsafe inventory remains 87 sites; the new test-only allocator
instrumentation has its own local safety obligations.

## Evidence and limits

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The summary (`evidence/diagnostics-verification-2026-10-10.json`) and compressed logs
(`evidence/diagnostics-verification-2026-10-10.raw.json.gz`) record commands, compiler
identities, features, source digests, test names and outcomes. Accepted source digest:
`46ec95cbba12ef564e2e0978cb033ac18a8be6b058888bb86aaaa64f0e90f6a3`. Baseline digest:
`404810774e8ce23bd8544770edf8a736c5577c6306846391bdb5d1bd8d2d5e6f`. There was no Git
metadata. A baseline copy and unified patch retain the changes.

Rust 1.96.0 and nightly-2026-10-03 ran on x86_64-unknown-linux-gnu. Counts below are
executions, including repeated features/seeds, not distinct tests.

| Campaign | Result |
| --- | --- |
| Native feature matrix | 1,175 tests + 73 doctests + one synchronization probe = 1,249 executions; fmt and Clippy passed across seven configurations. |
| Focused release | Eight executions across diagnostics, preparation failures and trait contracts, with debug assertions disabled. |
| Focused Miri | Five tests × two alias models × three seeds = 30 executions. Strict provenance, Stacked/Tree Borrows, seeds 1/17/42. |
| Focused ASan | Five tests plus one actual-instrumentation probe = six executions. |
| Focused TSan | Five tests plus one actual-instrumentation probe = six executions; checks the existing Acquire-load variant. |
| Directed mutations | Six of six killed; each unmodified baseline passed. |
| Verification harness | 43 tests under ordinary Python and 43 under optimized Python; registry and all 87 unsafe-site mappings validate in both modes. |

Leak checking stays enabled for normal observer lifetime and failed-preparation
tests. Only the two exact registered abandonment tests use Miri leak suppression
or ASan `detect_leaks=0`. Abandonment is intentionally retained by LIFE-02 and is
not a cleanup success. No TSan race suppression was added.

Two earlier attempts are preserved separately and excluded from accepted totals.
The first native campaign completed commands but failed its final source-identity
check after the test coverage was extended during the run. The first mutation
campaign reused one target across copied sources: Cargo retained a prior mutant's
binary and the next unmodified baseline failed. The script rejected that result;
fresh per-case target directories fixed the isolation, and all six cases were
rerun. Neither attempt is presented as a successful full campaign.

The concurrent tests exercise storage integration and a finite set of schedules.
Barrier/start synchronization and final joins are explicit; they do not establish
an exhaustive ordering proof. The unchanged reference protocol retains its prior
Loom evidence; no new Loom exploration is claimed here. This follow-up does not
rerun the entire Miri/sanitizer matrix, coverage, fuzzing, cross-target checks,
long soaks or performance campaigns, and does not establish production readiness.
