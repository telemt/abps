# Grant, lifetime and sidecar verification — 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

The review follow-up adds independent grant accounting, shared teardown models,
sidecar reuse checks, configuration exploration and diagnostic measurement probes
for the experimental Berry and Mosaic backends. No new allocator memory-safety
defect was reproduced. Public APIs, atomic orderings and the no-helping return
policy are unchanged. This campaign does not establish production readiness.

All accepted executions use source digest
`ff9b94f3151187c099cbb3c7276636833e0b87961f6752f707ba7a59aaa5add1`.
The preserved starting tree has digest
`0e66ace5458541ba2e46c8c0e6c6dab80da0b816ad6a650841e4f5b540a285fc`.
The workspace had no Git metadata; the evidence includes a comparison with that
baseline. The verification runner's digest includes implementations, examples,
tests, harnesses and the canonical contracts; this report is outside that digest.

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The machine-readable summary (`evidence/grants-lifecycle-verification-2026-10-10.json`)
records accepted campaigns, tool identities, bounds, exclusions and registry coverage.
The compressed evidence
(`evidence/grants-lifecycle-verification-2026-10-10.raw.json.gz`) contains 1,528 files,
including command logs, campaign drivers, mutation results and the source diff. Its
checksum is recorded in the summary. Only `complete-*` campaigns are accepted for this
source; the 22 earlier campaign records remain available as development or superseded
evidence.

## Changes and executable obligations

| Contract | Change and detector | Limit of the evidence |
| --- | --- | --- |
| SI-05/13: disjoint grant rights survive close, drain and record reuse | A shared [logical slot-set oracle](../../../src/test_support/grant_histories.rs) predicts rights independently of bitmap words. Both backends compare actual grants after completed actions; Mosaic additionally audits every sparse node and row through the [partition checker](../../../src/mosaic/raw/tests/partitions.rs). | Whole drained incarnations shrink independently. The existing public rights oracle retains interleaved histories and stable-ID trace reduction. |
| SI-13: grant opening changes nothing before resources are ready | Debug assertions check cached free-row and free-node sufficiency before the first control or record mutation. A regression corrupts each cached count, catches the assertion, restores it and checks the unchanged state. Existing preflight checks available rights. | This is a proof check under valid private invariants, not recovery or general rollback from arbitrary freelist corruption. No release-build scan or new public error was added. |
| SI-11, LIFE-01: all unfinished positive charges protect the arena | Raw teardown and [Loom](../../../src/mosaic/protocol/tests.rs) call the same private `root_drained` predicate. Three records and two returners cover both shared and separate ledgers, including a charged final record. Successful pre-join observations are mandatory. | Raw pointer destruction remains storage-test evidence; Loom explores the actual atomic predicate and transitions, not the unsafe arena. |
| BR-03/04, AP-02: local families finish record access before returning the global right | The [branch models](../../../src/mosaic/protocol/tests/local_branches.rs) reuse the same slot across family counts 1→2→1. [Storage tests](../../../src/mosaic/raw/branches/tests.rs) extend this to 1→2→1→3→2→1 and verify fresh initialization and payload isolation. | Host coverage flags reject vacuous exploration but add no synchronization inside the model. |
| SI-12, AP-08: depleted errors have an empty witness within the attempt | Masked observation models cover every cursor and publication word, preserve neighboring grants and assert the two-pass visit bound. Error documentation explicitly permits capacity to become available before the call returns. | Neither `Acquire` nor the confirming RMW promises that capacity stays empty until return. |
| MEM-01/02, OWN-01, SI-17: checked preparation cannot expose old bytes or exceed the accounted layout | An independent [range oracle](../../../src/test_support/layout_ranges.rs) checks alignment, disjoint typed regions, payload, padding and arena bounds. [Configuration tests](../../../tests/configuration.rs) and a new fuzz target exercise exact budgets and arithmetic boundaries. | Preparation is capped at 4 MiB in generated configuration cases; large successful plans are layout-only checks. Lazy/ZeroPayload do not promise resident pages or absence of later page faults. |
| AP-05/09: diagnostics have explicit cost and qualified evidence | The new [observability probe](../../../examples/spike_support/observability.rs) separates grant/page scans, busy teardown and mixed admission/remote-return measurements. | These local measurements validate the harness. They do not establish a production latency or throughput improvement. |

Grant tests cover native word boundaries: Berry stays within one word; Mosaic
includes W−1, W, W+1, 2W−1, 2W and 2W+1. The sparse structural audit checks
disjoint valid bits, exactly accounted nodes/rows, absence of cycles, valid cursors
and preservation of neighboring domains' payload. Failed opens and partial returns
must preserve rights until explicit maintenance completes recycling.

The full branch implementation is part of the review surface:
[raw/branches.rs](../../../src/mosaic/raw/branches.rs) and
[protocol/branches.rs](../../../src/mosaic/protocol/branches.rs). Branch construction,
local reference ownership, final global release and descriptor reuse are covered
by the tests above; they are not inferred solely from arena preparation code.

The canonical contract now states the positive-charge sum over unfinished
admissions, live allocations and final returns. It also records cold-path costs:
Mosaic drain is O(prepared domains); grant metrics traverse the relevant sparse
nodes. Diagnostic fields remain independent observations. No additional hot-path
atomic was added to distinguish final release from pending publication.

## Executed validation

Native validation used Rust 1.96.0 on `x86_64-unknown-linux-gnu`; instrumented
validation used `nightly-2026-10-03`. Counts below are executions, including repeated
feature combinations and seeds, rather than distinct scenarios.

| Campaign | Result |
| --- | --- |
| Native feature matrix | 1,135 native tests, 73 doctests and one synchronization-configuration probe passed; formatting and Clippy passed across the seven feature configurations. |
| Properties | 36 test executions passed across five groups; 1,000 generated cases, public trace bound 256 operations, seed 424242. Raw grant histories use their own bound of up to eight drained incarnations. |
| Loom | 40 tests passed for each final-reference synchronization variant: 80 executions plus two configuration probes. New drain, masked-search and branch compositions exhaust their finite models; older tests include preemption bound 2. The 10,000-branch guard was not exceeded; there are no permutation or duration caps inside the models. |
| Focused Miri | Nine selected tests across six selector groups passed with strict provenance, both default and Tree Borrows configurations, and seeds 1/17/42: 54 executions. |
| ASan | 124 test executions and one configuration probe passed. |
| TSan | 124 test executions and one configuration probe passed with the existing Acquire-load variant. Ordinary builds retain the Acquire fence; Loom exercises both variants. |
| Fuzz | Five targets completed their 20-second budgets: 471,798 aggregate executions. This is a short discovery run, not a sustained campaign. |
| Directed mutations | All 54 mutations compiled and failed their named regression; every unmodified detector baseline passed. Timeouts, build errors and exploration-limit failures are not counted as detections. |
| Allocation probes | All five existing probes passed, retaining allocation-harness positive controls. |
| Verification infrastructure | 41 reporting tests passed in each of ordinary and optimized Python; the inventory accounts for 79 lexical unsafe sites. |
| Local observability | All 18 release-built cases passed their accounting, drain and non-vacuity checks. |

All 99 registry checks have qualified execution evidence for this source. The 32
review checklist entries map to those checks; this is not a claim that every tool
ran every test. The registry contains 46 negative-control entries; the directed
campaign expands variants into 54 actual mutation executions.

The five added mutation detectors reject omission of the final domain from root
drain, loss of the free-node count, loss of the free-row count, premature grant
recycling, and loss of the last branch-family right. Existing publication,
refund, batch rollback and memory-ordering controls remain active.

## Diagnostic measurements

Berry/Mosaic common fixtures use two classes, 64 slots per class, 256 prepared
domain records and 16 seed domains. Mosaic's separate capacity fixture uses 129
slots per class, 4,096 records and 64 seed domains. Grant scans, domain pages and
busy teardown are sampled separately. Busy teardown first drops control handles,
so the retained allocation actually exercises Mosaic's ledger scan.

Mixed measurements use one returner and a bounded channel. Diagnostic intervals
0, 1 and 32 are admission-attempt counts. A completed startup exchange and at
least one successful admission are required. Empty buffers contribute zero useful
payload; requested bytes and charged capacity are reported separately. Timings
include startup, drain, channel and clock overhead. The archive retains source,
compiler, binary build identity and CPU affinity. A dedicated-host baseline and
sufficient tail samples are still needed to assess diagnostic overhead in service.

## Reproduction and remaining limits

Use new output directories with the [verification runner](../../verification.md). The
accepted commands, environments and selectors are retained in the archive. Typical
replays for this campaign are:

```sh
python3 scripts/verify.py --suite native --output verification/artifacts/replay-native
python3 scripts/verify.py --suite properties --profile pr --cases 1000 --operations 256 --seed 424242 --output verification/artifacts/replay-properties
python3 scripts/verify.py --suite loom --profile smoke --output verification/artifacts/replay-loom
python3 scripts/verify.py --suite asan --output verification/artifacts/replay-asan
python3 scripts/verify.py --suite tsan --output verification/artifacts/replay-tsan
python3 scripts/verify.py --suite fuzz --seconds 20 --seed 424242 --output verification/artifacts/replay-fuzz
python3 scripts/verify.py --suite mutations --mutation-scope directed --output verification/artifacts/replay-mutations
python3 scripts/observability-smoke.py --output verification/artifacts/replay-observability --build-root /tmp/abps-observability-replay
```

`run-complete-miri.py` in the archive records the focused six-group driver. It
uses the normal runner's discovery, tool identity and required-step checks. The
54 Miri executions above are not a claim that the entire Miri suite ran on this
source. Wide grant-boundary coverage comes from native and sanitizer runs; the
focused Miri generator uses smaller configurations.

Development runs exposed test/harness problems that were corrected before the
final campaigns: a bitmap fixture confused a slot count with a bit mask; the
property-runner test needed the two added groups; the short measurement probe
needed a startup barrier and separate payload/charge accounting; and concurrent
uncaptured Loom output could splice result lines. Captured `--show-output` retains
model bounds while allowing exact discovery/execution matching. Failed and
superseded logs remain in the archive and do not contribute accepted counts.

This work adds no new coverage report, MSan run, automatic mutation campaign,
cross-target validation, long soak, dedicated-host performance result or external
unsafe review. Interrupted returns may still retain charge indefinitely and owner
abandonment still intentionally retains the arena. Those documented guarantees
are tested without adding helping or making remote drops perform teardown.
