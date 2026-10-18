# TSan synchronization follow-up, 2026-10-10

> **Historical evidence.** Results below belong to the source and toolchain
> recorded in this report, before the [final-refund correction](../../final-refund.md).
> They do not validate the current revision or discharge its terminal-borrow
> obligation. Historical CI references do not describe configured workflows
> in this checkout.

Berry and Mosaic now use an Acquire load after the final reference decrement in
TSan builds. Ordinary builds keep the existing Release decrement and final Acquire
fence. No public API, Cargo feature, dependency, admission, accounting or teardown
policy changed. The [original failed campaign and AcqRel diagnostic control](verification.md#tsan-finding-remains-visible)
are retained unchanged; this report records the subsequent fix.

## Change and proof boundary

The private `abps_tsan` cfg is selected automatically by `build.rs` from the target's
`CARGO_CFG_SANITIZE`. Native, ASan and MSan configuration probes observed `fence`;
TSan observed `acquire-load` with target sanitizer `thread`. Direct Cargo sanitizer
builds select the same path, without a public feature or manually supplied cfg.
Loom alone explicitly forces the cfg to model the load variant without TSan.

At reference count 1->0, no live or reserved reference can increment the counter.
The slot is not yet published, so the issuer cannot reinitialize that counter.
The final Acquire load observes zero in the prior readers' RMW release sequences,
before bitmap publication and final refund. This provides the required reader
completion edge without changing every decrement to AcqRel. The argument and
invariant IDs OWN-03, SI-02/11 and BR-03 are recorded in
[the safety case](../../../SAFETY_CASE.md#atomic-edges-and-progress).

There are no race suppressions. The runner now rejects TSan reports even when
`TSAN_OPTIONS=exitcode=0` makes the process exit successfully. Its regression failed
before the classifier fix and passes afterwards. Build-script content is included
in source identities and disposable mutation trees.

## Completed checks

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The machine-readable record (`evidence/tsan-2026-10-10.json`) and compressed
commands/logs (`evidence/tsan-2026-10-10.raw.json.gz`) preserve all outcomes. Final
campaigns below share source identity
`8a31b45f27c99446ce357b7e87e321af1062316e2cb9ae0f7805c9e0ebec9dea`. Counts are
executions, not distinct tests or explored schedules.

| Check | Result and scope |
| --- | --- |
| TSan | All five groups passed: 51 raw-storage, 18 model, 2 branch, 2 snapshot and 3 stress executions; 76 total plus one compiled-configuration probe. No warnings. Includes teardown before worker join and slot/sidecar/payload reuse. |
| Loom | 30 models in each variant, plus two configuration probes. Both runs completed without preemption/permutation/duration caps; the 10,000-branch guard was not exceeded. Raw storage remains outside these models. |
| Native | Seven feature configurations; format and Clippy with warnings denied; 935 test executions plus one configuration probe, and 73 doctest executions. |
| Miri, final lifecycle | Six no-join teardown/reuse scenarios under both Stacked Borrows and Tree Borrows: 12 executions, seed 1, strict provenance, leak checks enabled. |
| ASan / MSan | All five groups passed for each sanitizer: 76 executions plus one configuration probe each. Rust std rebuilt; MSan native helpers instrumented with Clang. |
| Directed mutations | All 14 detected by their named regressions, including missing fences and Relaxed final loads for both backends and local-family reuse. No timeouts or compile failures counted as detections. |
| Allocation probes | All five probes passed, including positive controls; prepared experimental paths retained zero allocation/free/reallocation results in their measured regions. |
| Registry / harness | 44 selectors resolved; 19 runner tests passed; all 79 lexical unsafe sites unchanged. |

Earlier development results remain in the archive: a Clippy error in the new cfg
probe and an overly strict anchor check in the mutation harness were fixed. The
broader Miri smoke run (154 executions) and first dual-variant Loom run passed on
an earlier source identity; only verification code changed before final checks.
They are recorded separately and are not presented as additional final-source runs.

## Reproduction and limits

```sh
python3 scripts/verify.py --suite tsan --output verification/artifacts/tsan
python3 scripts/verify.py --suite loom --profile release --output verification/artifacts/loom
python3 scripts/verify.py --suite mutations --mutation-scope directed --output verification/artifacts/mutations
```

Use new output directories; an existing executable build filesystem can be selected
with `--build-root`. Tool versions and complete commands are in the record.
This is Linux x86_64 evidence. TSan checks the Acquire-load variant; it cannot
instrument the native fence. Loom checks both synchronization variants but not
unsafe storage. No new long soak, performance claim, independent unsafe review or
production admission follows from this fix.
