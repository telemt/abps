# Final-refund borrow lifetime and upstream integration

This guide describes upstream
[`668d2f3`, UB on final Refund fixed](https://github.com/telemt/abps/commit/668d2f3f1f20ce726e06c87e2b809b7acca19373),
whose parent is `f53f542`. The normative obligations remain OWN-03, LIFE-02,
SI-01/11 and AP-02/03 in [CONTRIBUTING](../CONTRIBUTING.md). This documentation
integration does not change the implementation or memory orderings.

## Why the last access was not enough

Mosaic permits the issuer to destroy an arena once admission is closed, controls
have drained and Acquire loads observe every admitted counter at zero. A returner
can publish its free slot, perform its final Release refund, and still be inside
the refund/finish function when the owner observes zero and deallocates.

The old PublishedReturn held `&DomainLedger`. That ledger has 64-byte alignment;
on the reported 64-bit target its single atomic word left 56 bytes of padding.
The function's borrow covered those non-atomic bytes as well. Miri's Stacked and
Tree Borrows models can protect that borrow through the live call, making a
concurrent owner deallocation invalid even when there is no later explicit field
access. This is a storage/aliasing obligation beyond the Release/Acquire ordering
proof; see the related [Rust lifetime discussion](https://github.com/rust-lang/rust/issues/55005).

The correction separates the two return phases structurally:

| Phase | Permitted access |
| --- | --- |
| Before bitmap publication | Finish old reader synchronization and capture stable origin, bitmap and charge operands. |
| After bitmap publication | Do not access the reusable slot or sidecar. Mosaic PublishedReturn carries only `&AtomicUsize` for admitted and the scalar charge. |
| After final refund | No further arena access; a live return frame must not protect padded metadata against the owner's destruction. |

`refund_admitted` takes the atomic word. Its post-RMW assertion inspects the
returned scalar, not the ledger. The ordinary `DomainLedger::refund(&self)` helper
still exists for paths whose owner/control rights keep storage alive; it is not
the final deallocation-enabling borrow in PublishedReturn.

Berry also narrows PublishedReturn to atomic admitted/allocation fields and a
transparent Claims wrapper. Its final root-claim decrement, rather than the
domain refund, enables arena destruction. This avoids depending on incidental
record layout if padding is added later. It should not be described as evidence
that the parent Berry layout necessarily reproduced Mosaic's padded-ledger UB.
Both Berry teardown paths share the same `Claims::only_owner` predicate over an
Acquire observation. Branch families reach Mosaic's terminal refund only after
their local record accesses have ended.

The resource protocol is unchanged: publication precedes refund, a pending old
charge can overlap a new slot incarnation, and no new root-wide return counter,
grant work or retry loop is introduced.

## What verifies each obligation

| Obligation | Current executable check | Evidence boundary |
| --- | --- | --- |
| Mosaic terminal borrow versus destruction | `si11_final_refund_races_teardown_across_many_schedules` | Real storage under Miri; 256 races per Miri seed, 512 natively. |
| Final local family versus destruction | `br03_final_family_release_races_teardown_across_many_schedules` | Real one-slot/one-domain branch root, 256 races per seed. |
| Berry terminal claim versus destruction | `life02_final_claim_release_races_teardown_across_many_schedules` | Real storage, 512 races per seed. |
| Owner observation acquires prior Berry returns | `life02_last_claim_observation_acquires_every_prior_return_before_destroy` | Production-protocol Loom model; raw allocation/borrow protectors are outside Loom. |
| Reintroducing a padded terminal borrow | `mosaic-final-refund-padded-ledger`, `berry-final-release-padded-ledger` | Directed Miri controls: every baseline at seeds 1/2/3 passes; one detected UB is sufficient. |

The persistent returner and repeated teardown races increase the chance of
sampling the narrow lifetime window. The channel orders handoff, not terminal
refund, and owner destruction precedes thread join. These are finite schedule
samples, not exhaustive executions of unsafe storage.

Upstream reports Windows x86_64 Miri observations and sensitivity measurements in
its safety case. Those reports are attributed to that revision; this integration
does not reconstruct their missing raw execution record or call them Linux results.
Detection rates are fixture/source/tool-specific, not a probability bound on the
implementation. Source changes can change the schedule produced by a seed.

The ordinary implementation uses a final Acquire fence; TSan builds use the
existing Acquire-load variant. Loom checks both variants' ordering. Miri checks
executed storage/aliasing histories. These claims are complementary, and a green
ordering model does not discharge the terminal-borrow obligation.

## Other public-contract changes in the commit

- Root identities come from one cold, process-local allocator shared by the
  backends, instead of arena addresses. Assigned nonzero values are not reused;
  exhaustion reports unassigned zero. Domain identities remain root-scoped.
  This introduces no identity-counter work on acquisition or remote return.
- Buf and Mosaic borrowed/local handles provide `try_as_io_slice`, returning
  TooLarge for a view beyond Windows' single-descriptor length limit. Existing
  infallible accessors document their panic. Splitting remains explicit.
- Mosaic Config is non-exhaustive. Construct with `Config::new` and assign public
  fields; downstream struct literals/update syntax are rejected. This makes the
  feature-gated local-branch field compatible with Cargo feature unification.
- Reference domain opening now uses the issuer backends' sequential error order.
  Only the reference's lack of a pending maintenance FIFO is normalized away.
- Verification helpers isolate mutation targets with short unique paths and
  terminate timed-out process trees on POSIX/Windows. Miri UB inside the named
  selected test can kill a mutation even before libtest prints FAILED; a generic
  crash, timeout or build error cannot.

## Reading older evidence

The imported reports retain their original results and source hashes. Earlier
passing Miri/Loom/native campaigns did not establish that all terminal-borrow
schedules were safe. Their source may predate the defect's correction; neither
their test counts nor their performance samples certify this revision.

The local-only diagnostics/preparation follow-up is preserved as history. Its
two integration-test binaries and diagnostic mutation script are absent from
upstream `668d2f3`, so they cannot be claimed as current configured coverage.
The published checkout also has no GitHub Actions workflows. Use the runnable
[verification commands](verification.md#tools-and-reproducible-commands) and attach
new source-qualified outcomes when those checks are actually executed.

The [documentation integration check](reports/2026-10-11/upstream-integration.md) records
the selected native/API checks and example builds performed for this branch;
it is separate from upstream's Miri observations.
