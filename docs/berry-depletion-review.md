# Berry depletion and Mosaic lifetime review follow-up

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

This change implements the accepted review plan under SI-01/02/05 and AP-01/05.
The canonical semantics remain in [CONTRIBUTING](../CONTRIBUTING.md#si-01-bitmap-admission-and-publication).
Both backends remain unpublished experiments; these checks are not a production
soundness certificate or a new progress guarantee.

## Correction and work bounds

Berry previously returned a depleted error directly from a zero Relaxed load.
The single issuer makes a positive hint safe to claim, but does not make a stale
negative hint a fresh depletion witness. The corrected path confirms a nonempty
eligible mask with one Acquire fetch-or-zero. A positive result is claimed with
the existing Acquire fetch-and; only a zero confirmation reports depletion. An
empty grant proves absence without a bitmap operation. Byte admission still comes
first, and every refusal rolls back its own charge and root claim.

There are no new public library types, error variants, retries, return-side atomics
or changes to grants and lifetime ownership. Berry keeps its separate claims;
Mosaic retains its existing confirmation protocol and prepared branch descriptors.

| Complete Berry operation | Source-level RMW bound |
| --- | --- |
| Successful acquire from the first hint | 4 |
| Successful acquire after confirmation | 5 |
| Confirmed depleted, including quota/root rollback | 5 |
| Empty grant after successful byte admission | 4 |
| Injected failure after claim, including confirmation and full rollback | 7 |
| Final buffer Drop | 5 |

The empty-path probe now reports Berry's own bound rather than applying Mosaic's
`W + 2` formula to it. Its `rmw_bound_scope` distinguishes complete ordinary
acquisition from injected rollback after a successful claim. Berry accepts one
bitmap word per class; Mosaic accepts up to 256. These are source-level bounds,
not wait-free hardware or wall-clock guarantees.

## Evidence and proof boundary

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The evidence manifest (`evidence/berry-depletion-review-2026-10-09.json`) records
source/binary hashes, build flags, commands, logs, model bounds, seeds and individual
measurement results. The checkout has no Git metadata, so the baseline was copied and
hashed before edits. Existing evidence files were preserved.

- The new Berry regression failed before confirmation was added, reporting
  `Err(ClassDepleted)` instead of the available bit. The negative-control script
  removes confirmation in a disposable source tree; that variant compiles and
  fails the same test at runtime.
- That regression deliberately carries an earlier zero sample into the actual
  production continuation after publication. It verifies the chosen confirmation
  protocol. It does not claim that a return after a valid empty observation makes
  the earlier observation invalid, or that join-based synchronization reproduces
  every weak-memory execution. Other Loom models test concurrent publication and
  reuse without relying on join for synchronization.
- Partial masks and an empty grant preserve other domains' bits. Repeated failed
  admission restores quota and root claims; insufficient budget still precedes
  an empty-grant error.
- The shared [snapshot regression](../tests/issuer_snapshots.rs) runs remote
  freeze/clone/drop while the issuer samples, then recycles the domain and slot
  between snapshot calls. Only quiescent samples are compared as a complete state.
  No private identity fields are exposed or accessed by the integration test.

The [branch and snapshot proof map](accounting-fanout-hardening.md#branch-descriptors-and-snapshot-review-boundary)
links both branch modules and their actual tests. A family pins its prepared
record through its global right; the last local owner finishes record access
before releasing that right. Root/slot descriptors are copied before release.
The current x86_64 layout tests assert 24-byte LocalBuf and 64-byte BranchRecord.
The descriptor optimization did not increase the previously aligned record size.

Snapshots and new origin writes run on the same sequential issuer. Remote freeze,
refcount changes and publication use atomics; remote owners never rewrite origin.
Consequently a snapshot cannot overlap reissuance of the same slot. Its independent
observations can still disagree in time, and it never reads another thread's local
Cell. Neither sampled diagnostics nor their sums authorize teardown.

## Local validation

| Check | Result |
| --- | --- |
| Native full local/bytes feature set, all targets | 222 test executions passed |
| Default package, all targets | 35 test executions passed |
| Full-feature doctests | 18 passed, including compile-fail boundaries |
| Production-protocol Loom | 30 tests passed; preemption bound 2, max branches 10,000 |
| Miri: Berry raw storage | 28 tests passed for each of 3 seeds × 2 aliasing modes |
| Miri: Mosaic raw storage including branches | 27 tests passed for each of 3 seeds × 2 aliasing modes |
| Miri: concurrent snapshots | 2 tests passed for each of 3 seeds × 2 aliasing modes |
| Negative controls | All 4 compiled variants failed their named regression at runtime |
| Formatting and Clippy | Format check and default/native/all-feature Clippy with warnings denied passed |
| Allocation probes | Positive controls detected; Berry, Mosaic and local branch prepared paths recorded zero allocations, frees and reallocations |
| Probe reporting | Berry reports 5 RMWs; two-word Mosaic reports 4; Berry rejects a two-word configuration |

Native checks used Rust 1.96.0. Miri used nightly-2026-10-03 with seeds 1, 17 and
42, default Stacked Borrows and explicit Tree Borrows. `-Zmiri-ignore-leaks` permits
the intentional forget/abandon tests; aliasing, race and initialization checks
remain enabled. Concurrent snapshot coverage was also added to the existing CI
matrix; no remote CI execution is claimed here.

The initial mutation run exhausted the already nearly full `/tmp` filesystem.
Its partial result/log was retained. Only this task's disposable build directories
were removed, and the complete run passed with independent targets under a root
cache directory. This infrastructure failure is not counted as a detected mutation.

Representative verification commands:

```sh
cargo test --locked --all-targets --features local-branches-spike,bytes-interop
cargo test --locked --doc --features local-branches-spike,bytes-interop
cargo test --locked --lib --features local-branches-spike,loom-model protocol::tests::
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17 -Zmiri-tree-borrows' \
  cargo +nightly-2026-10-03 miri test --locked \
  --features local-branches-spike,bytes-interop --test issuer_snapshots
python3 scripts/check-mosaic-mutations.py --output=/path/to/new/directory
```

## Paired performance observations

There were 224 successful runs: 16 cases × 7 repetitions × baseline/candidate.
Version order alternated each repetition. Both saved release builds used identical
features/compiler/profile flags; no compilation overlapped timing. The shared
x86_64 Hyper-V VM exposes five physical cores and ten logical CPUs. Workers used
distinct physical cores (0, 2, 4, 6), with issuer/control work on CPU 8. There is no
dedicated-host performance gate or evidence for 8–32 physical returners here.

Median rates below use untimed iterations for successful microcases and remote
return bursts. The empty probe necessarily includes its per-attempt clocks and
histogram work; its rate is refused attempts, not useful throughput. Latencies
come from separate instrumented runs except that empty probe. Each cell is the
median of seven per-run results, not a pooled latency distribution.

| Case | Baseline → corrected rate, million/s | p50, ns | p99, ns |
| --- | --- | --- | --- |
| Acquire + mutable Drop | 27.031 → 27.244 | 50 → 50 | 61 → 61 |
| Acquire + freeze + Drop | 23.232 → 23.190 | 60 → 60 | 70 → 70 |
| Scalar clone + Drop | 41.130 → 41.625 | 40 → 40 | 50 → 50 |
| Fan-out, 20 views per iteration | 2.903 → 2.969 | 350 → 351 | 371 → 381 |
| Unreserved depleted attempts | 15.439 → 16.025 | 30 → 30 | 41 → 41 |
| Reserved depleted attempts | 15.771 → 15.844 | 30 → 30 | 41 → 41 |
| Final-Drop bursts, 1 returner | 68.374 → 66.948 | 30 → 30 | 61 → 60 |
| Final-Drop bursts, 2 returners | 105.018 → 108.204 | 30 → 30 | 121 → 121 |
| Final-Drop bursts, 4 returners | 110.339 → 104.272 | 50 → 50 | 141 → 140 |

All successful microcases accepted their full offered work with zero errors; all
empty attempts returned the expected depleted category. Every run confirmed drain.
Return bursts exclude preparation, admission, barriers and channel transport, so
their rates are not pipeline throughput. Individual distributions, failures,
sample counts and metadata/retention observations are retained in the manifest;
insufficient-sample p99.99 values remain unavailable.

The fast acquire/drop median changed by +0.8%, and the freeze cycle by -0.2%.
The 4-returner burst rate changed by -5.5%, although the return protocol's source
was unchanged; this run does not establish its cause. These exploratory samples
do not demonstrate a speed improvement or certify absence of a performance
regression on a deployment host.

Saved disassembly of Berry `Domain::try_acquire` shows the positive hint bypassing
confirmation. In this compiler's x86_64 output, each zero-hint branch lowers the
identity RMW to `lock orl $0, -64(%rsp)` followed by a bitmap load. It does not
necessarily become an extra locked modification of the bitmap cache line. This
target-specific lowering explains why source-level RMW counts must not be reported
as hardware instructions or cache-to-cache transfers; no PMU attribution is claimed.

## Lifecycle limits

Shutdown still stops admission, drains tasks/queues and bounded domain maintenance,
and attempts owner-side `try_destroy()`. A stopped return can retain its remaining
charge indefinitely. Abandonment is terminal and observable; supervisor reservation
is refunded only after confirmed teardown and diagnostic release. Counters do not
identify whether an outstanding return is paused, forgotten or permanently lost.

This change introduces no helping, forced reclamation or background retirement.
No new sanitizer, long soak, fuzz campaign, cross-target execution or dedicated-host
performance evidence is claimed. Branch safety remains supported by local proofs,
finite Loom models and Miri, not certified for all executions by these results.
