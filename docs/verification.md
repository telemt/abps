# Verification of Berry and Mosaic

The [canonical contract](../CONTRIBUTING.md#executable-verification-program),
[safety case](../SAFETY_CASE.md), [registry](../verification/matrix.json) and
[unsafe inventory](../verification/unsafe-sites.json) form the review index.
Existing reference, Berry, Mosaic and branch regressions remain part of the suite.
The [final-refund correction](final-refund.md) explains upstream `668d2f3` and the
terminal-borrow obligation missing from earlier storage proofs. Dated reports
below retain historical results; they are not validation of this revision.
A model/function count is not the number of schedules explored, and repeated
feature/seed executions are not distinct tests.
The [TSan follow-up](reports/2026-10-10/tsan.md) records the sanitizer synchronization fix
and its separate fence/load verification evidence.
The [contract-verification campaign](reports/2026-10-10/contract-verification.md) records
qualified execution identities, reporting regressions, partial-I/O histories and
its focused instrumented checks.
The [generation-verification campaign](reports/2026-10-10/generation-verification.md)
adds batch-right boundaries, reuse before thread join, closed-domain drain and
retirement checks for the current Berry/Mosaic protocol.
The [grant and lifecycle campaign](reports/2026-10-10/grants-lifecycle-verification.md)
adds independent grant histories, the shared root-drain predicate, sidecar reuse,
configuration boundaries and diagnostic measurement probes with source-qualified
native, Loom, Miri, sanitizer and mutation evidence.
The [remote-authority campaign](reports/2026-10-10/remote-authority-verification.md)
adds a private buffer access boundary with compiler gates, paused remote reuse,
fault/unwind reincarnation histories, full configured-capacity FIFO permutations
and local before/after depleted-path probes up to 256 words.
The [branch-construction follow-up](reports/2026-10-10/branch-construction-verification.md)
records the winning-word cursor contract, guarded source/staged-family cancellation,
release checks, instrumented lifecycle evidence and the measured construction cost.
The [diagnostics and preparation follow-up](reports/2026-10-10/diagnostics-verification.md)
records local-only concurrent observer, separate deallocation and failed-prepare
tests. Its two integration binaries and diagnostic mutation script are absent from
upstream `668d2f3`; those archived results are not current configured coverage.

## Tools and reproducible commands

Shipping/native validation uses Rust 1.96.0. Instrumented validation pins
`nightly-2026-10-03`. Optional tools are pinned in the registry: cargo-llvm-cov
0.9.1, cargo-fuzz 0.13.2 and cargo-mutants 27.1.0. Criterion 0.8.2 is a development
dependency; libfuzzer-sys has a separate fuzz-package lockfile. No runtime or
verification dependency is added to the public core API.

```sh
rustup component add llvm-tools-preview
rustup +nightly-2026-10-03 component add miri rust-src llvm-tools-preview
cargo install --locked cargo-llvm-cov --version 0.9.1
cargo install --locked cargo-fuzz --version 0.13.2
cargo install --locked cargo-mutants --version 27.1.0
python3 scripts/test_verify.py
python3 scripts/verification_inventory.py
python3 scripts/verify.py --suite registry --output verification/artifacts/registry
python3 scripts/verify.py --suite native --output verification/artifacts/native
python3 scripts/verify.py --suite allocations --output verification/artifacts/allocations
python3 scripts/verify.py --suite properties --profile nightly --output verification/artifacts/properties
python3 scripts/verify.py --suite loom --profile release --output verification/artifacts/loom
python3 scripts/verify.py --suite miri --profile pr --output verification/artifacts/miri
python3 scripts/verify.py --suite coverage --output verification/artifacts/coverage
python3 scripts/verify.py --suite asan --output verification/artifacts/asan
python3 scripts/verify.py --suite tsan --output verification/artifacts/tsan
python3 scripts/verify.py --suite msan --output verification/artifacts/msan
python3 scripts/verify.py --suite fuzz --profile weekly --output verification/artifacts/fuzz
python3 scripts/verify.py --suite mutations --profile weekly --output verification/artifacts/mutations
python3 scripts/verify.py --suite stress --profile weekly --output verification/artifacts/stress
python3 scripts/verify.py --suite performance --output verification/artifacts/benchmark-smoke
```

Output directories must be new. `--build-root` selects a reusable, executable
filesystem with room for separate native/Loom/Miri/sanitizer/coverage targets.
Do not delete another run's artifacts to make space. The runner records free
space, source digest, commands, toolchain output, explicit test controls, logs,
passed execution counts and outcomes in `results.json`. Native test discovery
rejects an empty selector before interpreting it as evidence. Runner tests exercise
empty selection, timeout/process termination, compile errors, missing tools and
registry errors. If setup itself fails, the command fails; no test result is invented.

`passed` means the selected campaign completed successfully with its recorded
bounds. `failed`, `incomplete`, `unsupported`, and `not_run` are distinct states.
A bounded fuzz/stress campaign never claims exhaustive exploration. A missing
executable, unsupported toolchain, insufficient storage or timeout is not a
negative control detecting a bug. Registry success means selectors resolve; it
is not an aggregate assertion that every applicable tool has run.

Current results use `evidence_schema: 1`. Each test execution carries source,
Cargo target, resolved features, compiler, platform, tool and reference-acquire
identity. Discovery runs with the same configuration, and every selected full
test name must complete. A selector from another binary or one Loom variant
cannot supply missing evidence. The registry explicitly permits feature supersets
for the shared Loom suite; native and Loom compilations remain separate.
Doctests, including compile-fail cases, have execution records. The `empty_targets`
registry only permits known feature-gated or runnable-only targets to compile
with zero tests; these cannot fulfill a scenario. Legacy reports are not rewritten
or silently promoted to qualified evidence.

Required steps are recorded before execution; missing steps, source drift and
earlier failures prevent completion. Sanitizer reports and failed test summaries
override a zero exit code. Directed mutations record both the unmodified named
baseline and the mutant; a timeout/build failure is never a killed mutant.
Their logs are copied into the campaign even when the script fails.

Run the reporting regressions in both Python modes:

```sh
python3 -B scripts/test_verify.py
python3 -B -O scripts/test_verify.py
python3 -B -O scripts/verification_inventory.py
```

Concrete replay failures carry backend, violated postcondition, operation and
logical owner IDs. Shrinking compares that identity, rather than panic wording;
unknown panics retain the original trace. Deterministic boundary histories assert
that pressure states were reached. Scripted I/O covers partial prefixes, wakeups,
EOF, errors, cancellation and unaffected neighboring allocations. Search tests
count visits instead of treating noisy timing samples as a work-bound proof.

## Executable models and failure injection

Batch construction observes exact rights only at complete materialization
boundaries: the source, constructed handles and pending rights must sum to the
reference count. Both ordinary guard rollback and unwind cover every prefix of
N=0..3. The concrete handoff has no unwind point; a mutation that inserts one is
a detector control, not a reachable failure of the unmodified implementation.

The shared generation models compose actual scalar/batch reference operations,
final release, bitmap publication, next acquisition and reference initialization.
Reader visibility is checked before join or diagnostic synchronization. Host-only
coverage flags are read after exploration to reject a vacuous run that never
reached reuse. Berry's closed two-allocation model separately tests its monotone
drain predicate and record reuse while old root releases may remain outstanding.
These new models always use exhaustive finite exploration with a 10,000-branch
guard, even in the smoke profile; reaching that guard is an incomplete result.

The test-only `Trace` decoder is shared by native proptest, corpus replay and fuzz
binaries. It caps traces at 1,024 operations and prepares at most 4 MiB per backend.
Classes include word boundaries relative to `usize::BITS`, not a 64-bit assumption.
An independent model predicts exact admission/range/reference errors and checks
payload, filled/initialized prefixes, capacity, grants and origin retention after
each sequential operation. A common maintenance barrier normalizes the reference's
immediate record recycle with issuer-explicit maintenance. Reference metadata sizes,
opaque identity values and backend-specific root control counters are not compared.
The existing deterministic reservation/maintenance tests retain those differences.

### Concrete adversarial histories and review coverage

The registry additionally tracks all ABPS-001..032 review scenarios. These are
checklist IDs referencing existing invariants, not 32 new normative guarantees or
a requirement to add 32 duplicate test functions. Registration and actual execution
are reported separately. See [the conservation campaign](reports/2026-10-10/rights-verification.md).

`tests/adversarial.rs` uses a separate safe rights oracle and thin backend adapters.
Berry/Mosaic histories preserve explicit maintenance and exact sequential errors.
The reference comparison runs only common histories; no compatibility poll is
inserted into the adversarial issuer replay. Allocation IDs, handle IDs and domain
incarnations have distinct meanings. Staged publication can release a slot while
an earlier allocation's charge remains pending.

The state-aware generator prefers eligible pressure-directed actions in three of
four choices; otherwise ordinary valid operations build the next state. Fixed
preludes separately assert that last-slot, reference-limit, closing-domain,
grant-pressure and pending-maintenance states are actually visited. This prevents
random reachability from being mistaken for coverage. Existing binary corpora keep
their original decoder; the new `adversarial_rights` fuzz target is additive.

Failed issuer histories save version-1 JSON with configuration, seed, stable IDs,
concrete actions, the failing backend/step, expected/observed outcome and completed
observations. `ABPS_TRACE_OUTPUT` controls the artifact directory; the runner also
supplies its source digest. Delta debugging removes actions/reduces operands only
when dependencies remain valid and the same failure class reproduces. No missing
handle is rebound by a modulo lookup. Replay concrete JSON or new fuzz bytes with:

```sh
ABPS_RIGHTS_TRACE_FILE=/path/to/rights.json cargo test --features local-branches-spike,bytes-interop --test adversarial abps_concrete_trace_roundtrip_replay_and_dependency_preserving_shrink
ABPS_RIGHTS_BYTES_FILE=/path/to/fuzz-crash cargo test --features local-branches-spike,bytes-interop --test adversarial abps_concrete_trace_roundtrip_replay_and_dependency_preserving_shrink
```

Metamorphic tests cover independent drop permutations, batch splitting, snapshot
and zero-budget maintenance insertion, composed writes and larger budget on fresh
pools. Budget monotonicity compares successful fixed prefixes: accepting extra
requests in a different history could consume slots needed by a later request.
Lazy/ZeroPayload equivalence applies only to Mosaic's existing preparation API.

ASan/TSan/MSan selections include actual worker-fanout adapter tests. Credit tokens
remain charged after dequeue, until ownership moves into a bounded writer batch;
cancellation drops payload before its permit. Their native/Tokio evidence is not a
claim that Loom models Tokio. Source digests include adapter/example code. The runner compares the digest again
at completion; changes during a campaign make the result incomplete.

Directed conservation mutations live in `scripts/check-rights-mutations.py` and
run only in disposable trees. A killed mutation must compile and fail its named
test, or report Miri UB while executing that selected test before libtest can print
FAILED. A generic abort is insufficient. Survivors, build failures and timeouts retain distinct states;
unexplained outcomes fail the campaign. Publication-order mutations exercise both
fence and Acquire-load variants, in addition to existing final-acquire controls.

`ABPS_PROPTEST_CASES` and `ABPS_TRACE_OPERATIONS` select campaign depth. Smoke/PR
uses 128 cases up to 250 operations; long profiles use 10,000 up to 1,000. Miri
uses three short cases. Proptest shrinks failures and persists regression seeds.
Long profiles explicitly use `cargo test --release`; PR/native campaigns keep
debug builds as separate evidence. All oracle assertions remain enabled in release.
Preserve the minimized input too. Replay a single fuzz/corpus input with:

```sh
ABPS_TRACE_FILE=/absolute/input.bin cargo test --locked --features local-branches-spike,bytes-interop --test model verification_corpus_replays_and_can_be_selected_by_path
ABPS_BRANCH_TRACE_FILE=/absolute/input.bin cargo test --locked --features local-branches-spike --test local_branches br_verification_corpus_replays
```

The real acquisition path has private `cfg(test)` error/unwind injections after
RAII protection of pin (Berry), charge, bit, origin and initialized reference state.
Both policies and repeated reuse are covered. Production builds contain neither
the fault argument nor injection branches. Existing batch/branch builders are
interrupted at each materialization prefix. Existing return tickets let tests stop
before/after publication without adding user callbacks. After publication, tests
expect retained pending accounting, not rollback or helping. Persistent fuzzing
contains no forget/abandon action: each iteration must confirm teardown. Intentional
retention is confined to finite tests named explicitly in the registry.

## Tool boundaries

### Terminal-borrow regressions

The upstream correction adds repeated real-storage races with teardown before
joining the returner:

- Mosaic: `si11_final_refund_races_teardown_across_many_schedules`.
- Local families: `br03_final_family_release_races_teardown_across_many_schedules`.
- Berry: `life02_final_claim_release_races_teardown_across_many_schedules`.

The checks target a live terminal borrow during owner deallocation, not just a
late explicit field access. Loom's
`life02_last_claim_observation_acquires_every_prior_return_before_destroy` checks
Berry's actual owner predicate and ordering; it does not model raw borrow protectors.
See [the correction](final-refund.md#what-verifies-each-obligation) for the distinct
proof obligations and upstream-reported platform limits.

The two padded-ledger controls use strict provenance with Miri seeds 1/2/3,
independently of the general Miri campaign's 1/17/42 seeds:

```sh
python3 scripts/check-mosaic-mutations.py --only mosaic-final-refund-padded-ledger --nightly nightly-2026-10-03 --output /tmp/abps-refund-control
python3 scripts/check-mosaic-mutations.py --only berry-final-release-padded-ledger --nightly nightly-2026-10-03 --output /tmp/abps-claim-control
```

Every selected unmodified seed must pass. Mutated seeds run until the first
non-surviving outcome; one detected UB suffices, while a build failure or timeout
remains a failure to establish detection. Reproducibility requires the exact
source, compiler, target, flags and seed, not the seed alone.

Mutation controls use separate Cargo targets under `<output>/target/<hash>`.
For the current MSVC helper layout, keep the absolute direct-mutation output
path within 155 characters and the runner's `--build-root` within 106 characters;
these are harness path constraints, not allocator limits. Timeout cleanup uses
POSIX process groups or Windows process-tree termination. This support does not
imply that every sanitizer or tool is available on both platforms.

### What each tool covers

- **Loom:** production protocol with Loom atomics, not raw arena storage. Critical
  small tests use exhaustive exploration within their finite model. Other tests
  use `ABPS_LOOM_PROFILE=smoke` (preemptions 2), `deep` (3), or `exhaustive` (none).
  There are no permutation/duration caps; max branches is 10,000 and exceeding it
  fails. Conflicting `LOOM_MAX_*` settings are rejected. The external timeout is
  reported as incomplete. Successful completion prints its actual exploration
  scope and reference-acquire variant. The runner executes the same models twice:
  native `fence` and `acquire-load` with private `--cfg abps_tsan`, in separate
  targets without TSan instrumentation. The registry requires both variants.
  See [Builder](https://docs.rs/loom/0.7.2/loom/model/struct.Builder.html).
- **Miri:** strict provenance and both the pinned toolchain's Stacked Borrows and
  Tree Borrows. PR seeds are 1, 17, 42; long profiles add 0..9. Ordinary tests keep
  leak checking enabled; only exact intentional-retention selectors run separately
  with `ignore-leaks`. No UB/race/aliasing checks are suppressed. Flags are pinned
  toolchain behavior, not guarantees about future nightly versions.
- **Coverage:** explicit native feature configurations, never `--all-features`
  with Loom. JSON/LCOV/HTML and missing-line reports describe the merged native
  campaign. Doctests and Loom remain separate gates. Percentages are diagnostic;
  reachable critical transitions need named tests and local proof obligations.
  [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) supports merging profiles.
- **ASan/MSan/TSan:** separate target directories and instrumented std, symbolized
  debug builds. Ordinary ASan keeps leak detection; intentional-retention tests
  are excluded from that leak gate and covered by native/Miri. MSan requires all
  dependencies instrumented; the runner supplies target-specific Clang and
  sanitizer flags to cc-rs helpers as well as rebuilding Rust std. TSan does not
  recognize atomic fences. Its build automatically selects an Acquire load in
  place of the final reference Acquire fence, following Rust Arc's approach.
  All decrements remain Release; ordinary builds retain the fence. See the local
  release-sequence proof in the [safety case](../SAFETY_CASE.md#atomic-edges-and-progress).
  A compiled cfg probe verifies both the chosen path and target sanitizer before
  the campaign. No Cargo feature, runtime switch or suppression is required.
  TSan validates the load variant; Loom checks both. Do not strengthen ordinary
  atomics to appease TSan. Actual TSan warnings fail even if `exitcode=0` is set.
  Historical failed/control reports remain available. Investigate and document every
  finding; an unexplained report blocks admission. See the [Rust sanitizer
  contract](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html).
- **Mutations:** existing directed controls mutate isolated source/target trees and
  must compile and fail their named regression (including recognized Miri UB in that
  selected test). Automatic mutants cover grants and
  local reference predicates using bounded component tests. Grant tests monitor
  class reads independently, so a search that stops advancing fails an assertion;
  it cannot hang unrelated storage tests. Unmutated native/lifecycle campaigns
  remain full. Survivors need explanation or new tests. Build errors, empty runs
  and timeouts are not successful detections. `--mutation-scope components` or
  `directed` replays one part; the default `all` runs both. `--discard-builds` removes only this
  run's temporary compilation output after retaining logs and sources.
- **Native stress:** one issuer, 1/2/4/8/16 returners, one-slot reuse and segmented
  capacity, 100 domains, bounded queues, a delayed consumer, generation churn and
  complete drain. The harness itself allocates; allocation-free evidence comes from
  the `allocations` suite's existing stats_alloc probes with positive controls,
  not stress RSS. Reference allocation counts remain a baseline; only the
  experimental prepared paths assert zero allocations/deallocations.
- **Performance:** ordinary `performance` is Criterion functional smoke. Actual
  sampling requires `--dedicated-host`; it additionally runs the existing matched
  primitive/TCP probes with seven repetitions. Criterion is not a tail-latency or
  offered-load replacement. Compare source identities and policies on isolated
  hardware, and retain failures/late deliveries. No timing gate runs on shared CI.

## CI and release use

Upstream `668d2f3` contains the local verification runner and registry, but no
GitHub Actions workflows or reusable verification action. The commands above are
runnable entry points, not configured push/PR/nightly jobs. Historical reports may
describe local workflows that were not published; their schedules and self-hosted
runner names do not establish current automation.

Future CI should preserve source-qualified logs on failure and separate native,
Loom, Miri and sanitizer targets. Long campaigns and dedicated-host performance
measurements need explicit scheduling and retained outcomes.

Release admission requires evidence for the release source, review of every
unsupported/incomplete result and unexplained mutation/sanitizer finding, long-run
resource stability, an equivalent baseline on dedicated hardware, and an independent
unsafe review. An experimental backend does not become production-ready from a
single green runner invocation.

## Grant, drain and configuration regressions

`--suite properties --cases 1000 --operations 256 --seed 424242` selects an explicit
acceptance size without changing the longer release defaults. It includes the
shared public histories, branch histories, configuration checks and raw logical
slot-set comparisons. Raw grant cycles shrink whole incarnations; the older
interleaved public oracle retains stable-ID, invariant-preserving trace reduction.
The new `configuration` fuzz target checks both issuer backends, exact root budget
boundaries and wide arithmetic. Preparation is capped at 4 MiB; large plans never
become actual arenas. Structural raw tests separately audit every typed range.

`python3 scripts/observability-smoke.py --output <new-directory> --build-root
<target-directory>` builds the existing `spike_probe` example and exercises its
`--case=observability`. Berry/Mosaic common cases use the same classes, slots,
prepared domains, grants and operation count; the 129-slot Mosaic case is separate.
`grants`, `pages`, `busy-destroy` and `mixed` report diagnostic samples independently
from admission successes, failures and queue rejection. Busy teardown drops all
controls first so Mosaic really examines allocation ledgers. Mixed load uses a
bounded channel to one returner; channel and clock overhead remain in the result.
`--diagnostic-interval=0|1|32` means attempts between samples, not time-based pacing.
The initial bounded exchange completes before the short mixed loop and a successful
admission is required. Empty buffers contribute zero useful payload; requested and
charged bytes are separate. Elapsed time includes startup/drain overhead.
All local results are harness evidence; dedicated-host comparisons remain required.
