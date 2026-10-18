# Accounting and fan-out hardening

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

The normative decisions are [AP-01..09](../CONTRIBUTING.md#accounting-and-fan-out-hardening).
This report tracks implementation evidence, rather than introducing a second contract.

The review's stopped-return and early-owner-drop observations are intentional
experimental limits. Mosaic return already removes the root-wide claim RMW;
the older Berry remains a comparison baseline. Neither backend helps a
stopped return. Adding a background reaper or force-refunding its domain would
violate the accepted lifetime model.

## Transition proof map

| Transition | Local obligation | Executable evidence |
| --- | --- | --- |
| Reserve charge, claim bit, install ownership | Rollback returns only this request's charge and any claimed bit | `si11_rollback_stages_conserve_all_ledgers`, `rollback_at_every_admission_stage`, `rollback_survives_unwind_after_slot_claim` |
| Berry zero hint to exact depleted | Nonempty eligibility requires one confirming RMW; failed admission restores quota and root claims | `si01_confirmation_rechecks_a_stale_empty_hint`, `si01_confirmed_depletion_and_empty_grant_preserve_other_bits`, `si01_depleted_attempts_restore_claims_and_preserve_budget_precedence` |
| Clone versus another handle's release | A live source protects the count from zero; checked increments never resurrect | `checked_clone_races_with_nonfinal_release` in both production-protocol Loom modules |
| Last reference to zero, before publication | Slot remains unavailable and charge remains positive | `ap05_zero_references_and_published_pending_refund_are_honest_samples` in both raw backends |
| Publish, reuse, old refund | Copy origin/return operands first; never reread new slot incarnation | `si11_reissue_before_refund_cannot_credit_new_origin_or_destroy_root`, `old_return_after_reuse_refunds_only_its_origin_and_pins_shutdown`, Loom reuse models |
| Final refund, owner teardown | Release/Acquire drain covers prior users; no later storage access or live padded terminal borrow | `si11_final_refund_races_teardown_across_many_schedules`, `life02_final_claim_release_races_teardown_across_many_schedules`, `si11_zero_observation_acquires_every_prior_return_before_destroy` |
| Owner Drop versus last remote return | Destroy on proven drain, otherwise permanently abandon; return never destroys | `ap04_owner_drop_races_final_return_without_delegating_teardown` in both raw backends |
| Domain close, grants, generation reuse | Keep identity and grants through pending refund; retire on overflow | `si13_fragmented_grants_recycle_only_after_pending_refund`, `domain_completion_gates_new_identity_without_join_synchronization`, generation tests |
| Branch construction/unwind | Materialized plus guard-held global rights are conserved | `br01_unwind_at_every_materialization_prefix_releases_all_rights`, actual protocol branch Loom tests |
| Last local release and sidecar reuse | Copy descriptor before global release; only a unique family migrates | `br03_stopped_local_release_and_reuse_before_refund_pin_old_origin`, `br03_paused_family_release_pins_slot_then_old_refund_cannot_touch_new_family` |

Loom exercises the production atomic transitions with instrumented synchronization;
raw storage and pointer lifetime require Miri separately. These are bounded models,
not proofs of every execution or platform. Deliberate abandonment tests require
Miri's leak-check exception; that exception does not disable aliasing checks.

### Branch descriptors and snapshot review boundary

A complete branch review includes both [raw branch ownership](../src/mosaic/raw/branches.rs)
and [the local counter protocol](../src/mosaic/protocol/branches.rs), together with
[storage regressions](../src/mosaic/raw/branches/tests.rs) and
[production-protocol Loom models](../src/mosaic/protocol/tests/local_branches.rs).
`BranchRecord` descriptors are initialized once during prepare. A family retains
one global slot right; its last local release copies root/slot and ends record
access before global release. Only exclusive reissued slot ownership can reset
the counter. The immutable descriptor does not create an independent lifetime. The terminal
refund borrows only the atomic admitted field, as required by the
[final-refund correction](final-refund.md); absence of later explicit access alone
is insufficient to justify deallocation while a padded borrow remains live.

`br03_stopped_local_release_and_reuse_before_refund_pin_old_origin` checks sidecar
reuse before the old refund; `br03_remote_families_allow_teardown_before_worker_join`
checks actual teardown without a join lifetime crutch. The corresponding Loom
models exercise instrumented Cell access, final-reader synchronization, migration
and paused release. Construction-prefix unwind and close/forget tests cover the
remaining BR-01/05 transitions. Layout tests currently assert a three-word
LocalBuf and a 64-byte prepared record; the historical x86_64 record size did not
increase when descriptors moved into it. This is measured layout, not a portable
cache-line or performance guarantee.

For both backends, `snapshot()` runs on the same sequential issuer as acquisition
and domain recycle. Remote owners never write origin, and remote freeze/refcount/
publication touch atomics. Thus an origin read cannot overlap a new incarnation's
origin write. Independent fields can still describe different moments of the same
incarnation. Snapshot never reads local branch counters or authorizes reclaim.
[Concurrent snapshot tests](../tests/issuer_snapshots.rs) sample during remote
freeze/clone/drop, then reuse the slot and domain between calls; only quiescent
samples are compared as a complete state. Existing AP-05 tests separately stop
returns before publication and before refund.

The [depletion follow-up](berry-depletion-review.md) records the Berry correction,
current source identities, new checks and their limits. Historical measurements
below retain their original scope and do not become new validation by reference.

## Diagnostic migration

Experimental `ClassSnapshot` replaces `available`/`issued` with
`observed_available_slots`/`observed_unpublished_slots` and removes synthetic
`acquiring`/`returning`. `DomainSnapshot` replaces `admitted_bytes` and
`live_backing_bytes` with `observed_admitted_bytes` and
`observed_unpublished_backing_bytes`, removing synthetic pending phase fields.
Each is a sample; independent fields can disagree during remote returns. A zero
reference count before publication still contributes unpublished backing. The safe
reference model retains its exact phase partition. Quiescent oracle comparisons
explicitly normalize these different diagnostic interfaces.

Adapter contention is reported separately for admission, scalar clone and batch
clone. The aggregate is their sum. Contended is transient contention, including
races with refunds, and does not prove budget or capacity exhaustion.


## Implementation

LocalBuf stores a prepared-record pointer plus range. Each record holds its Cell
counter and immutable root, slot, payload pointer, capacity and reference limit.
The whole record is aligned, avoiding an accidentally padded embedded counter.
On x86_64 this reduces the handle from 40 to 24 bytes while keeping the record at
64 bytes. Preparation initializes descriptors; reuse resets only the counter.
Family completion copies root/slot before global release. The ordinary Buf path
and original Berry are preserved.

Mosaic class search advances its traversal index with bounded wrap rather than
calculating a remainder per word. The persistent cursor retains the winning
word/node for the next attempt; it does not rotate successful claims round-robin.
Both passes retain their original order and masks. The second
pass still uses confirming RMWs for exact depletion; a new occupancy counter,
retry or larger-class fallback was not introduced.

`--fanout-api=worker-branches` groups the existing same-worker subscribers of a
stream. Ingress reserves per-subscriber semaphore credits before payload admission,
then sends one Branch and fixed recipient array through a bounded stream mailbox.
The writer worker constructs local views and places them in bounded local queues.
The local family limit is the configured fan-out, and one sidecar is prepared per
slot. The old `local-branches` mode still prepares one sidecar per subscriber.

A mailbox has F*Q positions, each containing at least one already-reserved credit.
A local queue also holds its credit. Popping a queue item does not refund it;
transferring the frame into a bounded writer batch does. Cancellation drops payload
ownership first. No credit is refunded through a domain ID lookup. Bundles carry
the originating supervisor generation and are rejected on mismatch. Forwarding
runs at most eight bundles before yielding. Deadlines survive queue transfers,
and the existing writer checks them before every socket write poll.

Final flow accounting is sampled after cohort quiescence, including remote writer
workers: dispatched recipient rights equal local enqueues plus cancellations before
forwarding. Delivery loss remains separate. Root shutdown and supervisor charging
retain SI-16 semantics, including permanent abandonment. No recovery reaper was
added.

The resource plan preserves `1 + F*(Q + write_batch)` payload headroom. It reports
mailbox item storage, local queue items and semaphore object storage separately;
Tokio channel blocks, Arc control allocations, tasks and sockets are outside those
models. Core allocation checks and adapter allocation probes are separate.

## Harness and validation

The branch harness reuses owning scratch per worker and borrowed descriptors per
packet. It reports preparation and dispatch separately, uses a common start epoch
with a deadline and cancellation, and records fully served frames alongside view
successes/refusals. `prepare`, `materialize`, `churn`, `cycle`, `drops` and
`final-drop` have distinct timing windows. `final-drop` requires one owner/worker;
other multiworker owner-drop samples explicitly mix final and nonfinal Drops.
Instrumented view creation and nonfinal view Drop are separated from throughput
runs. The control mode measures harness overhead without subtracting it.

The runner defaults to one logical CPU per physical core; `--smt` and explicit CPU
lists permit separately identified experiments. It calibrates short cases, retains
actual iteration counts and interleaves seven repetitions. Prebuilt compiler flags
are reported as unknown unless the caller supplies an actual `--build-record`;
caller environment alone is not evidence of how a binary was built.

Native tests cover queue-credit handoff, mailbox cancellation, close after
preflight, Skip/Disconnect, an intentionally undersized local queue, partial
writes, original deadlines, stale generation, cancellation during ingress and a
stalled writer batch, and remote root shutdown. Type assertions cover each control
handle, BufMut, Buf, Branch and LocalBuf. The safe reference's exact oracle remains
in use for quiescent resource comparisons.

The six mutation controls intentionally remove a local increment, allow nonunique
migration, lose construction rights, remove final Acquire synchronization, report
a domain drained too early, and refund before publication. Each must fail its named
regression, not merely fail to compile. Miri runs use the pinned nightly's default
Stacked Borrows and explicit `-Zmiri-tree-borrows`, seeds 1/17/42. The installed Miri
README documents this default; `-Zmiri-stacked-borrows` is not a supported flag.
CI includes both aliasing modes; this local work does not claim a remote CI run.

## Measurements on 2026-10-09

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The summary (`evidence/accounting-fanout-hardening-2026-10-09.json`) and compressed raw
evidence (`evidence/accounting-fanout-hardening-2026-10-09.raw.json.gz`) contain
commands, build identities, source snapshots/diffs, individual results, validation logs
and textual profiles. This checkout has no Git metadata; file hashes identify the
baseline. Cargo.toml and Cargo.lock were unchanged.

Measurements used a shared x86_64 Hyper-V VM with five physical cores and ten
logical CPUs, stable Rust 1.96.0 and the release profile without extra compiler
flags. Primitive workers used CPUs 0/2/4/6, with the coordinator on 8. TCP used
server CPUs 0/2, readers 4/6 and generator 8. No compilation ran during timing.
These are exploratory measurements, not the dedicated-host AP-09 acceptance gate.
Eight, sixteen and thirty-two physical-worker cases remain unvalidated.

The before/after primitive comparison uses identical new harness code. The before
core includes the diagnostic corrections but precedes the compact record and
wrap cursor. Separate target directories and binary hashes prevent Cargo artifact
reuse from mixing builds. There are 1,764 main comparison runs, 126 calibrations,
30 separately instrumented latency runs, 112 empty-class runs, 84 remote-return
controls and 28 longer cycle controls. Paired runs use identical work; calibration
can select different iterations between modes. These fixed-work probes have no
independent arrival schedule. The TCP load generator measures offered arrivals
and keeps missed, rejected, skipped and late work.

### Compact local ownership and empty search

For 64-byte buffers and eight views, medians of seven interleaved repetitions:

| Case | Physical workers | Before, M accepted views/s | After | Change |
| --- | ---: | ---: | ---: | ---: |
| Local churn | 1 | 41.90 | 58.08 | +38.6% |
| Local churn | 4 | 169.54 | 222.87 | +31.5% |
| Local full cycle, calibrated run | 1 | 13.57 | 14.68 | +8.1% |
| Local full cycle, calibrated run | 4 | 42.47 | 49.58 | +16.8% |
| Local full cycle, 20,000 iterations | 4 | 32.88 | 33.99 | +3.4% |

All these local cases served every view. Full-cycle results include transport
wrapper size, preparation, dispatch and scheduling, so the improvement cannot be
assigned entirely to allocator instructions. The longer cycle control had wide
ranges: 28.32–37.10 Mviews/s before and 30.84–46.50 after.

Ordinary batch churn at four workers was nearly unchanged (47.18 to 47.47
M accepted views/s), with approximately 49% of offered views refused in both runs.
The short batch cycle control regressed 7.7% (45.77 to 42.26 Mviews/s). A longer
paired control measured +2.5% (32.18 to 32.99), with ranges 27.21–36.33 and
25.70–41.42 respectively; refusals were 0.451% and 0.433%. Both datasets are kept.
They do not establish an ordinary-path regression or a non-regression guarantee.

At 256 bitmap words, unreserved empty-search p50 fell from 822 to 621 ns.
Reserved search remained 922 to 921 ns. Exact depletion still requires the
confirming RMW pass; linked grant traversal remains part of reserved work. The
ordinary remote-return controls were broadly unchanged, including separate-root
four-worker bursts at 331.34 to 330.07 Mreturns/s. These burst figures use the
older return harness and do not describe sustained pipeline throughput.

Separate software CPU profiles put FamilyRight Drop at 27.1% of user samples
before and 18.6% after in local churn. The after batch profile put Lease Drop at
64.5%. These relative samples locate cost; they do not establish cache-line
transfers or absolute instruction savings. Hardware cycles, instructions,
cache-miss counters and perf c2c were unavailable on this VM.

### Worker fan-out did not meet performance acceptance

The 140 native TCP trials compare scalar, batch, local branches, worker branches
and Bytes with a byte budget and bounded queues. Each has seven repetitions per
configuration, two streams, remote consumers, Q=8, write batch 8, explicit Skip,
100 ms freshness, hard class reservations and validated full payload headroom.
For F=20 the envelope is 321 backings per stream, 642 slots per class/root and
481,500 admitted bytes per stream. Offered rate is total frames/s across the two
streams, each trial lasting two seconds. Payloads use the existing 512/1500-byte
distribution. Timely delivery divides by all scheduled recipient deliveries.

| Offered frames/s | Mode | Timely deliveries | Scheduled-arrival p99, ms | Frames/write |
| ---: | --- | ---: | ---: | ---: |
| 20,000 | scalar | 96.65% | 0.843 | 2.11 |
| 20,000 | batch | 97.15% | 0.778 | 2.12 |
| 20,000 | local branches | 97.36% | 0.612 | 2.09 |
| 20,000 | worker branches | 97.48% | 0.820 | 2.18 |
| 20,000 | Bytes | 97.45% | 0.676 | 2.10 |
| 50,000 | scalar | 92.94% | 1.036 | 5.83 |
| 50,000 | batch | 92.64% | 1.105 | 5.83 |
| 50,000 | local branches | 93.25% | 1.050 | 5.87 |
| 50,000 | worker branches | 74.78% | 1.161 | 4.46 |
| 50,000 | Bytes | 93.06% | 0.962 | 5.84 |

At 50,000 frames/s the worker mode's timely fraction ranged from 72.18% to 76.89%,
while batch ranged from 92.12% to 94.47%. Median queue-skipped recipient deliveries
were 471,524 versus batch's 109,906. Worker-mode whole-frame discards in these runs
were NoReceivers after queue-credit preflight, not budget/class exhaustion. No
native run lost an already enqueued delivery or cancelled a recipient before
forwarding. All roots were destroyed, cohort flow identities balanced, and the
supervisor ended with zero charged bytes.

Worker mode reduced root metadata from 1,759,576 bytes for per-subscriber local
branches to 198,232 bytes (scalar/batch: 116,056). It also modeled 130,560 bytes of
mailbox items and 1,600 bytes of semaphore objects outside root metadata, plus
local queue items. These models exclude Tokio's internal allocations. The saving
does not compensate for the observed throughput loss.

Process CPU samples attribute roughly 80% of server CPU at the high load to kernel
work; the software profiles show networking/copy paths. Separate strace runs
greatly reduced delivered load and are excluded from native comparisons. Their
writev counts were 38,935 for batch, 40,215 for local and 58,696 for worker mode.
Futex time includes waiting and is not evidence of an allocator lock bottleneck.

The extra mailbox/credit path and smaller writer batches are candidates for the
worker-mode loss; this experiment does not isolate a causal component. Keep the
mode explicit and scalar as default. Further work should measure scheduler hops,
credit reservation and writer batching separately before changing this adapter's
protocol. No custom semaphore, hidden retry, changed overload policy or default
promotion is justified by these results.

## Validation results

| Check | Result and scope |
| --- | --- |
| Native tests | 35 default and 219 feature-enabled test executions, including repeated example harness tests; all passed |
| Doctests | 18 passed, including 17 compile-fail ownership examples |
| Loom | 28 production-protocol models plus 16 other library tests passed; finite model bounds remain in their source |
| Miri | 27 Mosaic raw-storage tests for each of 3 seeds × 2 aliasing modes passed; the earlier focused AP test pass also covered the old Berry |
| ASan | 27 Mosaic raw-storage tests passed; leak detection disabled for deliberate permanent abandonment |
| Mutations | All six intentional violations failed their named tests at runtime, rather than compilation |
| Formatting / Clippy | Default, full local/bytes feature set and issuer/local without bytes passed |
| Core allocation probe | Positive control detected; 10,000 iterations and first remote return recorded zero allocations, frees or reallocations |
| Prepared worker dispatch probe | At F=20/Q=8 and 10,000 iterations, dispatch, credit-only and Tokio queue controls each recorded zero allocations/frees/reallocations; cold setup made 120 allocations totaling 98,320 bytes |

The allocation probe excludes sockets, tasks and runtime scheduling; it does not
claim an allocation-free TCP application. Latency histograms identify insufficient
samples for p99.99. No new long soak, fuzz campaign, TSan, cross-target execution or
remote CI result is claimed. The abandoned-arena leak exception does not authorize
reclamation of forgotten buffers.

Representative reproduction commands (exact per-run commands and flags are archived):

```sh
cargo test --locked --all-targets --features local-branches-spike,bytes-interop
cargo test --locked --doc --features local-branches-spike,bytes-interop
cargo test --locked --lib --features local-branches-spike,loom-model
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17 -Zmiri-tree-borrows' \
  cargo +nightly-2026-10-03 miri test --locked --lib \
  --features local-branches-spike,bytes-interop mosaic::raw::
cargo build --locked --release --features local-branches-spike,bytes-interop \
  --example branches_probe --example branches_alloc --example spike_probe --example spike_alloc
target/release/examples/branches_alloc --iterations=10000
target/release/examples/spike_alloc --case=worker-fanout --fanout=20 --queue=8 --iterations=10000
taskset -c 8 target/release/examples/branches_probe --case=churn --mode=local \
  --threads=4 --cpus=0,2,4,6 --size=64 --views=8 --iterations=1000000
python3 scripts/local-branches-performance.py --suite=smoke --output=/tmp/abps-ap-smoke
```

Choose physical CPU IDs valid for the host. The archive's orchestration scripts
record the exact paired and TCP matrices; temporary paths identify this run and
must be adjusted when reproducing it elsewhere.
