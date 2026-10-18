# Bounded diagnostics, resource planning, and batch fan-out

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

The unpublished Berry experiment implements
[SI-07 through SI-10](../CONTRIBUTING.md#si-07-bounded-diagnostic-sampling).
The canonical ownership, admission, shutdown, and return contracts remain in
CONTRIBUTING. This is not the original multi-issuer Shared implementation.

## Diagnostic work is requested explicitly

`Pool::domain_metrics_page(start, dst)` reads a consecutive prefix of the fixed
record table into caller storage. Its `MetricsPage` reports `written`, `next_index`,
and `done`. It examines free records too, with no hidden search for active records.
An empty output makes no progress. Start beyond the table is `InvalidRange`.
Only the written output prefix is valid for this call; the rest stays untouched.

Prepare `vec![DomainMetrics::default(); page_size]` once, process each written
prefix, and reuse it. The defaults are placeholders. A page exposes incarnation,
phase, limit, admitted bytes, allocation claims, owner presence and control-handle
count. The identity remains opaque and local to the sampled pool. Closing/reuse
between pages is allowed. Different counters and pages are not an atomic snapshot.
Never use samples to authorize acquisition, refunds or domain reuse.

`Domain::class_grants_into(dst)` samples all classes into prepared output. In
Reserved mode a short destination is `InvalidSize`, with no writes; in Unreserved
mode it writes nothing. Both APIs allocate nothing, do no slot scan, and perform no
maintenance. Domain-page work is O(page length); grant sampling is O(class count).

`metrics()` remains the cheap aggregate path. Full `snapshot()` is retained for
explicit cold diagnosis and quiescent checks. It still builds all domain/class
entries and slots: Reserved work/storage is O(domains * classes + slots), with
potentially substantial temporary allocations. The page API is not a drop-in
atomic replacement for that diagnostic.

## Capacity preparation and validation

The TCP adapter now builds one typed `PipelinePlan` before creating storage,
listeners or runtimes. Backends and reports consume the same classes, stream
limits, and grants. JSON is generated from that plan, not parsed back as config.

- `--class-slots=64,64` specifies total counts for the two payload classes. Root
  shares and domain grants use deterministic quotient/remainder distribution.
- `--domain-limit-bytes=N` overrides each domain's byte limit. Without it the old
  `(small_capacity + large_capacity) * (64 / workers)` limit is preserved, even
  when slot counts change.
- `--capacity-check=report` reports class/byte deficits and permits overload.
- `--capacity-check=require-envelope` rejects insufficient plans before server
  resources are created.

For each stream, `E = 1 + fanout * (queue + write_batch)`. Strict Reserved plans
need E grants in every possible class; strict Unreserved plans need the sum of
stream envelopes in every root class. Each domain needs `E * max_class_capacity`
bytes. Arithmetic and backend layouts are checked in both modes.

Occupancy includes the admission in progress and destructors through refund
completion. Taking an item out of a queue or publishing a free slot does not end
that stage's pending charge. A transform would need additional output headroom;
this TCP adapter performs none. External retention falls outside this envelope.
A strict result establishes the stated static bounds, not scheduler progress,
completion of interrupted returns, a successful CAS, or timely delivery.

Berry classes still have at most usize::BITS slots per root. Impossible
plans fail; they do not select a new representation or grow storage. For example,
20 subscribers with queue 8 and batch 8 need a conservative E of 321, exceeding
one root's current class ceiling even for one stream. Report mode remains useful
for deliberate overload experiments.

## Grant and ownership transitions

Grant opening resolves sorted requests against sorted classes once. Private class
indices use fixed stack scratch. Validation precedes the Active gate, grant
availability, record availability and root claims. Commit begins only after those
checks. Complexity is O(classes + grants + requested slots), including bit
selection. The reference resolver remains independent, but now uses the same
sequential error precedence as Berry and Mosaic. Differential tests require those
errors to match in equivalent quiescent states; only the reference backend's lack
of a pending maintenance FIFO is normalized away.

`Buf::try_clone_into(&mut [Option<Buf>])` creates identical immutable views. Output
must be empty (`OutputNotEmpty` otherwise). Zero-length batches require no atomic
operation. A nonempty Berry batch checks the finite reference limit and
makes one CAS attempt for the whole batch. Overflow/limit errors are
`ReferenceLimit`; a failed CAS is `Contended`. Errors leave output unchanged.

During construction a private guard owns the reserved references not yet
materialized as handles. On unwind it clears the completed prefix and refunds
remaining rights while the borrowed source still prevents final return. No user
callback or allocation occurs in that transition. Success transfers all rights to
output. Final buffer Drop still publishes its slot, refunds the saved origin, and
releases its lifetime claims; there is no new return queue or grant maintenance.

The TCP flag `--fanout-api=batch` prepares K-1 new handles for K queue permits and
moves the original to the last recipient, before any enqueue. At most three
explicit attempts on Contended apply to a batch, with yields between attempts.
`scalar` remains the default. Batch reduces producer refcount traffic; concurrent
consumer Drops and later scalar clones still share the same refcount.

## Reproduction

Build first, then measure without concurrent compilation:

```sh
cargo build --locked --release --features bytes-interop --examples \
  --config profile.release.debug=1 --config 'profile.release.strip="none"'
cargo test --locked --features bytes-interop --all-targets
cargo test --locked --features loom-model --lib berry::
./target/release/examples/spike_alloc --iterations=10000
./target/release/examples/spike_alloc --op=fanout_batch --iterations=1000
./target/release/examples/spike_probe --case=public-api --op=pages \
  --capacity-policy=reserved --classes=64 --records=65536 --page-size=32
./target/release/examples/spike_probe --case=observe --page-size=32 --records=65536
python3 scripts/berry-performance.py --suite=revisions --target=target \
  --output=/tmp/abps-revisions --repeats=3 --seconds=.6 --tcp-seconds=2 \
  --micro-cpus=0,2,4,6,8,1,3,5 --server-cpus=0,2 \
  --generator-cpus=8 --reader-cpus=4,6
```

Adapt affinity to the host. The revisions suite records its explicit file
descriptor limit adjustment for the 64-stream/fan-out-20 scenario, retains raw
results and hashes, and runs a fixed-seed shuffled schedule. The smoke suite also
checks real TCP batch fan-out with a fully covered strict resource envelope on
Berry, reference and bounded Bytes. Dedicated hosts are required for
performance gates and any decision to change defaults.

## Validation and evidence: 2026-10-07

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The machine-readable evidence
(`evidence/bounded-diagnostics-and-fanout-2026-10-07.json`) contains source/binary
identities, commands, per-trial results, offered-load categories, resource observations,
checks and the raw-artifact directory. No repository dependencies changed. The checkout
has no Git metadata; the baseline was preserved before editing and its complete file
hashes were verified again.

Passed locally: formatting; Clippy with warnings denied for default and all
features; default and bytes-interop all-target tests; both doctest configurations;
the independent four-ledger property model with seed 424242, extended with batch
creation; eleven production-protocol Loom models; twenty-six Berry Miri
tests on seeds 1/17/42 and five reservation/batch model regressions under Miri.
Loom uses the existing two-preemption bound and 10,000 per-execution branch bound;
this is model evidence, not an exhaustive proof of every execution.

The allocation harness's intentional allocation/free positive control passed.
Across 1,000 combined page/grant/batch calls on a prepared 65,536-domain, 64-class
Reserved pool, allocations, frees, reallocations and allocated bytes were all
zero. Separate prepared buffer, failure, maintenance and first remote return paths
also remained allocation-free. In isolated copies, regressions detected three
mutations: examining extra domain records, losing a grant on failed preflight,
and omitting the refund of unmaterialized batch references.

### Exploratory native measurements

201 native timing runs were accepted. Two additional runs adjacent to compiler-watch
observations were conservatively excluded and repeated; their original files remain.
The whole-job watch also sees the runner's `rustc -Vv` metadata query just before
process creation. Repeats used per-run watches and had no observations. Allocation
instrumentation and four cpu-clock profiles ran separately from native timing.

Host: shared Hyper-V VM exposing ten logical CPUs/five SMT core pairs. Microbench
runs used CPU 0, server workers 0/2, generator 8 and readers 4/6. Results below are
medians of three runs, not thresholds or hardware-independent guarantees.

| Operation/configuration | Before | After |
| --- | ---: | ---: |
| Reserved open/drop, 1 class, p50 | 70 ns | 70 ns |
| Reserved open/drop, 4 classes, p50 | 80 ns | 90 ns |
| Reserved open/drop, 16 classes, p50 | 261 ns | 160 ns |
| Reserved open/drop, 64 classes, p50 | 3,097 ns | 451 ns |

The open/drop comparison links the same probe source against the preserved and
updated libraries. Linear resolution improves the many-class case; the small
four-class case does not improve. Profiles put about 95% of baseline samples in
opening/resolution versus about 65% after; recycling becomes a larger share of
the shorter operation. The algorithmic bound is established independently by a
regression that counts production resolver visits.

| Prepared Berry fan-out | Scalar | Batch |
| --- | ---: | ---: |
| One additional view, complete batches/s | 22.51 M | 17.25 M |
| Twenty additional views, complete batches/s | 2.44 M | 8.10 M |
| One additional view, separately timed p99 | 61 ns | 81 ns |
| Twenty additional views, separately timed p99 | 451 ns | 151 ns |

Both variants drop all generated views in the measured iteration. These are
producer-side microbenchmarks. The TCP batch adapter additionally transfers the
original to its last recipient. The one-view regression and remaining consumer
refcount contention are reasons to retain scalar as default; this is not a general
speedup for concurrent clones of one backing.

On the empty 65,536-domain/64-class pool, full snapshot p50 was 58.4 ms, aggregate
metrics 100 ns, and a 32-record page 60 ns. These operations intentionally provide
different information. A page does not build the per-domain class matrix or derive
live/pending accounting from all slots. Full snapshot is still expensive; neither
those empty-pool timings nor a page boundary promise latency under arbitrary load.

### Capacity remains the main TCP constraint

All profiles below use 64 streams, two issuer workers, fan-out 20, queue 8, writer
batch 8, explicit Skip, a 100 ms server freshness limit, remote return, initialized
ingress and 20,000 offered source frames/s for two seconds. Byte limits are equal
across slot counts; 128 total slots give two grants per domain/class instead of one.

| Backend / total slots per class | Scalar delivery | Batch delivery |
| --- | ---: | ---: |
| Berry / 64 | 78.55% | 78.91% |
| Berry / 128 | 96.92% | 96.86% |
| Budgeted Bytes / 64 | 74.46% | 76.76% |
| Budgeted Bytes / 128 | 97.73% | 96.64% |

Berry scheduled-arrival p99 was approximately 6.3–8.0 ms across these
profiles. Offered work, pacing misses, ingress/class failures, queue losses and
late deliveries remain in the evidence. All completed TCP runs passed flow
conservation and final drain checks, including the strict-envelope batch smoke
profiles for reference, Berry and Bytes.

Adding slots increases configured backing; it does not prove universal pipeline
progress. Batch did not establish a TCP throughput or tail-latency advantage in
this workload. Cycle measurements also varied despite no change to acquisition or
final-return protocol; they are retained without attributing the variation to a
new allocator algorithm. No long soak, new fuzz/sanitizer campaign or cross-target
performance validation was performed for this revision.
