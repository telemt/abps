# Local branch ownership experiment

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

`local-branches-spike` implements [BR-01..06](../CONTRIBUTING.md#local-branch-experiment)
on Mosaic storage. A transferable `Branch` owns one shared backing right.
After `into_local()`, its `LocalBuf` family counts independent immutable views with
a checked `Cell`, without touching the shared refcount. Only its last local owner
releases that shared right through the existing SI-11 protocol.

This helps repeated local ownership churn under cross-thread contention. It is
disabled by default. The complete small-frame cycle and TCP results below do not
justify promoting it over ordinary or batch `Buf` ownership.

The subsequent [AP hardening report](accounting-fanout-hardening.md) records the
compact LocalBuf layout, physical-core harness corrections and `worker-branches`
adapter. Measurements below describe the earlier revision and harness; they are
not directly comparable to the revised timing windows or CPU placement.

## API and resource contract

Enable `local-branches-spike` and configure the Mosaic root before preparation:

```rust
use abps_reference::mosaic::{ClassConfig, Config, LocalBranchConfig};

fn configure(config: &mut Config) {
    config.local_branches = Some(LocalBranchConfig {
        max_branches_per_slot: 20,
        local_reference_limit: 256,
    });
}
# let mut config = Config::new(vec![ClassConfig { capacity: 64, slots: 1 }], 1, 1 << 20);
# configure(&mut config);
# assert_eq!(config.local_branches.unwrap().max_branches_per_slot, 20);
```

Fill a `BufMut`, then pass an empty, caller-prepared `&mut [Option<Branch>]` to
`freeze_branches_into`. An error retains the original writable buffer:
`error.into_parts()` returns `(Error, BufMut)`. Nonempty output, prepared branch
capacity and the global reference limit are checked in that order. Empty output
consumes/releases the lease. Construction is transactional, including unwinding.

Move a `Branch` to the destination worker before calling `into_local()`. Branch is
Send, !Sync and not Clone. LocalBuf is !Send + !Sync; `try_clone` and `try_slice`
check its independent local limit and never return Contended. Borrowed `try_view`
and `try_as_io_slice` do not acquire ownership. The fallible descriptor checks the
view length against Windows' single-descriptor limit; `as_io_slice` can panic
there. Construct Config with `Config::new`, then set `local_branches`. `try_into_branch` returns a transferable
handle only when that family has one owner; otherwise it returns the same LocalBuf.
There is no conversion of arbitrary local views to shared Buf handles.

The prepared array costs `total_slots * max_branches_per_slot * 64` bytes, plus
checked root layout/padding. For 1,024 slots and 20 branches this is 1.25 MiB of
additional metadata. It is charged even when a particular frame uses fewer branches.
No TLS, registration, per-view heap object or sidecar allocator is introduced.
The 64-byte alignment is a layout choice, not a universal cache-line guarantee.
AP-06 puts the immutable root/slot/payload/capacity/limit descriptor alongside the
counter in each prepared record. On the measured 64-bit target, the record remains
64 bytes and LocalBuf is three words (24 bytes), instead of five words (40 bytes).

Global reference diagnostics count branch rights, including construction and
unfinished releases. They never read another thread's local counter. Each family
retains the full original backing charge; forget can pin it permanently. Close,
abandonment and owner-side destruction retain their existing semantics. A record
cannot be reused independently of its backing slot.

## Implementation and validation

The private storage extension initializes records inside the checked arena layout.
Exclusive freeze establishes K global rights with a store before any publication;
the guard owns rights until their handles exist. Local count 1->0 completes access
to the record before the existing shared release. No slot access follows bit
publication, and the origin refund remains the last storage access. The terminal
return also borrows only the admitted atomic word: retaining a padded ledger borrow
through owner deallocation would violate OWN-03 even without a later access. See
the [final-refund correction](final-refund.md). The branch experiment does not add
local-counter work to ordinary Buf clone/Drop or the Berry backend.

The TCP adapter's explicit `--fanout-api=local-branches` mode carries Send Branch
handles in bounded queues and binds LocalBuf only inside the writer's LocalSet.
Queue permits still precede acquisition; all output is prepared before enqueue.
Cancellation and deadline expiry release both bound writer frames and unbound
queued branches. The typed resource plan and supervisor include sidecar reservation.
Scalar remains the default; core code has no Tokio dependency.

Validated locally on 2026-10-09:

- 196 native test executions with local branches and bytes interop, including
  repeated example harness tests; 35 default-feature executions and 18 doctests.
- Independent generated ownership/range model: 96 native cases. Tests compare
  reservation, capacity, admission and global ownership after each operation.
- 28 production-protocol Loom models, including four new branch models using
  instrumented Cell/atomics. These cover unique migration, guard rights, stopped
  releases and sidecar reuse before old refunds. Exploration has a preemption
  bound of two; it is evidence, not a soundness proof.
- Nine focused native-storage tests under Miri at seeds 1, 17 and 42; three
  generated cases under Miri at seed 17. Leak reports are ignored for intentional
  forget/abandonment; isolation and memory/aliasing checks remain enabled.
- Four isolated mutation controls fail their named regressions: missing local
  increment, nonunique migration, lost construction rights and missing final
  Acquire synchronization. Each uses an independent Cargo target.
- Allocation instrumentation detects its positive control and reports zero
  allocations, frees and reallocations in measured branch operations, including
  the first bind/clone/return on a previously uninvolved thread.
- Rustfmt and Clippy with warnings denied, including all features and the minimal
  local-branches configuration. CI jobs were updated; remote CI was not run here.

The first mutation harness used live handles and aborted during cleanup of the
deliberately broken increment. The final counter-level regression produces a clean
test failure. Initial Miri property execution encountered proptest filesystem
persistence; persistence is now disabled only under Miri, retaining isolation.
Both initial failures are retained in raw evidence.

## Measurements and limits

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The summary (`evidence/local-branches-2026-10-09.json`) and compressed raw evidence
(`evidence/local-branches-2026-10-09.raw.json.gz`) retain commands, source/binary
hashes, all attempts, latency samples, test logs and the ordinary Buf control. The
matrix has seven interleaved throughput repetitions, one separately instrumented latency
run per configuration, and 21 TCP trials.

This is a shared 10-vCPU x86_64 VM, not the dedicated performance host required for
promotion. Workers use distinct configured CPUs. The 16/32-thread cases are
explicitly unvalidated. `perf` is unavailable; no PMU or cache-to-cache measurements
were collected. Disassembly of the measured x86_64 binary shows plain local
increment/decrement instructions; only the last local decrement calls shared return.

For a retained 64-byte backing, groups of eight independently owned views per
worker, 30,000 groups per worker, medians are:

| Threads | Scalar: M accepted views/s; rejected | Batch: M accepted views/s; rejected | Local: M accepted views/s; rejected | Borrowed: M views/s |
| --- | --- | --- | --- | --- |
| 1 | 39.77; 0% | 48.51; 0% | 38.07; 0% | 254.42 |
| 4 | 19.71; 45.28% | 42.14; 35.14% | 117.66; 0% | 481.70 |
| 8 | 14.49; 62.59% | 28.94; 76.80% | 234.48; 0% | 1,011.94 |

Every rejected view remains in the offered denominator. Batch failures reject an
entire requested group. Borrowed descriptors have a narrower lifetime and cannot
replace independent ownership in every application. Initial fan-out is outside
this churn window; dispatch, view operations and ending owner Drops are included.

The full lifecycle probe includes acquire, initialization, fan-out, dispatch,
local work and completed reclamation. With 64-byte frames, eight views per branch
and 64 frames per cohort, accepted view throughput is:

| Threads | Scalar M views/s | Batch M views/s | Local M views/s |
| --- | --- | --- | --- |
| 1 | 16.78 | 18.42 | 13.51 |
| 4 | 45.81 | 49.02 | 41.65 |
| 8 | 58.13 | 65.13 | 49.57 |

Local branches are slower in this complete-cycle workload despite avoiding view
refusals. Preparation and access to additional records have a cost; these timings
do not isolate its hardware cause. Raw `frames_per_s` means processed backings,
including those with rejected view attempts, not fully delivered application frames.
These are fixed-work saturation probes, not independently scheduled arrivals.

In the eight-worker Drop-only probe, completed owner drops were 19.55/17.26/14.58
million/s for scalar/batch/local. The shared decrement per branch remains. Timing
includes dispatch and mixes final and nonfinal shared Drops; it does not prove a
standalone last-Drop latency improvement. p99.99 is left unset for insufficient
samples.

The real TCP comparison uses two streams, four subscribers, queue eight, writer
batch eight, remote writers, hard class reservations, explicit Skip and a 100 ms
deadline. A separate process schedules 500 frames/s total for three seconds.
Median timely delivery fractions are 99.93% / 100% / 99.87% for scalar/batch/local;
useful throughput is approximately 2.01 MB/s in all modes. Median per-run arrival
p99 is 0.567 / 0.790 / 0.673 ms. This offered-load test establishes integration and
accounting behavior, not an increased TCP throughput ceiling.

A separate seven-pair control of the unchanged ordinary owning-view path measured
18.54 M accepted operations/s before and 18.58 M after, with 65.30% / 65.46%
refusals. That diagnostic repeatedly creates empty views and differs from the group
workload above. It shows no material change in this control; local branches do not
make arbitrary cross-thread Buf cloning contention-free.

Use borrowed views when their lifetime suffices. Use this experiment when a branch
needs many independently owned local views. Keep ordinary shared Buf for arbitrary
cross-thread ownership, and validate the complete workload before choosing a mode.

## Reproduction

```sh
cargo build --release --features local-branches-spike,bytes-interop --examples --locked
python3 scripts/local-branches-performance.py --suite=compare --repeats=7 --iterations=30000 --tcp-seconds=3 --output=/tmp/abps-branches-comparison
python3 scripts/check-branch-mutations.py --output=/tmp/abps-branch-negative-controls
cargo test --features local-branches-spike,bytes-interop --all-targets --locked
cargo test --features local-branches-spike --doc --locked
CARGO_TARGET_DIR=/tmp/abps-branch-loom cargo test --features local-branches-spike,loom-model --lib protocol::tests:: --locked
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17' cargo +nightly-2026-10-03 miri test --features local-branches-spike --lib branches:: --locked
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17' PROPTEST_RNG_SEED=424242 cargo +nightly-2026-10-03 miri test --features local-branches-spike --test local_branches --locked
```

The reused measurement runner initially emitted a historical `debug=1` build label.
The summary corrects it to the actual release command above with no profile
overrides; original metadata remains in raw evidence. The runner now identifies
binaries as caller-supplied instead of guessing their build flags.
