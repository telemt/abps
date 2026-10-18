# Hard reservations and TCP freshness

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

This follow-up implements the accepted [SI-05 and SI-06](../CONTRIBUTING.md#si-05-hard-class-reservations-reference-and-berry)
experiment. The canonical contract remains CONTRIBUTING; these are implementation
and measurement notes. Neither backend is a production multi-issuer allocator.

## Changes and boundaries

`Config::capacity_policy` defaults to `CapacityPolicy::Unreserved`. Reserved roots
use `Pool::try_open_reserved_domain(byte_limit, &[ClassReservation])`. Each entry
names an existing class and a positive slot count; entries must be strictly sorted.
An empty list reserves no slots. Using the wrong open method is
`InvalidConfiguration`. Limits and grants remain distinct: the byte limit can be
smaller than the grant's total backing, and pending charges still consume it.

Opening validates all grants, checks unassigned capacity, obtains a domain record
and lifetime claims, then commits. `ReservationUnavailable` leaves no partial
grant or consumed record. Acquisition chooses the usual smallest fitting class;
no free eligible bit means `DomainClassDepleted`, even when another domain has
idle slots. It does not spill into a larger class or borrow another grant.

The Berry owns the mask matrix and each class's unassigned mask. Remote
buffer operations never access these fields. Only preparation creates the matrix:
`domain_slots * class_count * size_of::<usize>()`, with checked arithmetic and
alignment included in root admission. Unreserved roots omit the matrix. The safe
reference uses per-slot assignment under its existing mutex exception; the Bytes
benchmark uses prepared grant/use counters under a mutex and still allocates its
payload and owning wrapper.

All slot metadata now has 64-byte alignment and a size that is a multiple of 64.
`Layout::array` and final layout padding charge the full space. This separates
independent refcounts on the measured host; it cannot remove contention among
clones of one backing and is not a portable hardware cache-line promise.

### Return and reuse proof

The four ledgers and the return synchronization are unchanged. A granted slot's
bit is cleared with the existing acquire RMW. Its final reader completes the
release/acquire refcount protocol, saves the origin operands, publishes the bit,
refunds admission, ends the domain claim, then releases the root claim. The return
path neither changes grants nor performs extra atomic operations.

Publishing a free bit does not authorize transferring its grant. A domain must
be closed, have no control handles, and pass the existing acquired `drained()`
predicate before issuer-side recycle returns its masks. Thus a return suspended
between publication and refund still pins the old grant. Forgotten ownership can
pin it permanently. Generation retirement releases drained grants without reusing
the exhausted identity. Physical storage teardown remains separate cold work.

Until maintenance recycles a closed record, even physically free slots remain in
that incarnation's grant. A new reserved-domain open can therefore return
`ReservationUnavailable` while domain records are free and old grants await
recycle. Callers that want another attempt explicitly schedule `poll_domains(n)`;
there is no implicit sweep on either open error.

`poll_domains(n)` visits at most n entry-time candidates. Releasing a reserved
record additionally visits the fixed class table, so its bound is proportional to
`n * class_count`. The example `open_after_maintenance` now examines at most 32
records per call and returns the remaining error; it no longer traverses the whole
pending queue through hidden batches. The `pending-domains` probe measures both
individual polls and explicit caller retries to a ready tail.

Cold snapshots expose physical `available` slots separately from assigned
`reserved` and `unassigned` rights, with per-domain grants and eligible free slots.
These are exact at quiescence, not an atomic concurrent snapshot. In Unreserved
mode there are no assigned rights and `unassigned == total`, even for live slots.

## TCP integration

`--capacity-policy=reserved` divides every root's class slots evenly among its
streams; remainders go to increasing stream IDs. All backends use identical slot
counts, byte limits, queue policy, and batch bounds. PipelinePlan reports each
domain's grants, queue/writer envelope, and one ingress headroom slot. A stream can
exhaust its own grant; reservation does not guarantee that its full conservative
queue envelope fits. Capacity isolation does not isolate CPU scheduling or I/O.

`--max-frame-age-ms=100` enables the initial freshness profile; omission leaves
freshness disabled. The server starts the deadline when a complete frame and its
views are ready to publish. Writers check the earliest remaining batch deadline
before every socket poll, including an immediately ready write. A timer alone
that polls the I/O future first would not suffice. Expiry disconnects the writer
and drops its batch and queued ownership. Partial frames cannot be skipped on a
continuing connection. Already accepted kernel bytes may still arrive late.

Clients measure age from the planned source arrival, independently of the server
deadline. `delivered`/`bytes` are network delivery; `timely_delivered` and
`timely_bytes` count delivery before the configured limit. `expired_delivered`
remains in delivery latency histograms and the offered-load denominator. With
freshness disabled all deliveries are classified as timely, and the recorded
`max_frame_age_ms` is null. No transport-wide latency guarantee is claimed.

`--send-buffer-bytes` is a cold socket adapter option, disabled by default.
Each socket reports requested and observed values; the OS may clamp or transform
the request. This setting does not enter the allocator core. Batch size remains at
most eight already-ready frames, without waiting to coalesce. The optimized
explicit profile is queue 8, batch 8, read-buf ingress, and 100 ms freshness;
initialized ingress is also compared across all backends for a matched baseline.

## Reproducing validation and measurements

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --features bytes-interop --locked
cargo test --doc --features bytes-interop --locked
cargo test --features loom-model --lib berry:: --locked
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17' \
  cargo +nightly-2026-10-03 miri test --features bytes-interop --lib berry:: --locked
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=17' \
  cargo +nightly-2026-10-03 miri test --features bytes-interop --test model berry::reservations:: --locked
```

Miri seeds are 1, 17, and 42. Leak suppression is solely for intentional
forget/abandonment tests. Loom uses the production masked bitmap, grant transfer,
refcount, return, and drain helpers with a preemption bound of two and at most
10,000 branches. The independent property oracle models per-domain class counts,
allocation identities, bytes, and the four ledgers rather than duplicating bitmaps.
Mutation checks run in separate source/target directories.

Build before any measurements. Keep native timings, allocation instrumentation,
profiling, and the soak in separate runs. CPU lists below describe the recorded
host and must be adjusted on other machines.

```sh
CARGO_TARGET_DIR=/tmp/abps-reservation-target \
  cargo build --release --features bytes-interop --examples --locked \
  --config profile.release.debug=1 --config 'profile.release.strip="none"'
/tmp/abps-reservation-target/release/examples/spike_alloc --iterations=10000
python3 scripts/berry-performance.py --suite=isolation \
  --target=/tmp/abps-reservation-target --output=/tmp/abps-isolation \
  --server-cpus=0,2 --generator-cpus=8 --reader-cpus=4,6 \
  --seconds=.7 --tcp-seconds=4 --repeats=3
/tmp/abps-reservation-target/release/examples/spike_probe \
  --case=soak --seconds=1800 --rate=2000 --seed=42 --cpus=0,2
```

`concurrent-views` supports `--slot-offset=0..7` and `--slot-stride=1|2` to expose
address-phase effects. `pending-domains` accepts `--pending-records=4095` and
`--maintenance-batch=32`. The soak uses eight active reserved domains, sixteen
prepared records, bounded cross-thread retention, tiny views of full backing,
explicit failures, repeated domain replacement, periodic reservation/admission/RSS
observations, and a final full drain. It is a resource test, not a latency gate.

## Measured results: 2026-10-04

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The evidence file (`evidence/reservations-performance-2026-10-04.json`) records source
and binary hashes, commands, individual samples, summaries, and validation results. Raw
artifacts are under `/tmp/abps-reservations-2026-10-04/`. The original source and
binaries were preserved before changes. Native measurements, instrumentation, tracing,
and the 30-minute resource soak ran separately.

The host is a shared Hyper-V VM exposing ten logical CPUs in five SMT pairs on
an AMD Ryzen 9 5900X, with Rust 1.96.0. Release builds retain debug information and
use no target-cpu or LTO override. TCP uses server CPUs `0,2`, pacer `8`, and readers
`4,6`: two server workers, four streams, twenty subscribers each, equal total class
capacity, and explicit Skip. These are experimental comparisons, not dedicated-host
performance gates. Detected unrelated compiler activity invalidated samples;
detection at sample boundaries does not eliminate all shared-host interference.
The main series has 114 accepted runs: 24 core, 12 maintenance, and 78 TCP; two
compiler-overlap runs are excluded. Core and TCP configurations have three trials;
each maintenance configuration contains 25 episodes. Six additional TCP runs keep
the client's 100 ms measurement threshold while disabling the server deadline.

### Metadata placement

The controlled comparison compiles the same standalone probe against the original
library, the reservation implementation without padding, and that implementation
with only `#[repr(align(64))]` added to Slot. It measures offsets 0..7 and strides
1/2, twice each, with 0.15 s warm-up and 0.7 s measurement. Another 27 trials cover
shared-backing contention: 219 accepted trials in total. The 116 initial or
compiler-overlap samples are excluded and preserved separately.

Median successful clone/drop pairs, millions/s, across the sixteen samples per cell:

| Implementation | 4 threads, adjacent | 4 threads, stride 2 | 8 threads, adjacent | 8 threads, stride 2 |
| --- | ---: | ---: | ---: | ---: |
| Original | 164.10 | 164.42 | 271.56 | 320.52 |
| Reservations, no padding | 163.11 | 163.00 | 270.50 | 319.93 |
| Reservations, padded | 166.40 | 165.43 | 321.94 | 320.66 |

Padding improves the eight-thread adjacent-slot median by about 19% and removes
most of the stride/address-phase gap. On four threads, most phases were already
fast; the worst adjacent sample improves from 105.15 to 155.89 M/s, while the
median changes little. All distinct-slot samples have zero Contended failures.
With eight threads cloning the same backing, roughly 50% of attempts still return
Contended. This supports separating independent metadata, not a universal allocator
or TCP throughput claim.

For one 64-slot/64-domain microbenchmark root, accounted metadata grows from
9,528 bytes in the original implementation to 10,064 unreserved and 10,576 reserved.
The latter includes the 512-byte grant matrix; both include all layout padding.

### Reservation cost and bounded maintenance

Reservations add eligibility lookup and masking without changing the return
protocol or adding RMW operations. They are not free:

| Policy | 1 worker, native M cycles/s | 4 workers, native M cycles/s | 1 worker, sampled p99 | 4 workers, sampled p99 |
| --- | ---: | ---: | ---: | ---: |
| Unreserved | 16.80 | 62.51 | 91 ns | 100 ns |
| Reserved | 15.23 | 63.31 | 100 ns | 100 ns |

Each cycle acquires, freezes, and drops. Native throughput and per-operation
latency instrumentation are separate runs. The one-worker reserved median is
9.3% lower; four-worker ranges overlap substantially (57.85..67.40 versus
61.96..64.16 M/s), so they do not establish a throughput improvement. Instrumented
one-worker p99.99 medians are 14.38 and 17.65 microseconds respectively. Those tails
include host scheduling and do not contradict bounded algorithmic work or prove
a wall-clock bound.

With 4,095 busy records before one ready record, the Unreserved maintenance probe
measures these per-poll and explicit caller-retry costs:

| Poll budget | Poll p50 | Poll p99 | Complete caller retry p50 |
| --- | ---: | ---: | ---: |
| 1 | 30 ns | 41 ns | 318.98 us |
| 32 | 201 ns | 300 ns | 32.24 us |
| 256 | 1,513 ns | 1,663 ns | 25.44 us |

The adapter's 32-record cap bounds one call; it does not make reaching a distant
ready record constant-time. Reserved recycle additionally releases one mask per
configured class. That work remains explicit and is not shifted into remote Drop.

### Capacity isolation

The frame workload offers 500 frames/s in total for four seconds, with one slow
subscriber on stream 0. Stream 2 shares its root. Values are medians of three runs;
class refusal percentages use frames received by that stream as the denominator.
Delivery uses all 40,000 planned subscriber deliveries, including source misses.

| Berry policy | Queue | Stream 0 class refusals | Stream 2 class refusals | Total delivered |
| --- | ---: | ---: | ---: | ---: |
| Unreserved | 32 | 36.60% | 37.00% | 81.26% |
| Reserved | 32 | 69.60% | 0% | 82.55% |
| Unreserved | 8 | 0% | 0% | 99.06% |
| Reserved | 8 | 0% | 0% | 99.00% |

Reserved failures are DomainClassDepleted; Unreserved failures are ClassDepleted.
All reserved trials protect the neighbor's class capacity. The overloaded stream
pays for its own retained backing and loses access to idle neighboring grants.
This is isolation, not a promise of greater aggregate delivery. Under the same
reserved queue-32 policy, reference and budgeted Bytes deliver 82.70% and 82.60%,
also with zero neighbor class refusals. Queue sizing remains independently useful.

### Freshness and actual delivery

The following Berry frame runs all use hard reservations, queue 8, batch 8,
initialized ingress, and a **client** measurement threshold of 100 ms. The two
disabled-server rows are the additional matched-threshold experiment; disabled
freshness in the ordinary harness otherwise classifies every delivery as timely.

| Server deadline | Requested send buffer | Observed send buffer | Timely offered delivery | Slow subscriber p99 |
| --- | ---: | ---: | ---: | ---: |
| Disabled | OS default | 4.2 MB | 98.66% | 2,403 ms |
| Disabled | 65,536 B | 131,072 B | 98.57% | 1,205 ms |
| 100 ms | OS default | 2.6..4.2 MB | 98.52% | 930 ms |
| 100 ms | 262,144 B | 425,984 B | 98.61% | 304 ms |
| 100 ms | 65,536 B | 131,072 B | 98.66% | 178 ms |

Each deadline-enabled frame run expires exactly one writer. Late deliveries remain
in the histograms and loss accounting. Limiting queued TCP bytes improves the
slow-reader tail on this host, but neither setting makes it a 100 ms delivery SLO.
With the same 65,536-byte request and explicit read-buf ingress, timely delivery
is 98.66% and slow-reader p99 is 151 ms; the small difference is not evidence of
a general ingress-speed advantage. The default core and adapter policies remain
unchanged unless these options are requested.

At 20,000 packets/s, no slow reader, reserved queue 8, initialized ingress and the
same 100 ms deadline, median delivery is 94.33% Berry, 94.66% reference,
and 96.93% budgeted Bytes. Useful delivered payload is 379.60, 380.82, and
390.04 MB/s respectively. The explicit read-buf/smaller-send-buffer profile reaches
95.34% and 383.59 MB/s. This experiment does **not** demonstrate an overall TCP
throughput win over the bounded Bytes baseline. Source scheduling misses and
unavailable subscriber queues account for most losses; capacity failures are zero
in these packet cases. Keep offered-load and queue measurements ahead of further
allocator micro-optimization.

### Separate tracing

Two core and two TCP perf runs use the software cpu-clock event at 199 Hz, with
DWARF call graphs; a separate TCP run collects strace syscall counts. No samples
are reported lost. These runs do not contribute native throughput results.

The core profiles put about 30..35% of samples in final lease Drop and 18..20% in
acquisition. Reserved grant lookup accounts for about 1%; percentages alone do
not measure the incremental reservation cost. The four-ledger return obligations
remain intact.

The frame and packet TCP profiles spend 93.66% and 82.26% of sampled CPU time in
the kernel. Frame copying, TCP processing, and page management dominate the frame
profile; packet processing spreads cost across networking and syscall paths.
Whole-process strace records 40,509 writev calls, including 1,440 errors, plus
27,017 write calls. It includes setup and shutdown; futex time includes waiting
and is not evidence of an allocator mutex. Tracing itself changes delivered load
substantially, so these counts identify work rather than a production rate.

### Correctness and resource lifetime

Formatting, Clippy with warnings denied, default and bytes-interop tests, and
documentation tests pass. Validation includes the independent property oracle,
eight Loom models, and three Miri seeds (twenty storage tests and four reservation
tests per seed). Three isolated mutations are detected: ignoring a grant mask,
recycling a grant before drain, and bypassing the socket deadline guard. The
allocation harness detects its positive control. Its final 10,000-iteration run
records zero allocations, deallocations, and reallocations on prepared success,
failure, reservation/recycle, maintenance/interop, and previously uninvolved-thread
return paths. This does not cover allocations owned by Tokio or the application.

The 1,800-second resource soak completes 3,600,000 iterations, 3,233,141 remote
returns, and 204,474 domain replacements. There are 366,859 explicit refusals and
no external queue overflow. The final check finds every slot and domain record
free, every grant unassigned, and all admitted bytes refunded; owner destruction
succeeds. Root reservation stays exactly 67,131,792 bytes throughout.

RSS starts at 5,272 KiB before payload pages are touched and finishes at 71,216 KiB.
The last five-minute range is 71,164..71,216 KiB. After the initial page population,
RSS grows by roughly 0.3 MiB while the harness accumulates 360 cold diagnostic
samples. This is resource-conservation and drain evidence, not a claim of perfectly
flat process RSS or a proof about untested workloads.

This change has no sanitizer/fuzz campaign, other-target validation, hardware PMU
measurements, or dedicated-host performance gate. The original multi-issuer Shared
helping protocol remains a separate unimplemented contract. The demonstrated gains
are capacity isolation, reduced independent-refcount interference, bounded explicit
maintenance, and an observable freshness policy with measured limits.
