# Mosaic: segmented single-issuer experiment

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

This is an opt-in, unpublished implementation of [SI-11..15](../CONTRIBUTING.md#mosaic-segmented-single-issuer-experiment).
It retains worker-local admission and cross-thread buffer ownership. It does not
implement the original multi-issuer Shared algorithm, helping, a quota tree, or
hard wall-clock guarantees. The reference and original Berry source remain
independent baselines.

Enable `mosaic-spike` and import `abps_reference::mosaic`.
The default crate exports are unchanged. `bytes-interop` additionally enables the
checked mutable cursor; Tokio remains an example/dev dependency. Mosaic Config
is non-exhaustive: construct it with `Config::new`, then assign public fields.
Downstream struct literals and struct-update syntax are not supported.

```rust
use abps_reference::mosaic::{ClassConfig, Config, Error, StorageOwner};

fn packet() -> Result<(), Error> {
    let config = Config::new(vec![ClassConfig { capacity: 1500, slots: 321 }], 8, 1 << 20);
    let mut storage = StorageOwner::prepare(&config)?;
    let pool = storage.activate()?;
    let (stream, domain) = pool.try_open_domain(321 * 1500)?;
    let mut packet = domain.try_acquire(1500)?;
    packet.put_slice(b"headerpayload")?;
    let packet = packet.freeze();
    let header = packet.try_view(0..6)?;
    assert_eq!(header.try_as_io_slice()?.as_ref(), b"header");
    drop(packet);
    drop((domain, stream, pool));
    storage.try_destroy()
}
# packet().unwrap();
```

`Config::control_handle_limit` includes StorageOwner and all local control handles,
not allocations. This deliberately differs from the old backend's `handle_limit`.
Reference limits remain checked and fallible. An empty frozen buffer still holds
its full positive slot charge. BufView is borrowed; it is not independently owned.

## Proof boundaries and transitions

### Positive charge and cold destruction (SI-11)

The mathematical ledger sums positive charges of pending acquisitions, issued
allocations and published-but-unrefunded returns. The sole issuer adds a checked
charge with one CAS; one return/rollback subtracts it exactly once. A failed CAS
changes nothing. Sharing a view neither adds nor migrates charge. These rules
establish non-negativity and the exact domain limit without an allocation counter.

Acquisition cannot interleave with close or maintenance on its worker. Its private
rollback guard covers every stage before commit. After a claimed bit, rollback
clears private ownership state and publishes the bit before refund. There is no
user callback or fallible operation in public commit. Tests inject failures after
charge, bit claim and ownership initialization.

The last reader uses Release decrement and an Acquire fence to include preceding
readers. It copies the word pointer, bit, immutable capacity and origin ledger
before Release publication. Publication permits overwrite, so the return never
reads that slot again. Its charge remains positive through the Release refund,
which is the last storage operation. No root counter, allocation count, grant
maintenance, destructor, callback or tracing hook follows that refund. The terminal
PublishedReturn borrows only the atomic admitted word, not the padded DomainLedger.
No live terminal borrow may keep non-atomic arena bytes protected through owner
deallocation; see the [borrow-lifetime correction](final-refund.md).

A closed domain with no controls cannot gain a new charge until reuse. Issuer
program order prevents its drain load from observing the initial zero preceding
its own admissions. Acquire observing a subsequent zero includes all completed
refunds through the RMW release sequences, including interleaved issuer CASes.
Strictly positive charges imply that no allocation or unfinished return remains.
Only then may the record change generation and its grants be reassigned. Generations
retire instead of wrapping; the admitted word is not reset while returns exist.

Root destruction first closes admission, requires the owner to be the only local
control, and observes Acquire zero in every domain. Closed counters cannot grow,
so the successful scan establishes complete drain by its end. It does not sum
counters. Failed drain retains the owner, or owner Drop abandons the entire arena.
Abandonment diagnostics are independent of the arena; admitted totals are observed,
saturating values with an explicit `admitted_saturated` flag, never a drain proof.

### Exact multiword failure (SI-12)

For each eligible bit, during one acquisition there is at most one transition from
zero to one, until this sole issuer claims a bit and immediately finishes its search.
Other threads cannot clear a bit. Grant masks and the searched class are immutable
for the operation; invalid tail bits are always zero. Before publication a pending
last return still owns issued capacity. There is no separately delayed capacity credit.

The first pass is only an optimization: it may miss a recent return. After a miss,
each confirmation is an Acquire `fetch_or(0)`, an identity RMW. An RMW reads its
immediately preceding modification-order value. If every confirmation is zero,
none of the eligible publications can precede its own confirmation. Because the
eligible sets only grow and confirmations are sequenced, their negative observations
admit a common empty cut at the first confirmation. This is a monotone negative
observation, not a general coherent multiword snapshot. Independent stale loads
cannot replace the confirming RMWs.

A positive observation is safe to claim without a CAS retry: only this issuer
can remove that bit. Its Acquire fetch-and reads a Release publication or its RMW
release sequence before ownership metadata and payload are reused. Bounded Loom
models instantiate this actual search/return/refcount code, including a publication
missed by the first pass. They do not prove every weak-memory execution or platform.

| Operation | Library work bound |
| --- | --- |
| Class lookup | At most seven comparisons for at most 64 classes |
| Successful fast acquisition | One quota CAS, loads through the winning word, one bit RMW |
| Confirming search | At most 2W word probes and W+2 total RMWs |
| Rollback after successful claim | At most two additional RMWs (publication and refund) |
| Nonfinal Drop | One refcount RMW |
| Final Drop | Three RMWs and an Acquire fence |
| Owned clone / slice | One checked refcount CAS |
| Borrowed view | Range checks, no atomic operation |
| Domain maintenance | At most n records and n*C*W masks |

`W <= 256`, and Reserved searches use only their eligible mask nodes. An empty grant
has zero eligible words. A successful search retains the winning word/node as the
next starting point; this favors reuse and does not promise round-robin placement
or fairness. Traversal advances with bounded wrap, without a remainder operation. Platform
atomics may internally retry; source-level bounds do not establish wait freedom.

### Sparse rights and memory (SI-13)

A nonempty domain leases one prepared GrantSet row. Each class entry stores its
head/cursor and issuer-local node/right counts. Every mask node contains at least
one distinct assigned bit. Thus active nodes are bounded by assigned slots S, and
nonempty domains by min(D,S). A request that has a free domain record and enough
unassigned rights cannot additionally exhaust the prepared row/node pools.

Opening resolves class indices using stack scratch, checks all requested rights,
then domain availability and the control limit, before any mutation. Assignment
selects lowest unassigned bits; whole words do not require per-bit iteration. Nodes
are linked in word order, and each search pass traverses each eligible node once.
Recycling releases rights and nodes only after full charge/control drain. Remote
return does not inspect these structures.

Layout validation includes S nodes, min(D,S)*C entries, both free stacks, FIFO
links, domain ledgers, words, slot metadata, diagnostic storage and padding. Each
word and domain ledger is aligned to 64 bytes; this is an accounted layout choice,
not a hardware-independent cache-line guarantee. Unreserved roots have no grant
rows/nodes. Cheap metrics do not synthesize removed allocation/root counters.
Caller-sized domain/bitmap pages never invoke maintenance. Full snapshots are cold,
allocating observations, exact only with no operations or unfinished transitions.

## Workloads and reproduction

Build once before measurements:

```sh
cargo build --release --features mosaic-spike,bytes-interop --examples --locked --config profile.release.debug=1 --config 'profile.release.strip="none"'
python3 scripts/mosaic-performance.py --suite=alloc --output=/tmp/abps-mosaic-alloc
python3 scripts/mosaic-performance.py --suite=smoke --output=/tmp/abps-mosaic-smoke
python3 scripts/mosaic-performance.py --suite=compare --repeats=7 --output=/tmp/abps-mosaic-compare
python3 scripts/mosaic-performance.py --suite=classes --repeats=7 --output=/tmp/abps-mosaic-classes
# The 64 x 20 TCP case needs more than the common 1024-descriptor soft limit.
ulimit -n 8192
python3 scripts/mosaic-performance.py --suite=tcp --repeats=7 --output=/tmp/abps-mosaic-tcp
```

Choose CPU lists explicitly on a dedicated host for performance acceptance. These
scripts preserve commands, binary/source hashes, repeats, raw results, losses and
RSS samples. Allocation and trace processes are separate from native timing.
The returns case times synchronized final-Drop bursts only, excluding acquisition,
channel transfer and barriers. Do not present it as pipeline throughput.

Use `mosaic_probe --case=layout --backend=mosaic --classes=64 --records=65536
--capacity-policy=reserved` for cold layout accounting, and `mosaic_probe
--words=161 --capacity-policy=reserved` for the fully depleted path. Both binaries
are in `target/release/examples/`. The latter allocates small 64-byte slots, not
the full frame envelope. Repeat with W=1, 2 and 256 and both capacity policies.

`--distribution=mixed-frames` generates synthetic sizes independently of class
configuration, with seed 1 by default; `boundaries` targets 64 KiB, 128 KiB and 2 MiB.
Legacy packet/frame sequences retain seed 42 and their old sizes. The generator
reports a sequence hash, requested bytes, rounded bytes, unserviceable frames and
per-class offered counts, including arrivals subsequently missed or rejected.

`--class-capacities=65536,131072,2097152 --class-slots=64,64,60` is the fixed-backing
counterfactual to the original 64/64 frame classes. It trades four large slots for
64 medium slots; metadata changes are reported separately. `--class-profile=geometric`
selects powers of two (256..2048 for packets; 4096..2097152 for frames) and requires
matching explicit slot counts. For a request above the minimum class, rounding is
less than 2x; the minimum-class floor is separate. No profile silently spills or copies.

The full 64-stream / 20-subscriber / queue-8 / batch-8 layout needs 321 slots per
class per stream: 10272 per root, 161 words on 64-bit. Historical frame classes
reserve 44,430,262,272 backing bytes (41.38 GiB) over two roots, plus metadata.
The layout regression validates this envelope without allocating its payload. The TCP suite executes the same
queue/ownership envelope with packet classes; that is not evidence of running the
41 GiB frame configuration. The strict envelope remains a feasibility check, not
an admission, deadline or arbitrary-pipeline progress guarantee.

## Validation and measurement record

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

The 2026-10-08 evidence (`evidence/segmented-issuer-2026-10-08.json`) records source and
binary hashes, commands, individual rates and latency summaries, all TCP loss
categories, layouts, allocation counts and trace summaries. Raw files remain under
`/tmp/abps-segmented-4y8i_oxf`. The original measurement checkout had no Git metadata;
an initial file copy and hashes establish that baseline. Original `src/reference/`,
`src/berry/` and Cargo.lock are unchanged.

Local checks passed: fmt, native and all-feature Clippy with warnings denied,
35 default test executions, 148 with Mosaic/bytes features, and 14 doctests
(13 compile-fail). Example modules repeat some tests, so these totals are not
counts of independent scenarios. The same independent four-ledger oracle exercises
all three backends; a larger cross-backend regression covers multiword grants and
20 record-reuse generations.

The Loom run passed 40 library tests, including 13 new production-protocol models,
with preemption bound 2 and branch limit 10000. Removing the Release refund or the
final-reader Acquire fence in separate copied trees made the corresponding
drain/reuse regression fail. Miri passed 15 Mosaic library tests and five
reservation-model scenarios for each seed 1, 17 and 42. Intentional forget/abandon
tests require `-Zmiri-ignore-leaks`; other checks remain active. Allocation
instrumentation found zero allocations, reallocations and frees in the measured
prepared Mosaic paths, including errors, borrowed views, batch construction,
grant maintenance and the first remote return. Its deliberate allocation control
reported one allocation and one free.

### Measured effects and remaining costs

Measurements used rustc 1.96.0 on a shared x86_64 Linux VM exposing Ryzen 9 5900X
CPUs, with explicit affinities. No compilation ran during timing. CPU affinity
does not provide dedicated hardware or eliminate SMT/scheduler interference.
The comparison has seven interleaved repeats per case (238 processes in total).

| Final-Drop burst scenario | Berry, median M drops/s | Mosaic, median M drops/s |
| --- | ---: | ---: |
| One return thread | 84.91 | 121.30 |
| Four threads, separate roots | 296.22 | 389.55 |
| Four threads, one root, separate domains | 64.62 | 365.83 |
| Four threads, one root and domain | 30.89 | 60.07 |

Each thread returns 512 distinct allocations per round; threads use disjoint
classes. These results include layout changes as well as removed counters and do
not isolate a single atomic instruction's cost. In the separate-domain case the
median per-run p99 of the slowest worker's batch fell from 51.30 to 19.66 us.
There are only 300 measured rounds per repeat, so p99.99 is not reported.
The common domain's admitted word remains a shared write point; bitmap sharing
and owned-buffer refcount contention are not removed.

Clocked single-thread cycles changed from 10.32 to 10.69 M/s, while a 20-reference
batch changed from 5.82 to 5.41 M batches/s (about 7% lower). Its median per-run p99
increased from 171 to 191 ns. Owned clone and slice are nearly unchanged, and the
borrowed-view microcase is about 3% lower in the new backend. Borrowing removes
reference traffic by construction; this does not imply a measured speedup in an
uncontended loop. The shared adapter's K=1 fast path was also compared against a
binary built from the pre-change copy: 16.97 to 25.52 M batches/s, seven interleaved
repeats without per-operation clocks. That result belongs to the adapter change,
not the Mosaic protocol.

| 64 classes, 4096 slots, 65536 domain records | Berry metadata | Mosaic metadata |
| --- | ---: | ---: |
| Unreserved | 5.25 MiB | 8.76 MiB |
| Reserved | 37.25 MiB | 16.88 MiB |

Sparse grants reduce Reserved metadata in this layout; aligning domain ledgers
increases Unreserved metadata. `min(D,S)*C` rows can still be large for other
layouts. The exact depleted path is also deliberately linear in eligible words:
with W=1/2/161/256, Reserved p50 was 40/40/561/901 ns (10000 attempts per point).
The W=161 run included a 1.58 ms maximum; none of these results is a wall-clock
bound or a reliable p99.99 estimate.

The equal-backing class counterfactual replayed the same 400 synthetic frames
(sequence `e05eb4196cc54fe4`) with 132 MiB prepared backing across two roots,
identical domain byte limits and overload policy. Adding the 128 KiB class changed
offered rounded bytes from 505675776 to 81002496 for 51688438 requested bytes:
9.78x to 1.57x rounding. These are cumulative offered byte totals, not peak live
memory. Both ABPS configurations and their budgeted Bytes baselines delivered
all 1600 subscriber messages. The Bytes reservation is a capacity model rather
than physically prepared backing.

The TCP series used one two-second functional point per configuration. In the
64-stream/20-subscriber packet envelope, Mosaic delivered 79260/80000 messages
(99.075%), Bytes 78500/80000 (98.125%); the differences were 37 versus 75 missed
source arrivals before sending. Neither server discarded received frames; neither
reported late deliveries. This does not establish a TCP throughput advantage.
Mosaic server peak sampled RSS was 58908 KiB versus 15640 KiB for Bytes, whose
storage is allocated on demand. Mixed frames and the real `read_buf` path drained
successfully. The ten-second resource stress completed 20000 iterations, 17927
remote returns and 1138 domain replacements, with 2073 refusals and final drain;
it is too short to establish long-term RSS behavior.

Separate `perf` sampling (`cpu-clock:u`, 199 Hz) attributed 55.47% of 402 Berry
samples to Lease Drop versus 17.59% of 216 Mosaic samples. Both traces include
issuer preparation and harness work; neither lost samples. They support inspecting
the return path, but do not provide cache-line or hardware-counter attribution.
Whole-process `strace` includes startup mappings, channel/worker futexes and teardown;
it is not a hot-path syscall-freedom test.

Two invalid measurements are explicitly excluded: a 64-stream setup failed with
EMFILE under the 1024-descriptor soft limit; an earlier stress run ignored
`--backend` and executed Berry. The stress dispatch is fixed and the repeated
result records `abps_reference::mosaic::raw::Buf` as its implementation.

Hosted CI is configured but was not run here. Dedicated-host acceptance, the
41.38 GiB frame-envelope execution, sanitizers, extended fuzz/soak and cross-target
execution remain unvalidated. The observed K=20 regression and Unreserved metadata
growth must remain visible in any decision to adopt this experiment.

## Adapter lifecycle and preparation follow-up

SI-16/17 add bounded worker supervision and opt-in `ZeroPayload` preparation. The
return protocol and the results above remain the earlier baseline. See the
[implementation and new evidence](lifecycle-and-preparation.md) for error cleanup,
replacement limits, first-touch costs and the separate 64-drop scaling workload.
