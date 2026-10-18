# Fixed-arena delivery profiles

This experiment improves delivered frames per prepared arena by varying queue
descriptors and worker placement. It also offers an explicit freshness policy for
applications that can lose whole frames. The normative contract is
[AP-10](../CONTRIBUTING.md#ap-10-fixed-arena-delivery-profiles-and-local-freshness).
Ordinary allocator and adapter defaults remain unchanged.

## Resource plan

The packet profile has two streams, four subscribers per stream, writer batches
of eight, two classes with 130 slots each, a 97,500-byte domain limit, Reserved
capacity, Skip overload and a 100 ms server-ready deadline. Q8, Q32 and Q64 use
exactly those slots and limits. `capacity-check=report` exposes the conservative
envelope deficits at larger queue depths; it does not guarantee admission. The
existing `require-envelope` profiles remain available for conservative provisioning.

Fan-out queues often retain views of the same backing. More descriptors can absorb
a scheduling burst without requiring one new payload allocation per queued view.
The conservative envelope must still allow independent retention in each queue;
the experiment accepts that availability risk explicitly while every actual
allocation remains subject to unchanged physical and domain admission limits.

Increasing Q grows only external queue metadata. Local routing uses two issuer
workers instead of one issuer and one remote writer worker; each local root serves
one stream. Their aggregate payload capacity is unchanged, but duplicated root
metadata must be reported. Socket buffers remain separately measured; this profile
does not increase them to conceal retention.

## Optional local queue

`--queue-backend=local --return=local` selects prepared worker-local queues for
Mosaic scalar or batch fan-out. The default backend is `tokio`. Local queue rights
are not Send or Sync. Queue depth includes ingress reservations and unfinished
transfers/releases; dequeuing refunds a credit only after a bounded writer batch
owns the frame. Cancellation releases payload before its credit.

`--freshness-priority=oldest --overflow=skip` allows a full local queue to replace
its oldest whole queued frame. `--freshness-min-age-ms=2` requires at least 2 ms of
queue residence; the default zero permits eviction at any age. Freshness defaults
to `off`. Unsupported combinations fail during preflight.

Only queue entries are eligible. A writer-batch frame, including a partially sent
frame, is never replaced. Existing deadline checks still run before each write.
An eviction is irreversible: if replacement admission, payload input or view
construction fails, the old frame stays lost. An old backing can also remain pinned
by another subscriber, so eviction does not guarantee free arena capacity.
No codec dependencies are interpreted; use this policy only where whole-frame loss
is acceptable. In particular, a TCP write completion is not a playback guarantee.

## Measurements and reproduction

Build before timing, with the same flags for every candidate:

```sh
cargo build --locked --release --examples --features local-branches-spike,bytes-interop \
  --config profile.release.debug=1 --config 'profile.release.strip="none"'
python3 scripts/performance-review.py --suite fixed-arena --repeats 7 --seconds 3 \
  --output verification/artifacts/fixed-arena --build-record /path/to/build-record.json
target/release/examples/spike_alloc --backend=mosaic --case=local-queue --iterations=10000
```

The suite compares Q8/Q32/Q64, local Tokio queues, local FIFO queues and local
oldest policies (0 and 2 ms), interleaving seven repetitions at low and saturated
offered load with and without a slow reader. CPUs default to separate physical
cores; inspect affinity and resource plans in the resulting records.

Reports keep planned arrivals, generator/source losses, acquisition failures,
evictions, failed replacements, writer cancellation, transport outcomes and late
deliveries. `uncompleted_enqueued` includes evictions; do not add evictions to that
total again. `failed_replacements` is a subset of evictions, not an additional lost
old frame. Local queue records expose occupancy high-water, age sample count/max,
prepared capacity and descriptor storage model. Queue age is not an atomic snapshot
of end-to-end latency. Local counters are observed on their owning worker.

Choose a candidate on median delivered/planned and timely delivered/planned across
runs, retaining per-subscriber p99, worst-run loss and all failed runs. The explicit
experimental target is healthy-subscriber p99 <= 5 ms. A shared VM can identify
candidates but cannot pass the dedicated-host performance gate. See the
[harness guide](performance-harness.md) for evidence qualification and the
[implementation report](reports/2026-10-18/fixed-arena-delivery.md) for current results.
