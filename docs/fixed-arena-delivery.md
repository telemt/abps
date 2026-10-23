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

## Adaptive freshness

`--freshness-priority=adaptive --queue-backend=local --return=local --overflow=skip`
selects [AP-11](../CONTRIBUTING.md#ap-11-adaptive-freshness-and-dependency-recovery)
for Mosaic scalar/batch fan-out. Physical capacity stays Q. A per-subscriber
controller contracts its logical window and next writer batch under pressure,
then slowly restores them after recovery. It uses a 100 ms fast average, 60%/20%
hysteresis, and accumulated bad time that repays twice as fast during recovery.
Each existing writer task samples at 10 ms with missed ticks skipped.

| Option | Default | Meaning |
| --- | --- | --- |
| `freshness-target-ms` | 20 | Soft queued-frame age target |
| `slow-grace-ms` | 2000 | Accumulated high-pressure time before close |
| `write-stall-ms` | 500 | Active write without positive byte progress |
| `frame-dependencies` | `independent` | Independent frames or `keyframe` recovery |
| `keyframe-wait-ms` | 5000 | Recovery timeout, only with keyframe mode |

Durations accept 1..3600000 ms. `freshness-min-age-ms` belongs to oldest, not
adaptive. Existing `max-frame-age-ms` deadlines still close the connection and
may expire before grace or stall timeouts. Idle input does not start a write-stall
clock. A slowly progressing partial frame still contributes its age to pressure.

Pruning removes at most eight whole queued frames per pass. Outstanding permits
can temporarily keep occupancy above the new logical window; physical Q remains
invariant. A partial write is completed or the connection closes. Queue pressure
is an operational observation, not proof of a network/client fault. Socket write
completion does not acknowledge delivery, and bytes already accepted by TCP may
still reach a client after local disconnect.

With `--frame-dependencies=keyframe`, the source supplies a monotonically increasing
per-stream sequence and a recovery-point flag. Every gap invalidates dependent
queued work for the affected recipient; resume only when a new keyframe commits.
The source must ensure that marked frames need no missing predecessor. This does
not parse codecs or support arbitrary dependency graphs/open GOP recovery.

Keyframe mode explicitly selects wire v2: prefix `ABPS\x02\0\0\0` before the
existing eight-byte connection handshake, then 32-byte frame headers. The first
20 bytes retain v1 fields; bytes 20..28 hold the big-endian sequence, byte 28 is
0/1 for delta/keyframe, and the last three bytes must be zero. All peers must use
the same mode; version mismatch fails handshake. Independent mode retains v1.
The benchmark marks every twentieth source frame as a recovery point by default;
`keyframe-interval` changes that explicit generator cadence. `keyframe-burst`
continues to describe payload sizes and does not supply recovery metadata.

Queue reports distinguish window/age/dependency eviction, skipped dependencies,
stale commits and terminal reasons (`sustained_pressure`, `write_stall`,
`frame_deadline`, `keyframe_timeout`, `recovery_generation_exhausted`). A stale
commit never changes a newer generation. Failed replacements count attempts that
removed at least one old frame but failed to commit; they are not additional lost
frames. `max_delivery_gap_ns` includes startup and the remainder of the measurement
window after disconnect. Keep planned-delivery denominators for every subscriber.

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

The adaptive campaign also exercises a 100 ms pause, sustained/ramping slow reads,
and stop/resume, plus mixed frame sizes:

```sh
python3 scripts/performance-review.py --suite adaptive --repeats 7 --seconds 15 \
  --output verification/artifacts/adaptive --build-record /path/to/build-record.json
target/release/examples/spike_alloc --case=adaptive-queue --iterations=10000
```

The allocation probe covers prepared local controller/queue operations with a noop
waker. It does not measure Tokio timers or prove that the complete TCP path avoids
allocations. Adaptive policies are opt-in experimental behavior; the ordinary-path
performance gate and dedicated-host requirement remain unchanged.
The [adaptive implementation report](reports/2026-10-18/adaptive-freshness.md)
records source-qualified checks, comparisons and remaining limits.
