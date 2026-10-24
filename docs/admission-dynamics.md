# Admission dynamics research

[AP-16](../CONTRIBUTING.md#ap-16-experimental-admission-pacing) tests whether
phase-correlated final returns and immediate readmission harm delivery at fixed
capacity. Bounded allocation is not a stability guarantee. This is an opt-in
adapter experiment; ordinary defaults, allocator errors and Drop are unchanged.

The motivating [paper, v3](https://arxiv.org/html/2606.15555v3), studies workloads
whose memory grows during service. ABPS charges an entire slot immediately. Its
model does not establish instability in ABPS. The GPU mixing experiments and the
simulated admission cap are different evidence; neither supplies ABPS rate values.

## Interfaces and ownership

`spike_probe --case=admission-replay|admission-returns|admission-tcp` accepts
`--admission-pacing=off|bucket|tick-cap`. Enabled pacing needs positive
`--admission-rate-bytes` (rounded backing bytes/second) and
`--admission-burst-bytes` at least as large as the maximum class. Tick control is
single-class, with a whole-slot burst. Its period and effective rate are reported;
it permits adjacent-window bursts and has no arbitrary-interval bucket guarantee.

Each root has one issuer. `--return-workers=1..32` chooses remote workers without
creating roots. Native return timing requires one distinct physical CPU for each
worker plus the issuer. TCP also needs separate generator/reader CPUs. Missing
cores make a case unsupported; oversubscription is not equivalent evidence.

The TCP research profile allows scalar/batch fan-out up to 100 and requires
initialized ingress. One pending header per stream, at most 64 streams, waits in
FIFO without payload or queue rights. A single root timer serves prepared records.
At most eight expiry/ready signals are emitted per pass; deadline discovery and
removal examine at most 64 records. A large head request can delay smaller ones.

On eligibility the adapter obtains queue permits and makes one synchronous memory
policy/core attempt. Failed attempts refund provisional rate credit. Committed
credit is not returned by Drop or by a later I/O error. A stalled attempt cannot
accumulate refill before its successful admission and then release a second burst.
Pacing off avoids the pending table and driver timer, but keeps the same deadline,
one-attempt semantics, fan-out and measurement boundaries for comparison.

The absolute deadline starts at validated-header receipt (100 ms by default),
covers pacing, payload and writes, and never restarts. Expiry with unread input
closes the stream. Rejected payloads are drained only within that deadline. Source
latency still starts at the generator's scheduled arrival; backpressure and missed
offered work remain visible. Pacing rejection is not `Contended` or class depletion.

`--slow=yes` in this profile rate-limits only stream 0 / recipient 0, including
wire headers, to `--slow-bytes-per-second=1024` by default. The limiter allows a
64-byte burst, retains fractional credit and caps idle accumulation. It is an
upper-bound rate with runtime scheduling overhead, not a promise of exact bandwidth.
Ordinary TCP slow-reader behavior is unchanged.

## Evidence boundaries

Replay and native returns consume an immutable offered/service trace.
`--release-pattern=synchronized|spread` permutes service requirements within
31-event cohorts: offered bytes and the service-time multiset match. Successful
admissions determine actual release dates; saturation may change observed phase.
This is phase-correlated service, not an externally forced release barrier.
`impulse` correlates the first cohort and then uses a constant service duration.
Neither mode proves self-sustained instability merely by exhibiting bursts.

Replay uses actual allocator APIs with a sequential independent charge oracle;
its clock is logical. Native runs use real remote final Drops and bounded prepared
mailboxes, heaps and event storage. Their per-event snapshots/clocks are invasive:
use them for mechanism exploration, not production throughput or CPU claims.
Admission events and Drop intervals retain allocation identities. Slot publication
can precede Drop completion; no completion timestamp is relabeled as publication.

TCP compares Bytes logical budgets/queues, greedy Mosaic and paced Mosaic at equal
Mosaic reservation. Bytes uses dynamic heap storage; its RSS is measured separately.
Controller, pending tables and prepared fan-out scratch appear in the resource
plan. Histograms, allocator rounding, runtime/task and socket overhead are outside
that storage model and must be distinguished from observed RSS/cgroup usage.
The promotion ceiling is 5% additional modeled adapter storage and zero arena growth.

Full TCP results keep source misses, queue/policy/core losses, expiration, writer
aborts, unread deliveries and consumer payload validation. Header-to-grant and
pacing histograms describe admitted/eligible samples, not expired headers. Counts
of excluded outcomes accompany them. p99.99 requires one million measured samples;
small runs cannot establish that tail.

`received` counts complete consumed or drained payloads. Preflight loss decisions
commit at that boundary. An unread deadline or partial payload remains a header/I/O
failure, with no invented received frame. Completed frames expiring before enqueue
are terminal (`Fatal`) discards and also appear in the expiry observations.
`payload_io_failures` counts post-admission payload input failures; a failure while
draining a rejected payload is identified by the stream's `discard_payload` stop
reason. These observations are not a single exhaustive I/O-reason counter.

## Reproduction

Build `spike_probe` and `spike_alloc` with `local-branches-spike,bytes-interop` in a
tmpfs Cargo target. Preserve a source snapshot and a schema-1 build record accepted
by `scripts/memory-shortcuts.py`: source root/hash, rustc, profile, features and
binary paths/hashes. Run all builds and correctness checks before measured campaigns.

```sh
python3 scripts/admission-dynamics.py --suite=mechanisms \
  --build-record=/tmp/lab/build.json --output=/tmp/lab/mechanisms
python3 scripts/admission-dynamics.py --suite=calibrate --seconds=15 \
  --build-record=/tmp/lab/build.json --output=/tmp/lab/calibration
python3 scripts/admission-dynamics.py --suite=tcp --seconds=60 --repeats=7 \
  --rate=1000 --pacing-rate=1500000 --burst=6000 \
  --build-record=/tmp/lab/build.json --output=/tmp/lab/confirmation
```

Use calibration-only data to choose a rate; check 0.9/1.0/1.1 times sustainable
baseline backing bytes/s and bursts of 1/4/8 maximum slots. Freeze parameters before
confirmation. This script never builds while timing and requires tmpfs with swap
disabled. Keep raw evidence local and publish compact results/hashes in dated report
directories. Missing PMU events are unsupported, not zero; software operation counts
are not hardware RMW measurements. Shared-host outcomes do not authorize defaults.

Acceptance: at least 20% lower measured tail and shorter resource-error streaks,
no more than 1% steady timely-goodput loss, no increased losses/disconnects, no more
than 5% healthy-control CPU regression, and all correctness/resource checks passing.
Report uncertainty and negative findings. Telemt integration requires a separate
matched, dedicated-host decision.

Implementation, verification and measured limitations are recorded in the
[admission dynamics report](reports/2026-10-23/admission-dynamics.md).
