# Bounded adapter lifecycle and explicit payload preparation

> **Reading this page.** Use [concepts](concepts.md) and the
> [final-refund correction](final-refund.md) for the current contract. Dated
> measurements and validation sections below retain their original sources
> and results; they are not reruns of upstream `668d2f3`. References there to
> CI describe historical local workflows, absent from this checkout.

Backend names and runnable commands use the current
[Berry/Mosaic naming](../CONTRIBUTING.md#backend-names). Dated evidence retains its
original names, commands and hashes; renaming does not constitute a new measurement.

SI-16/17 implement the next step after the Berry review. The normative
contract remains [CONTRIBUTING](../CONTRIBUTING.md#si-16-bounded-adapter-supervision-and-recovery).
The Berry and safe reference remain independent baselines. The Mosaic
return protocol is unchanged: three RMWs, no supervisor, registry, maintenance,
allocation, or diagnostic callbacks in buffer Drop.

## Lifecycle

The TCP adapter now coordinates workers with cancellation, completion messages and
deadlines. Its old setup/final barriers could wait forever after a peer failed a
handshake or exited early. Accepted sockets and their eight-byte handshakes use
async I/O under one setup deadline. Writer handoff uses bounded `try_send`.
Ingress and writer tasks are observed together so a writer panic cannot hide
behind an idle ingress. Shutdown closes admission, releases runtime/task/queue
ownership, waits for peer quiescence within the drain deadline, and attempts arena
teardown on its issuer. Confirmed workers can be joined; unconfirmed threads are
detached and remain charged. OS scheduling and blocking cold preparation are not
hard real-time operations; a timeout never proves that their memory is free.

A process-lived adapter registry charges the full typed root reservation before
prepare. Its record array and independent byte cap cover Preparing, Active,
Draining, Abandoned and Unconfirmed generations. Only proven arena destruction
and release of adapter diagnostics refund a record. A late old-generation message
cannot refund a reused record. Its snapshots expose abandonment and unconfirmed
bytes; they do not export diagnostic ownership. Reference/Bytes numbers remain
modeled reservations; this registry does not turn their metadata into a physical
allocation bound or bound all process RSS.

| TCP option | Default | Meaning |
| --- | --- | --- |
| `--setup-timeout-ms` | `30000` | Overall accept/handshake/worker-ready deadline. |
| `--drain-timeout-ms` | `2000` | Bounded drain/teardown coordination. |
| `--supervisor-roots` | Number of initial roots | Total prepared record capacity, including retained old roots. |
| `--supervisor-bytes` | Sum of initial planned reservations | Root overlap needs explicit additional headroom. |
| `--replacement-attempts` | `1` | Lifetime automatic replacement allowance per logical worker; `0` disables it. |
| `--preparation` | `lazy` | `zero-payload` is supported by Mosaic only. |

An undrained cohort can be replaced only when both caps and the lifetime attempt
allowance permit it. New endpoints are printed with `generation` and
`new_work_only`. Remote-return workers are replaced as one cohort. Old sessions
are closed; neither the server nor load generator reconnects or replays frames.
Setup errors with successful cleanup terminate the run without automatic replay.
A failed replacement does not start another loop. The final JSON retains prior
generations, errors, unconfirmed workers and rejection reason. On task panic,
partial ingress statistics may be unavailable: client offered/undelivered counts
remain necessary, and such runs are not successful throughput samples.

The issuer's 10 ms maintenance timer calls `poll_domains(32)` only with pending
records and skips missed ticks. No snapshots or opening calls acquire an implicit
scan. The FIFO fairness regression leaves a busy domain at the head while a later
ready domain is recycled.

```sh
cargo run --locked --release --features mosaic-spike,bytes-interop \
  --example spike_probe -- --case=recovery --backend=mosaic
```

This short recovery example intentionally abandons one small root. It admits one
replacement using a second record and explicit byte headroom, processes new work,
and rejects another replacement. Releasing the old buffer does not refund the
abandoned root. Run it as a separate process; intentional abandonment lasts until
process exit, even though memory safety is preserved.

## Preparation

`mosaic::Config::preparation` defaults to `PreparationPolicy::Lazy`.
`ZeroPayload` writes every checked payload byte before activation, using volatile
aligned word writes and byte prefix/tail writes. Layout, reservation, metadata
and allocation counts are unchanged. Fresh leases still have zero filled and
initialized lengths. Acquisition, recycling and Drop do not zero payloads.

This trades startup work for fewer initial faults during use. It is not mlock,
zero-on-release, or a guarantee of page residency/no subsequent faults. The
benchmark reports prepare, first full pass, and warm full pass separately, with
Linux process minor/major faults and RSS. Its /proc observer overhead is included
identically in both policies; fault counts are not an inferred page count.

## Measurements on 2026-10-08

The raw artifacts referenced here are [kept locally](README.md#evidence-storage)
and are not included in Git.

Evidence (`evidence/lifecycle-and-preparation-2026-10-08.json`) records source/binary
identities, per-run results, failures, trace availability and validation. Full raw round
times and logs are retained in the compressed archive
(`evidence/lifecycle-and-preparation-2026-10-08.raw.json.gz`) and in
`/tmp/abps-lifecycle-2bqum5oy`. This is a shared 10-vCPU VM, not a dedicated performance
host. No performance gate or production throughput guarantee is inferred.

Return scaling uses seven interleaved repeats at 1/2/4/8 returners; 16/32 are
explicitly **unvalidated** because there are insufficient distinct CPUs. The new
`--return-layout=scaling` has one class and 64 allocations per returner; it is
separate from the historical 512-drop batches. Throughput runs have no per-drop
clocks; individual latency runs are separate and suppress the throughput metric.
Acquisition, barriers and channels are outside the measured Drop burst.

| Returners / topology | Berry, million final drops/s | Mosaic, million final drops/s |
| --- | ---: | ---: |
| 1 / distinct domains, one root | 88.94 | 137.31 |
| 2 / distinct domains, one root | 113.72 | 147.54 |
| 4 / distinct domains, one root | 115.59 | 186.94 |
| 8 / distinct domains, one root | 151.44 | 311.55 |
| 8 / one shared domain | 77.76 | 182.98 |
| 8 / separate roots | 351.61 | 453.01 |

These compare the already existing Berry/Mosaic protocols, not a return algorithm
changed by this patch. At eight returners, median per-run instrumented p99 Drop
latency is 151/90 ns for distinct domains and 260/111 ns for one shared domain
(Berry/Mosaic). Every offered burst completed with zero acquisition failures;
insufficient samples keep p99.99 unset. Contention on the shared domain remains
visible. Sharing one bitmap word among eight returners yields 110.42/142.53
million drops/s, with only 64 total slots; it is a separate topology, not directly
comparable to 512 total slots in the other eight-thread rows.

Shared refcounts remain another independent limit. With four threads repeatedly
creating and dropping views of one backing, median accepted rates are 23.98/23.45
million/s (Berry/Mosaic), with 39.19%/41.43% `Contended` failures. Distinct backings
reach 197.77/195.99 million accepted operations/s with zero failures. These seven
interleaved comparisons retain every rejected attempt. Supervisor and preparation
changes do not eliminate competition on a shared refcount.

Exact depleted-path medians for W=1/2/161/256 are 30/40/511/802 ns unreserved and
40/40/571/901 ns reserved (one 10,000-call diagnostic run each). Confirmation work
remains bounded by the existing SI-12 contract; there is no speculative negative
load fast path and no larger-class fallback.

For 64 MiB backing, medians of seven interleaved preparation runs:

| Phase | Lazy | ZeroPayload |
| --- | ---: | ---: |
| Prepare | 7.9 microseconds; 4 minor faults | 7.34 ms; 547 minor faults |
| First full pass | 6.73 ms; 544 minor faults | 3.52 ms; 1 minor fault |
| Warm full pass | 3.79 ms; 1 minor fault | 3.21 ms; 1 minor fault |

No major faults occurred. Prepared RSS moves from roughly 2 MiB to 66 MiB with
ZeroPayload; both policies reach roughly 66 MiB after use. The allocation harness
reports identical preparation counts and zero allocator calls/frees/reallocations
on measured hot paths and first remote return; its intentional-allocation positive
control fires for both policies.

Seven interleaved TCP before/after pairs use the same Mosaic backend, four
streams, two issuer workers, remote return, fan-out 4, reserved capacity, 100 ms
freshness, Skip policy and 1,000 offered frames/s for two seconds. Median delivery
fractions are 99.6% before and 99.7% after. No server admission discards, queued
undelivered messages or late deliveries occurred; missing arrivals were recorded
by the external load generator. These short runs validate conservation and
shutdown, not a TCP performance improvement. Five additional TCP smoke points
cover Bytes, mixed frames and `read_buf`; every root confirms destruction and its
supervisor charge returns to zero. A separate shared-reference TCP run also drains.
The standalone bad-handshake reproduction leaves the old process stuck beyond a
two-second observation window; the new process exits with a structured error,
confirmed destruction for both roots and zero supervisor charge.

A separate software trace has 98 samples, zero lost samples and 32.65% self samples
in Mosaic Lease Drop. It includes issuer/harness work and is too small for a
cache-line attribution. `perf stat` reports cycles/instructions/cache-misses as
`<not supported>` despite exit code zero. `perf c2c` fails because no PMU supports
memory events. Hardware attribution, 16/32-thread scaling, dedicated-host
acceptance, sanitizer/cross-target runs and long soak remain unvalidated.

## Reproduction and regression checks

```sh
cargo test --locked --all-targets --features mosaic-spike,bytes-interop
cargo clippy --locked --all-targets --all-features -- -D warnings
CARGO_TARGET_DIR=/tmp/abps-loom-clean cargo test --locked \
  --features mosaic-spike,loom-model --lib protocol::tests::
MIRIFLAGS='-Zmiri-ignore-leaks -Zmiri-seed=1' cargo +nightly-2026-10-03 miri test \
  --locked --features mosaic-spike,bytes-interop --lib mosaic::
python3 scripts/check-mosaic-mutations.py --output /tmp/abps-mutations-new
cargo build --locked --release --features mosaic-spike,bytes-interop \
  --examples --config profile.release.debug=1 --config 'profile.release.strip="none"'
python3 scripts/mosaic-performance.py --suite=scaling --output=/tmp/abps-scaling-new
python3 scripts/mosaic-performance.py --suite=preparation --output=/tmp/abps-preparation-new
```

Native tests cover wrong second-worker handshake, prepare error, ingress/writer
panic, silent handshake deadline, normal remote shutdown, stale completion,
unconfirmed roots, independent caps and replacement exhaustion. Miri seeds
1/17/42 pass all 17 Mosaic tests including byte/word alignment guards and fresh
lease initialization. The minimal Loom build passes 24 production protocol tests
across both issuers. Negative controls remove the acquire fence, corrupt prepared
words and allow stale registry refunds: each named regression fails as expected.
Mutation sources **and Cargo targets** are disposable and separate; the final
normal build is never reused from mutation experiments. CI configuration is
updated; hosted CI was not executed in this session.
