# Streaming qualification

AP-17 through AP-20 qualify the existing Berry/Mosaic experiments. They do not
stabilize the public API, prove soundness or change defaults. The question is
whether bounded ownership improves overload behavior, and what it costs.

## Contract and proof audit

Use the existing unsafe inventory and [safety case](../SAFETY_CASE.md). The focused
review covers terminal atomic-word borrows (including aligned-record padding),
publish/reuse/refund, guarded admission, branch construction/sidecar reuse and
serial cancellation. Record tests and negative controls by source, feature set,
compiler, tool and fence/load variant. Loom models the production protocol, not
the raw storage layer. Miri exercises raw storage, not all possible executions.
Independent review remains open until an external reviewer signs off on an exact
source identity; internal checks cannot manufacture that evidence.

| Public operation | Work/ownership boundary |
| --- | --- |
| Berry acquire | One bounded attempt, class search and one-word claim; failed attempts roll back their own rights. |
| Mosaic acquire | Class search and at most two bounded bitmap passes; confirming depletion is an observation witness. |
| Clone/batch | Checked reference reservation; no hidden retry; source stays alive through materialization. |
| Final return | Complete slot access before publication; complete stable accounting before terminal lifetime release. |
| Domain maintenance | At most caller-requested record examinations, explicitly invoked by issuer. |
| Snapshot/teardown | Cold traversal, not a coherent concurrent snapshot or a hot-path constant-time guarantee. |
| Serial begin/finish | Constant metadata work; retains one full backing charge even when idle. |

These bounds concern algorithmic work, not scheduler/cache/page-fault latency.

The focused campaign is reproducible with the existing evidence runner:

```sh
python3 scripts/contract-freeze.py --tool=miri --seeds=1,17,42 \
  --output=/tmp/abps-contract-miri --build-root=/tmp/abps-contract-build
```

Select `native`, `loom`, `asan` or `tsan` separately. Native runs the supported
feature matrix; Loom runs both synchronization variants with recorded smoke bounds.
Miri and sanitizers select the named terminal-return, branch, receipt and serial
obligations; they are not a claim that the entire safety matrix ran under every
tool. Each output directory must be new. The caller must place build/output paths
on tmpfs; the build and performance runners enforce this explicitly.

## Matched comparisons

`scripts/streaming-validation.py` freezes a manifest before execution. It reuses the
existing TCP generator, resource preflight and complete-payload/flow oracle.
Screening uses three interleaved 15-second trials; dedicated-host qualification
uses seven 60-second trials. Explicit `--suite=soak` runs slow-consumer profiles
for 30 minutes each. It is not silently included in the short suite. TCP results
include fresh-process startup effects; no warmup interval is removed. The runner
records the expected number of invocations and rejects partial execution or source
drift even when previously completed cases passed.

* Fan-out: one source, 100 recipients, healthy or one slow reader at 1 KiB/s.
* Domain isolation: eight independent sources, one recipient each. Reserved grants
  partition source rights; they do not isolate recipients sharing the same frame.
* Retention surface: safe reference, Bytes, Berry and Mosaic; one-factor sweeps
  around 1500 bytes, 64 slots, 75% preheld occupancy and 50% requested remote drops.
  Sizes 64/65536, occupancy 25/95/100%, remote 0/100%, and holds 1/50 ms vary
  independently. Preheld occupancy is a floor, not a coherent live measurement.
* Serial TCP: fresh Mosaic, SerialBuffer and retained BytesMut echo the same frame
  count at 64/1500/65536 bytes. One frame is in flight. This is explicitly closed
  loop, without fan-out or queue; roundtrip latency is not offered-load latency.

The surface includes channel, clock and return-worker costs. Actual remote returns
are counted; a requested percentage is not silently treated as achieved. Shutdown
may shorten outstanding artificial holds. Reference wrappers/mutex and Bytes'
modeled physical backing are disclosed by the fixture. Do not compare their
physical reservation or RSS as if it were identical to a prepared arena.

All TCP variants use pacing off, memory policy off, equal slot counts, byte limits,
queues, deadlines and offered schedules. Reserved metadata may cost more and is
reported. No tuning, new policy or default promotion follows automatically.

## Terminal queue accounting

Ordinary Tokio queues prepare a shared counter block and attach a receipt to each
sent entry. An unused permit contributes no enqueued frame. A received entry stays
in the queue-accounting stage until installed in a writer batch. A dropped entry
finishes payload destruction before recording cancellation; writer-owned messages
use the existing writer outcome counters.

At cohort quiescence:

```text
enqueued = transferred_to_writer + cancelled_queued
transferred_to_writer = writer_completed + writer_aborted
writer_completed = delivered + unobserved_completed
```

Tokio can retain a send through an old permit after receiver closure until remaining
sender handles disappear. Therefore receiver closure alone is not quiescence.
Remote queue metrics are read after all workers complete, never used to reclaim
storage. Counters are independent relaxed diagnostic observations during execution.

The `queue_accounting=receipts-v1` plan requires complete receipt reports and zero
unclassified server ownership. Legacy reports retain their explicit residual.
Local queue and worker-branch credit accounting remain separate. Client receipt,
frame expiry and complete socket write remain different observations.

Receipt storage is modeled before resources. Prepared Arc counters add no
per-message heap object; their reference operations and diagnostic atomics have a
cost and belong in before/after measurements. This does not assert that Tokio's
channel/runtime makes no allocations. Core buffer Drop is unchanged.

## Execution and evidence

```sh
python3 scripts/build-streaming.py --target=/tmp/abps-streaming-target \
  --record=/tmp/abps-streaming-build.json
python3 scripts/test_streaming_validation.py
python3 -O scripts/test_streaming_validation.py
python3 scripts/streaming-validation.py --build-record=/tmp/abps-streaming-build.json \
  --output=/tmp/abps-streaming-screen --suite=all --profile=screen
# On a dedicated host, after recording its isolation/configuration:
python3 scripts/streaming-validation.py --build-record=/tmp/abps-streaming-build.json \
  --output=/tmp/abps-streaming-qualification --profile=qualify --dedicated-host
```

Both build cache and evidence must be on tmpfs with swap disabled. The host must
have four distinct physical cores for separate issuer, returner, source and reader
roles. Affinity does not make a shared host dedicated. Build records include dirty
source contents, features, compiler and binary hashes; reuse rejects source drift.
Failed, incomplete and unsupported results remain evidence, not passing gates.

Process samples include RSS and minor/major faults. `--cgroup-parent=PATH` optionally
creates a child under an already delegated cgroup v2 parent and observes server
memory.current/peak/stat/events. It never enables parent controllers or changes
limits. Unavailable delegation is reported as unsupported, not zero usage. Cgroup
charges, kernel socket memory, RSS, modeled external metadata and arena allocation
are distinct ledgers. A successful destroy does not promise immediate RSS decay.

Public reports belong in `reports/<execution-date>/`, with undated basenames and
compact metrics. Raw logs/builds stay in ignored local scratch. A negative result
is a completed investigation; it does not satisfy the separate workload-value gate.
AP-20 delivers a contract/evidence handoff for separately planned integration,
without touching an application or duplicating live client writes.

The [2026-10-24 screening report](reports/2026-10-24/streaming-validation.md)
records the executed checks, queue-accounting cost and negative delivery findings.
