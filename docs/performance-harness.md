# Performance harness contracts

This guide describes the current measurement tools for the reference, Berry and
Mosaic experiments. [AP-09](../CONTRIBUTING.md#ap-09-comparable-evidence) governs
comparison; [SI-08](../CONTRIBUTING.md#si-08-typed-pipeline-resource-plan) governs
resource planning. Measurements do not change allocator semantics or provide a
production performance guarantee.

## Reproduction and provenance

Build before timing; record the command, resolved features, compiler, flags and
binary hashes. `scripts/performance-review.py` runs separate `diagnostics`,
`fanout`, `tcp` and `fixed-arena` suites with seven interleaved trials by default.
The [fixed-arena suite](fixed-arena-delivery.md) preserves physical capacity while
varying descriptors and explicit queue policies; it reports envelope deficits.
Pass
`--build-record=PATH` to retain actual build provenance. Without it, build flags
remain unknown; the current shell's `RUSTFLAGS` is not proof of how a binary was
built. A preserved server has its own hash and optional `--server-build-record`.

```sh
cargo build --release --locked --features local-branches-spike,bytes-interop \
  --config profile.release.debug=1 --config 'profile.release.strip="none"' --examples
python3 scripts/test_performance_support.py
python3 -O scripts/test_performance_support.py
ABPS_SPIKE_PROBE="$PWD/target/release/examples/spike_probe" \
  python3 scripts/test_performance_tcp.py
python3 scripts/performance-review.py --suite=diagnostics \
  --target=target --output=/tmp/abps-diagnostics
python3 scripts/performance-review.py --suite=tcp \
  --target=target --output=/tmp/abps-tcp
```

The common topology reader selects one allowed logical CPU per package/core pair.
It honors the process cpuset. `--smt` explicitly selects a separate logical-CPU
experiment. Insufficient distinct CPUs produce `unvalidated` records, never
oversubscribed comparisons. TCP server, pacer and reader sets must be disjoint.
Explicit CPU lists must also avoid shared physical cores unless SMT is selected.
Profiling runs are separate from native timings. Affinity does not isolate a VM.

Schema 2 records carry status, errors, commands, exit codes, identities and scope.
`passed` confirms that the selected harness checks passed, not production approval.
`failed`, `incomplete` and `unvalidated` records remain in the output directory.
Only matching successful trials can resume. A prior unsuccessful or legacy record
requires a new directory; a later success cannot replace the earlier evidence.
Raw process output survives validation errors and nonzero exits. The runner hashes
`build.rs` as well as manifests, implementation, examples and Python tools.

## Complete payload and terminal observations

The `splitmix64-v1` test pattern covers every byte and is keyed by seed, stream,
frame ID, length and eight-byte block index. It is a deterministic corruption
detector, not authentication. Both TCP harnesses use it; the older reference
harness keeps its own schedule and uses seed/stream zero. The 20-byte wire header
and handshake remain unchanged; the forwarding server treats the payload as opaque.
Do not compare old constant-payload generator costs to this validator's throughput.

The separate load generator validates planned ID range, stream routing, monotonically
increasing IDs, scheduled timestamps, expected lengths and the complete payload.
Gaps caused by missed arrivals or explicit Skip are legal; received-frame counts
are never used as planned sequence IDs. Golden vectors and deliberately corrupted
inputs test the detector independently of successful generation.

Each source reuses scratch storage; consumers prepare scratch and histograms before
the planned measurement window. Filling and validation cost CPU on the client and
remain included in end-to-end measurements. They do not belong to allocator cost.

Consumers report `clean_eof`, `truncated_header`, `truncated_payload`,
`connection_reset`, `read_error`, `read_deadline`, `cancelled_after_delivery`, or an
`invalid_*` reason. They retain completed deliveries and partial-prefix lengths.
Invalid content/identity makes the load generator emit its JSON and exit nonzero.
Truncation is a loss observation: SI-06 can legitimately close a partially written
frame on expiry. A complete socket write likewise does not guarantee timely receipt.
Server reports preserve writer I/O error kinds separately from delivery residuals.

## Conservation and denominators

Python validation uses explicit exceptions and runs unchanged under `python -O`.
It rejects invalid/missing counters, failed server/load-generator status,
unconfirmed teardown, abandoned reservations and broken flow equations.
Writer-only workers may have null storage snapshots; issuer storage requires a
confirmed drain, with shared reference storage checked at root scope.

```text
planned = attempted + missed_arrivals
attempted = source_enqueued + source_queue_full + source_queue_closed
source_enqueued = sent + failed_writes
received = accepted + discarded
received * fanout = enqueued + cancelled_before_forward + skipped
                  + queue_rejected + no_subscriber + ingress_rejected
enqueued = writer_completed + uncompleted_enqueued
writer_completed = delivered + unobserved_completed
delivered = timely_delivered + expired_delivered
          = histogram_samples + histogram_out_of_range
```

The older reference schedule also reports `not_attempted_after_stop` explicitly.
The two delivery residuals do not identify the cause of a missing delivery.
`network_bytes_per_planned_s` and `useful_bytes_per_planned_s` divide by the planned
source window. Actual source completion and load-plus-drain times are separate.
No loss category is removed to improve a reported success rate.

## Diagnostic windows

For `--case=observability --op=mixed --cpus=A,B`, A and B must be distinct.
The returner pins itself before signalling readiness. The report records both TIDs
and effective allowed CPU masks at startup and completion. Without explicit CPUs,
a direct invocation is a functional probe with inherited affinity.

The forced depletion check and startup return batch are outside the admission
loop. `admission.attempted` equals `iterations`; startup attempts have their own
counter. `admission_loop_seconds` includes diagnostics, bounded channel operations
and clocks. `completed_cycle_seconds` also includes drain. Startup returns remain
in conservation totals but never in measured admission throughput.

`--diagnostic-kind=pages|grants` selects the mixed observer. The review matrix uses
1, 16 and 256 bitmap words, sampling disabled or every 1/32 attempts. Pages examine
a caller-bounded domain page; grants can traverse all grant nodes. Neither runs
maintenance nor turns independent observations into a coherent snapshot.

## Explicit TCP envelopes

`spike_probe --case=tcp --role=plan` invokes the same Rust `PipelinePlan` used by
the server, before arena, socket, runtime or supervisor resources exist. Runners
save that output and require the server's plan to match. JSON is a projection;
Python does not replace the canonical planner.

The queue comparison uses two streams, one Mosaic root, classes 512/1500 bytes,
fanout 4, writer batch 8, Reserved policy, explicit Skip and 100 ms frame age:

| Queue per recipient | Envelope per stream | Total slots per class | Domain byte limit |
| --- | --- | --- | --- |
| 8 | 65 | 130 | 97,500 |
| 32 | 161 | 322 | 241,500 |
| 64 | 289 | 578 | 433,500 |

Increasing queue capacity also increases the configured slot and byte envelope.
Berry comparisons use a separate common profile: fanout 1, queue/batch 8, envelope
17 per stream, 34 total slots per class and a 25,500-byte domain limit. Do not
compare this directly to four-recipient Mosaic throughput. Exact-boundary tests
reject a deficit of one slot or one byte before server preparation.

Scalar fan-out remains the default. Compare scalar, batch, borrowed and local
families at equivalent offered work, retaining contention and partially served
frames. Primitive wins do not establish a TCP win. Useful no-drop results always
name the load, capacity, consumer behavior and deadlines; overload remains explicit.
