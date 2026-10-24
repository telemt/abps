# Streaming qualification: contracts, delivery and retained backing

Execution date: 2026-10-24. Scope: AP-17–20 on the current experimental backends.
The implementation and screening campaign are complete. Release qualification and
application integration are not complete; no backend, pacing, memory-policy or
branch default is promoted.

## Outcome

The concrete improvement is terminal queue accounting. Ordinary Tokio queues now
classify each enqueued frame as transferred to a writer or cancelled before that
transfer. Across 36 principal TCP trials, 123 queued cancellations were observed;
unclassified server ownership and invalid payloads were zero, and every root
completed confirmed teardown. Twelve additional fan-out trials also closed their
ownership equations. Complete writes that the client did not observe, expired
receipts and source misses remain separate losses.

The experiments did not establish a compelling delivery advantage over the bounded
Bytes baseline. Serial reuse saved admission calls in the primitive but did not
show a useful median TCP roundtrip improvement. Grants changed where pressure was
reported; this short workload did not establish a durable delivery gain. These
are screening results on a shared VM, not evidence of equivalence or a dedicated
performance qualification.

## Change and invariant audit

The canonical definitions are [AP-17–20](../../../CONTRIBUTING.md#ap-17-contract-qualification).
The [guide](../../streaming-validation.md) documents operation costs, memory
boundaries and reproduction commands.

| Obligation | Mechanism and executed check |
| --- | --- |
| OWN-03 / SI-11: publish before reuse, final refund ends storage access | Existing production-protocol Loom models and raw-storage Miri regressions; core unchanged. |
| SI-10 / BR-01..06: pending batch rights, source retention, sidecar incarnation | Existing guarded materialization, branch construction and final-family regressions under Miri and sanitizers. |
| LIFE-01/02: terminal access and teardown | Paused-return/teardown regressions, native full feature matrix and confirmed TCP drain. |
| AP-19: `enqueued = transferred + cancelled` at quiescence | Prepared queue receipts, six actual adapter tests, strict flow oracle and two killed receipt mutations. |
| AP-19: cancellation completes payload Drop before accounting | Drop observer/unwind test; partial writer cancellation distinguishes queued ownership from writer batch ownership. |
| AP-18: evidence cannot pass on partial execution or source drift | Runner records failed TCP attempts, expected record count and source verification failure; ordinary/optimized Python negative tests. |
| AP-20: evidence handoff does not authorize deployment | This report and backend contracts; no application integration or default change. |

Receipts hold the existing message, without cloning its buffer. An outstanding
permit is not a frame; a received entry stays queue-owned until installed in a
writer batch. Receiver closure is not full quiescence: Tokio may retain a send
through an old permit until remaining senders disappear. Final diagnostics are
therefore collected after the complete worker cohort finishes.

The first receipt test expected immediate reclamation after that late send and
failed. The test was corrected to require complete sender/permit quiescence; the
failed log remains in local evidence. No forced reclamation or weakened ownership
assertion was introduced. An early probe compile error (`class` instead of the
reservation field `capacity`) was corrected before qualification.

The inventory reconciles 87 lexical unsafe sites; that is an inventory result,
not an independent soundness proof. Berry, Mosaic and reference implementation
files are unchanged. The added adapters/probes forbid unsafe code. Remote-authority
compile checks passed their baseline and eight prohibited-access cases.

## Qualified verification

Counts below are **test executions**, including repeated targets/features, not
unique logical scenarios. Native features include default, Berry, Mosaic,
bytes interop, local branches and serial adapter in the ten supported combinations
recorded in local evidence. No `--all-features` claim mixes native storage and Loom.

| Campaign | Passed executions | Qualification |
| --- | ---: | --- |
| Native | 3955 | Supported feature matrix, examples, integrations, doctests; fmt/clippy and inventory checks also passed. |
| Focused Miri | 132 | Seeds 1, 17, 42; strict provenance; Stacked and Tree Borrows. Includes terminal returns, batches, branches, receipts and serial adapter. |
| Serial TCP Miri | 12 | Two transport/payload tests under the same six seed/model combinations. |
| Loom | 84 | 41 model tests per fence/load variant plus two variant-selection tests. |
| ASan | 15 | Selected native transition/receipt tests and instrumentation check. |
| TSan | 15 | Same selected group; actual acquire-load variant; no warnings accepted. |

Each Loom variant executed 80 builder calls: 69 reported exhaustive exploration
within their small model and 11 used preemption bound 2. The branch limit was
10,000; permutations and duration were uncapped. This does not mean exhaustive
coverage of the allocator or raw storage. Instrumented atomic production paths
remain distinct from Miri's storage executions.

Directed mutations were killed only after the unmodified named detector passed:
`queue-lost-cancellation`, `queue-lost-transfer`,
`mosaic-generation-missing-final-acquire`, and
`mosaic-generation-relaxed-final-load-acquire-load`.

The evidence runner's 50 Python tests, flow harness's 18 tests and final streaming
runner's seven tests passed under both ordinary Python and `python -O`. Prepared
allocation probes for Berry, Mosaic, fresh serial cycles and retained serial cycles
reported zero measured allocations/reallocations/deallocations with positive
allocation controls. This excludes setup, Tokio channel/runtime and socket work.

## Build and measurement identity

The qualified source digest is `43895b41d5a4d9b111a04d9a971195993fe7f0bd2ce1b65a794b665dc2029e8e`; base Git HEAD is
`92284eac2588119a7b9b06f27e3b0b7bd57c9bd4`, with existing local work retained. Rust stable was 1.96.0
(`ac68faa20`); Miri/sanitizers used 1.101.0-nightly (`0abfedbc7`, 2026-10-02).
Target: `x86_64-unknown-linux-gnu`. Performance build: release, debug info 1,
strip none, `local-branches-spike,bytes-interop,serial-adapter`.

After measurements, only `scripts/streaming-validation.py` and its tests changed:
an ineffective TCP `warmup` argument was removed, and partial/source-drifted
campaigns gained an explicit failing gate. The final source digest is
`c6fe10978954cb393600efbcc85fece6ef3002aacf9fe5968cb433154f7113c4`; all six Rust binaries are byte-identical to the measured build.
The final Python tests and a 27-case, 32-frame serial driver smoke passed; this
smoke checks orchestration, not performance. The earlier Rust/model results retain their original
digest rather than being relabeled. Full hashes are in the compact metrics.

The VM reports an AMD Ryzen 9 5900X, ten logical CPUs and five core identities.
Pinned roles used issuer/returner CPUs 0/2, readers 4/6 and source 8. These are
VM topology identities, not exclusive host cores. Builds, logs and raw measurements
were on tmpfs with swap disabled. Raw artifacts remain local; the public
[compact metrics](streaming-validation-metrics.json) contain identities, per-trial
losses, summary ranges and measurement scopes, without the large raw traces.

Three interleaved trials used 15 seconds of offered load. No warmup interval was
removed from TCP samples. CPU seconds use the maximum sampled cumulative process
user+system ticks and include setup/drain; they are not only allocator time.
Reported tables are medians of trial metrics, including medians of trial p99.9s,
not merged histogram quantiles. Scheduling and source misses are part of results.

## Bounded TCP results

The principal comparison uses one 1500-byte class, variable 512/1500-byte payloads,
bounded queue 8, writer batch at most 8, Skip overflow, 100 ms frame deadline,
requested send buffer 4096, pacing off and memory policy off. Source sequence
hashes match across each three-backend comparison. Bytes has logical byte/slot
limits and the same bounded queues; its backing/metadata accounting is modeled,
not a physically identical prepared arena.

* Fan-out: one source, 100 recipients, 32 slots, 48,000-byte domain limit,
  1,000 offered source frames/s. One slow reader receives at 1 KiB/s.
* Domains: eight independent sources, one recipient each, 16 shared slots,
  24,000-byte limit per domain, 4,000 offered source frames/s total. Reserved
  mode partitions the same physical slot count. Domain grants isolate sources,
  not the 100 recipients retaining one source's frame.

`capacity-check=report` is intentional: these arenas cannot cover the full queue
plus writer envelope. Queue growth does not silently increase arena capacity.
Healthy delivery uses offered opportunities for healthy recipients as denominator;
the slow recipient is excluded only from that column. All-recipient delivery
continues to include its loss. All p99.9 values below include received late frames.

| Topology | Backend / policy | Timely, all | Timely, healthy | Server CPU s | Delivery p99.9 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| fanout, healthy | bytes, unreserved | 99.588% | 99.588% | 8.33 | 5.718 |
| fanout, healthy | mosaic, unreserved | 99.680% | 99.680% | 8.28 | 5.751 |
| fanout, healthy | mosaic, reserved | 99.541% | 99.541% | 8.22 | 6.001 |
| fanout, slow | bytes, unreserved | 98.650% | 99.647% | 8.16 | 5.263 |
| fanout, slow | mosaic, unreserved | 98.630% | 99.627% | 8.24 | 4.588 |
| fanout, slow | mosaic, reserved | 98.558% | 99.553% | 8.24 | 4.588 |
| domains, healthy | bytes, unreserved | 98.403% | 98.403% | 1.89 | 5.517 |
| domains, healthy | mosaic, unreserved | 98.678% | 98.678% | 1.85 | 5.865 |
| domains, healthy | mosaic, reserved | 98.492% | 98.492% | 1.87 | 4.817 |
| domains, slow | bytes, unreserved | 86.872% | 99.282% | 1.73 | 5.669 |
| domains, slow | mosaic, unreserved | 86.363% | 98.701% | 1.73 | 5.108 |
| domains, slow | mosaic, reserved | 86.600% | 98.971% | 1.76 | 5.460 |

One first-trial domain-pressure example illustrates the trade-off: unreserved
Mosaic reported 24 `ClassDepleted` refusals; reserved Mosaic reported 84
`DomainClassDepleted` refusals. Their final queued cancellations were 8 and 1.
That is a change in pressure distribution, not a standalone throughput win.
The slow consumer received 16 expired frames in those trials after earlier socket
writes; a complete write is not proof of timely client receipt. The compact file
retains source misses, queue refusal, missing recipients, core errors, writer
aborts, queued cancellations, expiry and unobserved complete writes separately.
These stages overlap and must not be blindly added together.

## Cost of queue observability

Three alternating before/after pairs used healthy 100-recipient Mosaic with the
same 51,344-byte arena and the same source sequence. The earlier build's adapter
and core sources match the saved pre-change source; the later feature set also
includes the unrelated serial adapter. This is a whole-build comparison, not an
isolated cost estimate for one atomic instruction.

* External storage model: 253,804 -> 266,604 bytes, **+12,800 / +5.043%**.
* Paired server CPU change: median **+5.225%**, range **-0.945% to +5.333%**.
* Paired timely delivery change: median **+0.206 percentage points**, range
  **-0.323 to +0.393 points**; no delivery improvement is established.

The receipt fix improves evidence quality and cancellation accounting; it is not
advertised as a speed optimization. Its measured model growth exceeds a 5% growth
ceiling by 0.043 points and cannot be reported as meeting that ceiling. It does
not enlarge the arena or change buffer Drop. Prepared counter/Arc storage adds
reference/counter operations, which remain a candidate for future measured
optimization if the diagnostic cost matters.

## Serial retention and fan-out economics

The serial TCP probe runs one frame in flight, validates every payload byte and
echoes it between separate processes. It is a closed-loop roundtrip comparison,
not an offered-load benchmark or a backpressure experiment.

| Payload bytes | Fresh Mosaic p50 us | SerialBuffer p50 us | Retained BytesMut p50 us |
| --- | ---: | ---: | ---: |
| 64 | 24.927 | 25.055 | 24.815 |
| 1500 | 25.263 | 25.551 | 25.183 |
| 65536 | 46.175 | 46.079 | 46.591 |

Fresh Mosaic retained zero admitted bytes while idle; both retained variants kept
one full payload capacity (64, 1500 or 65536 bytes). At 1500 bytes the prepared
serial allocation probe used one admission for 10,000 serial cycles versus 10,001
in its fresh-ring baseline; both allocation-instrumented loops made zero allocator
calls. The primitive removes work, but that saving did not establish a useful
TCP median gain. Do not trade away available backing based on this result alone.

A separate 2-source x 4-recipient comparison used 130/130 class slots, 97,500 bytes
per domain, hard reservations and `require-envelope`, with the same queues and
15-second offered schedule. It is deliberately separate from the tight-arena test.

| Fan-out mode | Timely delivery | Server CPU s | Delivery p99.9 ms |
| --- | ---: | ---: | ---: |
| scalar | 99.720% | 0.92 | 5.439 |
| batch | 99.520% | 0.93 | 5.067 |
| local-branches | 99.713% | 0.90 | 4.944 |
| worker-branches | 99.687% | 0.90 | 4.743 |

Local and worker branches remain experimental. Their small CPU differences on
this host do not establish an economic advantage or justify changing scalar default.

## Retention surface and OS boundaries

The 150 retention runs compare the safe reference, bounded Bytes, Berry,
unreserved Mosaic and reserved Mosaic. Ten one-factor profiles per backend vary
message size, preheld occupancy, requested remote fraction and artificial holds.
Every attempt is counted; successful operations, failures and actual remote
returns are reported separately. The full-occupancy cases rejected every attempt
and drained successfully.

At the 1500-byte/64-slot/75%-preheld anchor, median successful-operation p50 was
100 ns for reference/Bytes, 60 ns for Berry/unreserved Mosaic and 70 ns for reserved
Mosaic. Accepted fractions were respectively 36.45%, 53.48%, 52.42%, 50.83% and
50.03%. Those unequal success counts prohibit presenting ns/attempt as an equivalent
throughput comparison. The probe touches one payload byte, includes clocks/channel
scheduling, and may shorten artificial holds during shutdown. It is not a full-copy
benchmark. With 50 ms holds, a short 10,000-attempt burst accepted only 16 slots
per backend: the run is intentionally too short for steady-state hold economics.

Server sampled RSS medians were about 6.3–6.5 MiB for fan-out and 10.1 MiB for the
eight-domain profile, far above the arena alone. All sampled major-fault counts
were zero; that does not prove a fault-free hot path. Minor faults include setup.
The cgroup parent did not delegate its memory controller, so cgroup memory is
**unsupported**, not zero. No parent control or memory limit was changed.

A separate perf probe observed software events (22.06 ms task-clock, 545 context
switches, 391 page faults). Hardware cycles/instructions/cache-misses reported
`<not supported>` despite perf exiting zero. No PMU, cache-to-cache, atomic-RMW
count or hardware-cycle conclusion is drawn.

## Handoff and remaining gates

AP-20 hands off the existing backend contracts, the bounded TCP adapter, exact
resource plans, reproduction runners and these qualified results. There is no
external application change or duplicate live write path.

Before release or a default change:

1. Independent unsafe review of the exact selected source, including terminal
   atomic-word borrows and local-family sidecar reuse.
2. Dedicated-host seven-pair 60-second measurements and explicit 30-minute slow
   consumer soak (`--profile=qualify`, `--suite=soak`). Neither ran here.
3. Delegated cgroup/PMU support if physical memory or cache claims are required.
4. A sustained workload exhibiting a meaningful resource or delivery advantage;
   this screening alone does not satisfy that value gate.
5. Separate application integration with cancellation/shutdown acceptance criteria.

This campaign observes RSS/faults, final drain and a controlled preheld occupancy
floor. It does **not** reconstruct a coherent TCP occupancy history, measure the
publish-to-refund interval, establish p99.99 grant latency from these small sample
counts, or prove long-term RSS stability. Further tracing must keep these gaps
explicit instead of inferring them from final counters.

Local evidence root: `/tmp/abps-streaming-20261024/` (ephemeral). `build2.json` and
`build-final.json` identify measured/final sources; tool directories retain commands,
selected tests and logs; `screen/`, `overhead-before/`, `overhead-after/`, `fanout/`
and `allocations/` retain raw results. Directed mutations and initial failed
attempts remain alongside successes. Reproduction commands are in the linked guide;
raw paths are local evidence identifiers, not public download links.
