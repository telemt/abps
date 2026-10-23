# Memory shortcuts: implementation and comparison

This report implements the useful candidates from the local RAM experiments.
Scope is the serial adapter, AP-14 admission work, class lookup and an explicit
final-executable LTO profile. It does not introduce new core unsafe or atomics,
change ordinary defaults, or promote a production performance guarantee.

## Implementation decisions

| Candidate | Implementation | Boundary |
| --- | --- | --- |
| Reuse one mutable allocation | Public `serial-adapter` feature, `src/serial.rs`, initialized stdin/stdout example | Serial borrowed frames; the complete slot stays charged even while idle |
| Reuse a successful admission decision | `memory::Bank::acquire_shared` keeps an unchanged cached decision; full mode refreshes before one check | A refresh invalidates the earlier decision; retain the pending-cache guard and one core attempt |
| Hybrid class lookup | Linear below 64 classes; binary at 64 | Smallest sufficient capacity, identical error and charge semantics |
| Thin LTO | Explicit `perf-lto` Cargo profile, one codegen unit | Defaults unchanged; downstream applications choose their final executable profile |
| Broader memory/queue changes | Deferred | No unsafe ring, thaw, uncharged retained buffer, speculative reclaim or automatic arena growth |

The serial wrapper transfers one existing `mosaic::BufMut` unchanged. `begin`
clears filled length; `SerialWrite::finish` transfers an exclusive borrow to
`SerialFrame`. Destructors clear filled length. No frame has an independent
reference or refund right, and no slice outlives the frame. `into_buffer` returns
the original lease with its then-current lengths. All implementation is safe Rust.
See [the guide](../../serial-reuse.md) for the public contract and example.

This replaces repeated admission only when the application can keep exclusive
serial ownership. It does not accelerate independent asynchronous fan-out by
silently reusing storage retained by another reader. The TCP experiment continues
to use ordinary owning buffers and measures only the admission/build changes.

## Invariants and executable checks

| Contract | Postcondition / detector |
| --- | --- |
| AP-15, OWN-01/02 | A frame or its slices exclude the next writer; compile-fail docs, Send/!Sync/!Clone checks |
| AP-15, MEM-02 | Idle, empty, cancelled and finished frames keep one full charge and the original domain identity |
| AP-15, LIFE-02 | Close cannot revoke the retained lease; remote final destruction releases it through ordinary Mosaic return; owner teardown stays blocked before that |
| AP-15 initialization | Filled length starts at zero at each begin, including after a forgotten guard; initialized length remains lease-scoped and can contain prior frame bytes |
| AP-15 errors/unwind | Range errors preserve state; guard unwind clears filled length before any new begin can conceal a broken destructor |
| AP-14 cache | Only an unchanged successful decision is reused; a count-based refusal refreshes once, and only actual ledger observations lower cached charge |
| AP-14 lookup | Independent minimum-eligible-capacity oracle covers 1/2/8/32/63/64 classes, exact boundaries, zero and `usize::MAX` |
| AP-09 evidence | Source and binary hashes, real teardown, complete losses, positive allocator controls, fixed generator and identical resource plans |

Prepared serial begin/finish/drop add no allocation or atomic operation. Explicit
copies/initialization cost work proportional to touched bytes. Initialization is
not confidentiality erasure: callers must overwrite an initialized prefix before
exposing a new logical frame. New *core acquisitions* still reset both lengths.
A retained buffer per idle connection may reduce availability for other streams;
this retention cost is visible, never refunded on frame completion.

## Validation

Final source identity:
`92b82e9bf60f49221dbf610ea5c8af530eed0fd5a85df66cfc53aaa74e58c752`.
The source-qualified native campaign passed 851 executions, including 56 doctests
and 24 serial tests across three feature combinations. It covers the full target
set with `local-branches-spike,bytes-interop,serial-adapter`, library baselines and
serial-only combinations, Clippy with warnings denied, formatting and compilation
with `loom-model` plus the serial feature. This is not a new Loom exploration.

Miri passed 108 executions: 8 serial tests and 19 AP-14 tests, each with strict
provenance, Stacked/Tree Borrows and seeds 1/17. Serial properties use 8 generated
cases per Miri execution and 128 natively. The initialized stdin/stdout example
also round-tripped 2,097,408 bytes exactly. Existing core synchronization was not
changed; no new sanitizer or formal-proof claim is made.

Three directed controls were killed: removing the independent begin reset, writer
cleanup, or frame cleanup makes the named unwind/forgotten-guard detector fail.
The unmodified detector passed first; altered source and failure logs are retained
in the disposable mutation tree. These controls ran on the initial source
`9d7fcfc83c1fdc738474e8bcf7a5541331ed37f9db602bb21fede0d85e76cfe4`;
the final change only disables proptest filesystem persistence under Miri.

Initial compile failures (domain identity type and mutable owner binding), a
Clippy warning in sample-capacity arithmetic, and the first Miri unsupported
`getcwd` operation remain in the local evidence. The Miri issue was test setup:
filesystem persistence is disabled only under Miri, without disabling isolation
or skipping the property test. Final campaigns ran on the corrected source.
Python harness tests ran normally and with `-O`: 86 discovered each, 80 passed,
6 declared skips. Checks reject missing controls, invalid timings, source/binary
drift, lost charges and unconfirmed teardown.

## Measurement method

All source snapshots, Cargo targets, binaries, raw results and caches live under
`/tmp/abps-serial-20261023/` on tmpfs; swap is disabled. Raw evidence is local and
not copied into the public repository. The public metrics file retains compact
results, source/binary identities and raw-file hashes. This is a shared VM with
CPU affinity, not a dedicated performance host. No compilation, sanitizer or
profiling job runs during the measured campaign.

Baseline is the initial working tree at HEAD `92284ea`, including its pre-existing
local changes, with only the same `memory-cycle` measurement harness overlaid.
Its identity is `1f6d090444e7d840d71b01a9429bf69d911ea94323da674d6c0c060f5a591c5f`.
Candidate release and LTO use the final source above. Separate decision-only and
lookup-only builds isolate the two admission changes. Each source tree has its
own Cargo target. Raw build records include compiler, features, commands and hashes.

The serial sweep uses six paths: fresh/direct parked/actual adapter, each with
copy and initialized-prefix write. Sizes are 64, 1500, 65536 and 1048576 bytes;
live working sets are 1 and 64 slots. Both comparisons retain the same other
W−1 slots. A black-box input and consumer make payload work observable; final
payload equality, retained charge and actual owner destruction are checked.
The fresh path also explicitly normalizes filled length to zero before copying.
Throughput, sampled per-frame clocks and allocator instrumentation are separate.
Clocked tails describe only sampled operations, not whole-run quantiles.

Policy sweeps use the actual Bank, 2/8/32/63/64 classes, 1/8/32 streams, full/cached
accounting and 500,000 cycles per cell. Longer confirmations use 20 million cycles,
8 streams, cached accounting and 2/64 classes. Seven repetitions rotate variant
order; comparisons use paired runs rather than pooling separate campaigns.
These cycles return each allocation immediately: all acquisitions succeed. They
measure the admission cycle, while native tests and the saturated TCP cases cover
refusal/refresh behavior. At 20 million cycles, all five variants have identical
ledger-read/cache-pass totals: 2,499,992 / 19,687,501 at two classes and
78,120 / 19,990,235 at 64 classes. No throughput claim is inferred from these
accounting totals alone.

TCP uses four orthogonal profiles: steady/synchronized arrivals × healthy/slow
consumer, with seven 15-second repetitions and 16,000 offered frames/s. Each has
8 streams × 8 subscribers, local Q=64, adaptive freshness, explicit Skip,
initialized ingress, cached adaptive-shared policy and a 100 ms hard frame age.
The transport uses the packet distribution (512/1500 bytes), not encoded video.
The slow case delays only stream 0 / subscriber 0 by at least 5 ms per receive;
healthy-recipient outcomes remain separately observable.
The same baseline release **load generator binary** serves every server build,
including LTO. This corrects the generator/build confound in the earlier disposable
experiment. One-second preflight and earlier exploratory runs are excluded.

Arena reservation is exactly 70,800 bytes (64,384 payload + 6,416 metadata),
32 slots each at 512/1500 bytes. The selected external storage model is 725,488
bytes for every variant; runtime/socket buffers and allocator rounding are outside
that model. Resource plans must compare equal before TCP timing. Server CPU uses
cumulative process ticks, preserving the last nonzero sample after process exit.
Delivery is offered-load delivery, with generator misses, policy/core refusals,
queue outcomes, partial I/O and late frames retained separately.

## Results

The completed campaigns contain 2,220 primitive/allocation records and 84 TCP
records. The [metrics summary](memory-shortcuts-metrics.json) retains all cells,
paired ranges, loss categories, source identities and hashes of the eight primary
raw result files. Per-process raw logs remain in RAM; they are not extra trials.

### Serial frames

Release medians in ns/frame, working set 64; seven uninstrumented repetitions:

| Payload | Fresh copy | Parked copy | Serial copy | Fresh initialized | Serial initialized |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 B | 45.06 | 11.13 | 13.47 | 47.12 | 14.22 |
| 1500 B | 61.90 | 29.18 | 32.00 | 73.33 | 32.57 |
| 65536 B | 1166.60 | 1154.80 | 1127.10 | 1821.07 | 1167.04 |
| 1048576 B | 61414.42 | 61249.05 | 60176.38 | 97054.46 | 63604.80 |

At 1500 bytes the actual adapter takes 32.00 ns versus 61.90 ns for a fresh
cycle and 29.18 ns for direct parked access. The wrapper costs about 2.8 ns in
this cell. Large copy-only cases are dominated by memory traffic; the result does
not establish a comparable gain there. Initialized reuse avoids repeatedly zeroing
the previously initialized prefix, but does not erase prior frame contents.

Both serial allocation modes measured zero allocations, reallocations and frees
over 100,000 prepared cycles, in release and LTO, with successful positive controls.
Their 64 × 1500-byte backing remains admitted at 96,000 bytes while idle. All
leases then drop and the owner confirms teardown. Clocked serial tails used 15,625
samples per mode/build; they include clock overhead and are separate from throughput.

### Admission and build profile

Long confirmation: 20 million cycles, 8 streams, cached accounting, seven pairs.
Percentages below are medians of per-trial relative differences; they need not
equal ratios of independently computed medians.

| Classes | Variant | Median ns/cycle | Paired change | Faster pairs |
| --- | --- | ---: | ---: | ---: |
| 2 | baseline | 52.97 | +0.00% | 0/7 |
| 2 | decision-only | 51.67 | -3.31% | 6/7 |
| 2 | lookup-only | 53.22 | -1.46% | 5/7 |
| 2 | candidate | 51.55 | -1.86% | 5/7 |
| 2 | lto | 43.82 | -18.61% | 7/7 |
| 64 | baseline | 83.51 | +0.00% | 0/7 |
| 64 | decision-only | 85.38 | -0.72% | 4/7 |
| 64 | lookup-only | 71.98 | -13.80% | 7/7 |
| 64 | candidate | 73.61 | -12.53% | 7/7 |
| 64 | lto | 67.70 | -18.27% | 7/7 |

The combined release candidate improves the 64-class confirmation by 12.53%
on the paired median. The two-class improvement is only 1.86%; isolated removal
of a policy check is not a universal timing improvement. Across the shorter full/
cached matrix the candidate ranges from −10.61% to +2.82% paired median change.
Retain these small regressions/noise cells in comparisons, rather than selecting
only the best case. Binary lookup stays restricted to 64 classes.

The executable sizes are 3,178,512 / 3,170,952 / 2,410,840 bytes for baseline / candidate /
LTO. LTO reduces the baseline executable by 24.15%. This is an explicit
final-build choice, with no default profile change.

### Fixed-arena TCP

| Profile | Baseline timely delivery | Candidate paired delta | LTO paired delta | Candidate CPU change | LTO CPU change |
| --- | ---: | ---: | ---: | ---: | ---: |
| steady-healthy | 97.244% | +0.200 pp | +0.006 pp | -0.61% | -4.41% |
| steady-slow | 95.752% | -0.030 pp | +0.101 pp | +0.62% | -3.13% |
| synchronized-healthy | 99.750% | +0.097 pp | +0.043 pp | -0.83% | -3.05% |
| synchronized-slow | 98.118% | +0.031 pp | -0.060 pp | -3.71% | -2.65% |

Negative CPU change is less process CPU. Delivery denominators include every
planned recipient, including source arrivals that the generator missed. Every
candidate delivery-delta range crosses zero; none of these seven-pair screens
establishes a reliable delivery gain. For example, steady/healthy candidate
differences range from −1.113 to +1.043 percentage points. LTO shows a modest
2.65–4.41% paired CPU reduction across these cells, which still needs dedicated-host
confirmation. Median system-time fractions are roughly 71–81% of server CPU;
this supports investigating syscall/batching costs separately, not attributing
the entire pipeline to admission.

| Profile | p99 baseline / candidate / LTO (ms) | p99.9 baseline / candidate / LTO (ms) | Healthy timely baseline / candidate / LTO |
| --- | --- | --- | --- |
| steady-healthy | 2.871 / 2.904 / 2.777 | 6.910 / 5.906 / 6.078 | 97.244% / 96.976% / 97.195% |
| steady-slow | 3.131 / 2.914 / 3.029 | 6.402 / 5.800 / 6.373 | 97.271% / 97.445% / 97.425% |
| synchronized-healthy | 2.744 / 2.589 / 2.296 | 5.042 / 4.960 / 4.506 | 99.750% / 99.645% / 99.787% |
| synchronized-slow | 2.388 / 2.142 / 2.339 | 4.887 / 4.342 / 4.837 | 99.675% / 99.690% / 99.675% |

The quantile entries are medians of seven run quantiles, not a pooled latency
distribution. Healthy recipients exclude only the explicitly delayed receiver.
The JSON also retains worst healthy-recipient p99, maximum delivery gaps and
priority-rank outcomes. A healthy p99 screen does not establish a hard latency bound.

All 84 runs reconcile retained charges and confirm teardown. Invalid payload
counts are zero. Loss is **not** zero: generator misses, class/policy refusals,
queue eviction/cancellation, writer aborts and late deliveries remain visible.
Partial trailing wire bytes in the delayed-reader cases are retained separately;
they are not counted as successful frames. No unclassified queue outcome or
unobserved completed write is hidden in the reported delivery.

## Reproduction and acceptance

Use separate RAM source snapshots and Cargo targets for before/after builds.
Build `spike_probe` and `spike_alloc` with `local-branches-spike,bytes-interop` for
the baseline; add `serial-adapter` and the serial examples for the candidate.
The LTO candidate uses the same source with `--profile perf-lto`. The metrics
include exact build commands, compiler/features and binary digests. The baseline
includes pre-existing local changes: a checkout of HEAD alone is not that source.
Preserve the source bundle when reproducing; the current RAM bundle is volatile.

The [measurement guide](../../performance-harness.md#memory-shortcuts) documents
the build-record schema and `scripts/memory-shortcuts.py --suite=all` invocation.
The driver requires a new tmpfs output directory, verifies source/binary identity,
alternates trials, rejects failed conservation and keeps the same baseline generator.
Allocation instrumentation, sampled clocks and preflight runs remain separate.

The implemented candidates stay opt-in. Promotion still requires seven matched
60-second runs on a dedicated host, held-out seeds, unchanged arena/queue/overload
policy, complete losses and no material regression in healthy delivery or tails.
Add an application-level serial integration with realistic idle retention before
using its primitive speedup to estimate delivery. New core unsafe is not justified
by these results. Ordinary defaults and the retained-rights lifetime model remain
unchanged.
