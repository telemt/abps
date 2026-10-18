# Performance harness correction and repeat measurements

Base: `92284eac2588119a7b9b06f27e3b0b7bd57c9bd4` plus the working-tree harness
changes described here. No `src/`, allocator layout, synchronization or dependency
changes. This report separates implementation validation from performance acceptance.
The [current harness guide](../../performance-harness.md) contains reproducible
commands, field meanings and resource profiles.

Timed source identity: `1acf29e0f68cdc6cb04ce86b27969628b9de24d12cb6995a94311e535cc89ab2`.
Release `spike_probe` SHA-256:
`4afb344bc389cb149de3a1df90193b840e46b1192a3c83ece04406c4f163ff8d`.
The source identity includes verification scripts; final Python-only resume/test
hardening has separate rerun logs and does not change this executable.

## Confirmed defects and corrections

- Flow checks implemented with Python `assert` disappeared under `-O`. Explicit
  validation now checks counters, failed status, payload errors and teardown.
- TCP consumers previously checked only the first and last payload bytes. Both
  harnesses now validate all bytes. The separate load generator also validates
  planned identities, lengths and timestamps, allowing legitimate skipped IDs.
- The mixed observability returner inherited the issuer's single-CPU affinity.
  It now pins itself separately and reports actual TIDs/masks before and after.
- Startup, admission and drain previously shared one timing denominator. Mixed
  diagnostics now expose these windows separately and retain startup conservation.
- Failed process output and unsuccessful resume records were insufficiently guarded.
  Schema 2 preserves raw output and explicit status; failures cannot become passes
  through a later run. Preparation resume also preserves successful records.
- Writer I/O errors now remain observable by kind. Mid-frame closure is reported
  separately from corruption, preserving the SI-06 deadline contract.

## Executed validation

Rust 1.96.0 on x86_64 Linux; sanitizer/Miri toolchain `nightly-2026-10-03`.

| Check | Actual scope and result |
| --- | --- |
| Native matrix | Seven supported feature configurations; fmt/clippy and 1,350 test executions, including 77 doctests, passed |
| Loom | 41 protocol models × fence/Acquire-load variants, plus two configuration checks; 84 executions passed |
| Miri | Three final-release/teardown scenarios × seeds 1/17 × Stacked/Tree Borrows with strict provenance; 12 executions passed |
| ASan | Configuration plus 11 actual adapter/consumer/queue-credit tests passed |
| TSan | Same 11 tests with the checked Acquire-load variant; no sanitizer warnings |
| Allocation probes | All five registered controls passed, including worker fan-out |
| Python | 49 verification-harness and 12 performance-contract tests passed under ordinary and optimized Python |
| Process controls | Six tests under ordinary and optimized Python: real relay corruption/truncation, actual affinity, plan preflight and immutable resume |

Loom smoke uses small exhaustive models where configured and preemption-bound-2
larger models, with max branches 10,000 and no permutation/duration cap. This is
not exhaustive exploration of the complete unsafe implementation. Miri and
sanitizers were focused runs, not full campaigns. No long soak, hardware-counter
campaign or dedicated-host performance gate was run.

The native/Loom/sanitizer/Miri campaigns preceded final Python/documentation-only
hardening; Rust sources, manifests and `build.rs` remained identical. Python and
process controls were rerun after that hardening. Each campaign preserves its own
source identity; earlier failed compilation attempts are not passing evidence.

## Measurements

Hyper-V VM with five exposed physical cores (ten logical CPUs), selected cores
0/2/4/6/8. Release with debug information, no tracing or concurrent builds/tests
while timing. Seven interleaved repeats: 14 before/after affinity probes, 126 mixed
diagnostic probes, 126 TCP runs and 42 core probes: **308 timed executions**.
These short VM runs establish harness behavior and directions for investigation.

With 200,000 admission iterations, the old affinity configuration produced a
median 2,396 successful acquisitions; the corrected configuration produced 177,822.
This removes an artificial issuer/returner scheduling conflict. It is not an
allocator speedup; the old total denominator also included startup and drain.

At eight classes and a diagnostic query on every mixed admission:

| Bitmap words per class | Domain-page query median p50 | Grant query median p50 |
| --- | --- | --- |
| 1 | 60 ns | 70 ns |
| 16 | 60 ns | 311 ns |
| 256 | 60 ns | 3,979 ns |

Use bounded pages for frequent observation and reduce the frequency of full grant
queries. No extra hot-path atomics were added to improve diagnostics.

TCP uses the new full-payload generator for every variant: two streams, one Mosaic
issuer, a separate writer, four recipients, Reserved, explicit Skip, batch limit
8, 100 ms maximum frame age, one-second planned windows. Scalar saturation at
20,000 offered frames/s:

| Queue | Delivery / sent, median | Delivery / planned, median | Median p99 | Arena reservation |
| --- | --- | --- | --- | --- |
| 8 | 95.15% | 93.25% | 3.15 ms | 286,352 B |
| 32 | 99.26% | 97.29% | 4.03 ms | 706,896 B |
| 64 | 99.92% | 98.60% | 3.65 ms | 1,267,600 B |

Slots and domain budgets increased with each queue profile; this is a capacity
tradeoff, not a free optimization. Batch and worker-branches at queue 64 had median
100% delivery of sent frames, but only 98.12% and 98.23% of planned deliveries.
Neither eliminates source pacing misses or establishes unconditional no-drop.

All 63 normal-load runs at 1,000 frames/s delivered every sent frame to every
recipient; the pacer missed 271 of 63,000 planned frames in total. All 126 TCP runs
had zero invalid payloads, zero late deliveries and confirmed drain. Full-payload
validation changes client CPU cost, so historical constant-payload TCP rates are
not direct before/after allocator comparisons. Scalar remains the default.

## Local artifacts

Raw artifacts are ignored under
`verification/artifacts/performance-hardening-20261018/`: `build-record.json`,
`summary.json`, per-run source/binary hashes, logs and validation reports. The
baseline investigation remains separately under
`verification/artifacts/performance-92284ea-20261018/`.
These are local artifact locations, not public download links. The public tree
retains this report, tests, resource profiles and reproduction scripts.
