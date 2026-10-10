# ABPS

ABPS is a Rust systems crate for predictable memory use in video streaming,
low-latency packet processing, and frame pipelines with overlapping lifetimes.

**Status: three unpublished experiments.** The default
[`abps-reference`](docs/reference-spike.md) backend is a safe mutex-based oracle for
resource ledgers, dynamic domains, and lifecycle. The optional
[Berry](docs/berry-spike.md) backend (`berry-spike`) prepares payload and
ownership metadata, admits on one worker per root, and accepts buffer returns from
arbitrary threads. It uses a single atomic bitmap per class and no hot-path heap
wrapper. The separate optional
[Mosaic](docs/mosaic-spike.md) backend (`mosaic-spike`) extends each class to at
most 256 bitmap words, removes root-wide return accounting, prepares sparse grants,
and adds borrowed views. All three remain experiments, not production releases
or stabilized APIs. The counter tree and multi-issuer Shared algorithm remain
unaccepted.

**Berry** is the single-issuer implementation with one bitmap word per class,
exposed as `abps_reference::berry` from `src/berry`. **Mosaic** is the segmented
implementation with multiple bitmap words per class, exposed as
`abps_reference::mosaic` from `src/mosaic`. Both retain single-issuer admission and
cross-thread buffer returns. CLI backend values are `berry` and `mosaic`.
The rename has no compatibility aliases; invariant IDs and archived measurements
retain their identities. See the canonical [backend names](CONTRIBUTING.md#backend-names).

The TCP experiment now has deadline-bounded worker coordination and a bounded
supervisor for replacement roots. Mosaic storage also supports opt-in
`PreparationPolicy::ZeroPayload`; Lazy remains the default. See the
[lifecycle, preparation and measured limits](docs/lifecycle-and-preparation.md).

The additional opt-in `local-branches-spike` experiment prepares bounded metadata
for one transferable owner per processing branch and non-atomic local owning views.
`Branch` is Send and not Clone; `LocalBuf` is !Send + !Sync. Only the last local
owner releases the branch's shared backing right. Configuration is disabled by
default; ordinary `Buf` remains available. See the
[branch contract](CONTRIBUTING.md#local-branch-experiment) and
[implementation and measurements](docs/local-branches.md). This targets repeated
local slicing, not the cost of one final Drop per independent consumer.

The accepted [accounting and fan-out hardening contract](CONTRIBUTING.md#accounting-and-fan-out-hardening)
specifies sampled diagnostics, compact local families and an opt-in worker fan-out
adapter with conserved queue credits. The `--fanout-api=worker-branches` adapter
uses one Branch per writer worker, then local families under per-subscriber queue
credits. See [implementation and evidence](docs/accounting-fanout-hardening.md).
Scalar remains the default. The worker adapter lost timely throughput at the
highest measured TCP load; dedicated-host evidence is still required for promotion.

## Five design guarantees

These are the original Shared design requirements. The authorized experiments
have explicit [scope exceptions](CONTRIBUTING.md#reference-backend-scope): the
Berry and Mosaic have bounded algorithmic work without helping stopped returns.

1. **Bounded memory** — the root bounds backing reservations and ABPS-owned
   metadata. Domains bound admission, including pending operations. Views retain
   the charge for their full backing, regardless of logical length.
2. **No hidden allocations** — acquisition reports explicit failure without heap
   fallback, implicit waiting, or retries. Contention is distinct from confirmed
   budget or class exhaustion.
3. **Deterministic reclamation** — when the last buffer drop completes, its slot
   has been returned to its class in the owning pool and its origin charge refunded.
   Interrupted published returns require helping; completed drop cannot merely
   enqueue reclamation. Physical arena destruction is separate owner-side work.
4. **Bounded hot-path work** — acquire, freeze, try-clone, try-slice, and release
   bound ABPS's algorithmic work and synchronization attempts, including helping.
   This does not imply wait-free platform atomics or a hard wall-clock bound.
   Cache coherence, scheduling and page faults remain outside that work bound.
5. **Zero-copy composition** — borrowed views and bounded scatter/gather descriptors
   compose payloads without concatenation or per-view heap allocation.

**ABPS optimizes for predictable resource consumption before it optimizes for
minimum average allocation latency.**

A 128-byte view of a 2 MiB slot retains that slot's full charge. Moving the handle
does not move its origin domain. A domain limit bounds admission; it does not
reserve particular slots or guarantee that a request can currently be served.
The opt-in `CapacityPolicy::Reserved` experiment separately assigns hard class
grants with `try_open_reserved_domain`. Other domains cannot borrow idle grants;
byte admission and complete-domain drain still apply. This isolates class capacity,
not arbitrary pipeline progress. See [SI-05](CONTRIBUTING.md#si-05-hard-class-reservations-reference-and-berry).

## First version and boundaries

v0 targets CPU-resident byte buffers with Shared ownership, fixed storage/classes,
and flat domains leased from a fixed table. The core is synchronous and
runtime-agnostic. The reference vertical integration uses Tokio TCP through bounded
queues across workers, disconnecting slow subscribers by default; skipping whole
new frames is explicit opt-in. Local
mode, hierarchical quotas, owning chains, magazines, and specialized storage are
extensions.

ABPS does not bound all process RSS, external queues, or task counts; guarantee
progress of arbitrary pipelines; or provide an end-to-end zero-copy transport.
Applications still need queue limits, overload policy, and headroom for overlapping
input, output, and in-flight I/O. Owning conversions to third-party buffer types
may allocate even when payload bytes are not copied.

Owned buffers may outlive public pool handles. A separate `StorageOwner` performs
cold teardown after drain. Dropping that owner with outstanding access or unfinished
returns closes admission and retains the arena and its reservation until process
exit. Forgetting a handle never authorizes forced reuse or a fictitious refund.
Abandonment is observable through bounded external diagnostics; aggregate limits
across replacement or abandoned roots remain an application responsibility.

The canonical contract defines the [resource model](CONTRIBUTING.md#resource-model),
[progress model](CONTRIBUTING.md#progress-model),
[lifecycle model](CONTRIBUTING.md#lifecycle-model), and
[interop contract](CONTRIBUTING.md#interop-contract). It separates required behavior
from candidate algorithms and extensions.

## Contributing

The repository root is a single library package, with its public entry point in
`src/lib.rs`, integration tests in `tests/`, and runnable examples in `examples/`.
Local consumers can point a Cargo `path` dependency at the repository root;
the package name is `abps-reference` and the Rust import is `abps_reference`.

Run the unpublished experiment with `cargo run --locked --example tcp_fanout`;
add `-- --overflow=skip` for explicit skipping. See the
[spike report](docs/reference-spike.md) for results, allocation limitations,
API findings, and reproducible checks.

Enable the separate backend with `features = ["berry-spike"]` and import
`abps_reference::berry`. Its worker-local control handles are neither Send
nor Sync; immutable buffers are Send + Sync. Roots have at most 64 classes and
`usize::BITS` slots per class, with no borrowing of capacity from other roots.
See its [protocol and measurement report](docs/berry-spike.md) before use.
Closed domains with retained buffers require explicit bounded `poll_domains(n)`
before their records can be reused; snapshots never sweep them. The optional
`bytes-interop` feature enables safe bounded Tokio `read_buf` integration through
`bytes::BufMut`, without adding Tokio to the core or converting ownership to `Bytes`.
The [architecture follow-up](docs/architecture-performance.md) describes queue
admission, overload handling, bounded write batches, and measured improvements.
The [reservation and freshness follow-up](docs/reservations-performance.md)
documents hard domain grants, metadata layout, explicit TCP deadlines, and their
validation and performance costs.
The [SI-07 through SI-10 follow-up](docs/bounded-diagnostics-and-fanout.md)
adds caller-bounded diagnostic pages, a typed TCP resource plan with optional
strict envelope checks, linear grant resolution, and explicit batch cloning.
Scalar fan-out and report-only capacity checks remain the adapter defaults.

Enable `features = ["mosaic-spike"]` to use
`abps_reference::mosaic`. Its separate Config counts control handles,
not allocations, under `control_handle_limit`. It retains exact domain byte limits
and synchronous final refunds. See the [protocol, bounds and validation record](docs/mosaic-spike.md).

The [performance audit](docs/performance.md) provides separate native timing,
allocation and tracing harnesses, including a budgeted Bytes baseline and
independently scheduled TCP load. The [Berry comparison](docs/berry-spike.md#performance-evidence)
adds a separate load-generator process and worker-affinity controls. Measurements
are experimental evidence, not production performance guarantees or CI thresholds.

- Read [CONTRIBUTING.md](CONTRIBUTING.md) for the specification, safety obligations,
  validation, portability, and performance evidence.
- Agents must also follow [AGENTS.md](AGENTS.md).

## Executable verification

[SAFETY_CASE.md](SAFETY_CASE.md) maps storage/lifetime/synchronization obligations to
[registered checks](verification/matrix.json). The [verification guide](docs/verification.md)
describes common traces, Miri/Loom profiles, coverage, sanitizers, fuzzing, mutations
and release evidence. Configured campaigns and completed runs are reported separately;
the experimental status and public resource contracts remain unchanged.
