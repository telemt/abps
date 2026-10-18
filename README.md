# ABPS

ABPS (Allocator Bounded Per-Stream) is an experimental Rust crate for predictable
resource use in streaming and packet pipelines. It accounts for prepared storage,
class capacity, domain admission and buffer ownership separately. A buffer is an
exclusive or shared right to retain part of that prepared storage.

**Status: three unpublished backends; APIs and production acceptance remain
experimental.** The canonical contract is [CONTRIBUTING.md](CONTRIBUTING.md).
Start with the [concepts](docs/concepts.md) or the [documentation index](docs/README.md).

## Backends

| Backend | Admission and storage | Lifetime and intended use |
| --- | --- | --- |
| [Reference](docs/reference-spike.md), default exports | Safe mutex-based executable model; metadata costs are modeled and wrapper allocations remain. | Independent oracle for accounting, ownership and lifecycle. |
| [Berry](docs/berry-spike.md), `berry-spike` | One issuer per root; one bitmap word per class, at most `usize::BITS` slots per class. | Allocation/root claims make return and teardown explicit; compact fixed-capacity experiment. |
| [Mosaic](docs/mosaic-spike.md), `mosaic-spike` | One issuer per root; up to 256 bitmap words per class and prepared sparse grants. | Positive domain charge protects each allocation through final refund; segmented-capacity experiment. |

Berry and Mosaic both support remote buffer returns. Their control handles are
worker-local (`!Send + !Sync`); `BufMut` is transferable and exclusive, and `Buf`
is `Send + Sync`. Segmentation does not introduce multiple issuers. CLI backend
values are `berry` and `mosaic`; see the [canonical names](CONTRIBUTING.md#backend-names).

The optional `local-branches-spike` extends Mosaic with a transferable `Branch`
and `!Send + !Sync` local owning views. One local family holds one global backing
right. Scalar buffers remain the default; see [local branches](docs/local-branches.md).

## Resource contract

- **Reservation:** prepare and account for payload, metadata and layout padding.
  Root limits bound these accounted bytes, not process RSS or page residency.
- **Capacity:** fixed class slots are leased and returned. `CapacityPolicy::Reserved`
  assigns disjoint class grants; idle grants cannot be borrowed by another domain.
- **Admission:** each allocation charges its origin domain for its entire slot,
  including unfinished acquisition and return. Limits alone do not reserve slots.
- **Ownership:** fill exclusively, freeze, create checked views, then release rights.
  A 128-byte view of a 2 MiB slot still retains the full 2 MiB charge.

In Berry and Mosaic each acquire/reference operation makes a bounded attempt.
`Contended` is an expected result of a concurrent update, including a refund far
below the limit. Retry and overload policy belong to the caller. A depleted result
witnesses the attempted search; a later return may make capacity available before
the error reaches the caller. See [errors and progress](docs/concepts.md#errors-and-progress).

A completed final Drop publishes the slot and refunds its origin. Publication and
refund are separate transitions: a newly available slot may coexist with an old
pending charge. Only the owner performs physical arena destruction after drain.
Owner Drop with outstanding access abandons storage until process exit; forgotten
ownership never permits forced reclaim. Diagnostics have their own lifetime.

These experiments provide bounded library work, not hard wall-clock latency or
helping of stopped returners. The broader Shared design requires helping and
remains an unaccepted target. The reference has its own
[explicit allocation and blocking exceptions](CONTRIBUTING.md#reference-backend-scope).

## Using the experiments

This repository is one library package: `abps-reference`, imported as
`abps_reference`, with implementation in `src/`, tests in `tests/` and runnable
examples in `examples/`. Rust 1.96 is the declared minimum. Local consumers can
use a Cargo path dependency and enable `berry-spike` or `mosaic-spike` explicitly.
Create Mosaic configuration through `Config::new`, then assign fields; its
non-exhaustive shape supports Cargo feature unification.

The core is synchronous and runtime-agnostic. Optional `bytes-interop` supplies
bounded `bytes::BufMut` integration; Tokio remains an example/development dependency.
Borrowed descriptors do not copy payload or create ownership. Use
`try_as_io_slice` for an explicit platform-size error; [I/O limits](docs/concepts.md#io-boundaries)
are independent of backing capacity.

```sh
cargo run --locked --example tcp_fanout
cargo run --locked --example tcp_fanout -- --overflow=skip
```

The TCP experiments use bounded queues, disconnect slow subscribers by default,
and support explicit skip, freshness and worker fan-out policies. Applications
must bound queues, tasks, in-flight I/O and overlapping roots separately. Read the
[adapter lifecycle guide](docs/lifecycle-and-preparation.md) and
[performance evidence](docs/performance.md) before interpreting throughput results.

## Safety and verification

The [final-refund correction](docs/final-refund.md) explains the UB fixed in
upstream `668d2f3`: a return frame retained a borrow of padded metadata while the
owner could already free its arena. Terminal operations now borrow only atomic
words. Earlier passing tests did not cover every schedule of that lifetime window.

[SAFETY_CASE.md](SAFETY_CASE.md) maps proof obligations to
[registered checks](verification/matrix.json). The [verification guide](docs/verification.md)
separates native tests, Miri, Loom, sanitizers and mutation controls. Historical
reports retain their source identities and limitations; they do not certify the
current code. This checkout contains runnable verification scripts but no GitHub
Actions workflows. Remote CI and production acceptance are not implied.

Contributions must preserve the [canonical invariants](CONTRIBUTING.md), with local
proof obligations for unsafe changes and reproducible evidence for measurements.
