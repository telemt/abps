# Upstream documentation integration — 2026-10-11

This documentation branch is based on upstream
[`668d2f3f1f20ce726e06c87e2b809b7acca19373`](https://github.com/telemt/abps/commit/668d2f3f1f20ce726e06c87e2b809b7acca19373)
(`UB on final Refund fixed`), still GitHub HEAD at the end of the check. Its parent
is `f53f542b09ce3f173cc711372d8029dec69ddc16`. Implementation, tests, manifests,
verification scripts and registry remain byte-identical to that upstream revision.
The original local checkout remains unchanged.

## Contract reconciliation

The [concepts guide](../../concepts.md) separates reservation, capacity, admission and
ownership. Berry means the one-word single-issuer backend; Mosaic means segmented
single-issuer storage, not multiple issuers. CONTRIBUTING remains normative.

The [final-refund guide](../../final-refund.md) explains the terminal-borrow UB, the
atomic-field correction and its storage/ordering proof boundaries. It also covers
opaque root identities with an unassigned zero sentinel, view-length descriptor
limits, non-exhaustive Mosaic Config and common sequential opening error order.
Guides retain bounded contention and winning-word cursor semantics.

Historical reports keep their measurements, source hashes and limitations. All
30 pre-existing evidence files were compared byte-for-byte with the original local
archive. Historical passing campaigns are not validation of the corrected source.
The local diagnostics/preparation additions and GitHub Actions workflows absent
from upstream are explicitly identified; none was imported as implementation.

## Executed checks

The raw artifacts referenced here are [kept locally](../../README.md#evidence-storage)
and are not included in Git.

The machine-readable record (`evidence/upstream-integration-2026-10-11.json`) and
compressed logs and check drivers
(`evidence/upstream-integration-2026-10-11.raw.json.gz`) retain commands, configuration,
tool versions, outcomes and content hashes. The host was Linux
`x86_64-unknown-linux-gnu`, Rust 1.96.0. Cargo used a fresh target directory belonging
only to this checkout, with four build jobs.

| Check | Result |
| --- | --- |
| Root identity, descriptor bound, opening error precedence and Berry/Mosaic/branch I/O regressions | 8 native test executions passed with `local-branches-spike,bytes-interop`. |
| Crate doctests, including compile-fail API contracts | 77 executions passed across seven feature configurations. |
| `cargo doc --locked --no-deps` with `RUSTDOCFLAGS="-D warnings"` | Passed in all seven configurations. |
| Rust examples in the Mosaic and local-branch Markdown guides | Both compiled and executed through rustdoc against the full native feature set. |
| Verification harness | 49 tests passed normally and 49 with Python `-O`. |
| Registry and unsafe inventory | 112 registry rows validated and 87 lexical unsafe sites matched, in both Python modes. This did not execute every registered selector. |
| Documentation integrity | Relative links/anchors, fences, invariant IDs and whitespace passed. |
| Preservation | 159 original-checkout files and 30 historical evidence files matched their baseline hashes; upstream code/configuration remained unchanged. |

The feature configurations were default, `berry-spike`, `bytes-interop`,
`mosaic-spike`, `mosaic-spike,bytes-interop`, `local-branches-spike`, and
`local-branches-spike,bytes-interop`. Repeated feature executions are not distinct
logical tests. The two Markdown tests invoke their examples, not just unused
function declarations.

The runner's source digest before and after these checks was
`e9f7fb4fe31e7ba6fdf7836dd3c84a94562d1968a68ee383d14119782428e06a`.
That digest includes canonical contract/safety files but excludes README and
`docs/`; the record therefore also retains a separate Markdown content manifest.

## Evidence limits

This was a documentation and selected API regression check. It did not rerun the
full native registry, Miri, Loom, sanitizers, mutation campaigns or benchmarks.
Windows Miri observations in the safety case remain attributed upstream reports,
not new executions here. Registry validation and lexical inventory agreement are
not proofs of soundness. Remote CI and production acceptance remain unclaimed.
