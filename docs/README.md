# ABPS documentation

The current guides are synchronized with upstream
[`668d2f3`](https://github.com/telemt/abps/commit/668d2f3f1f20ce726e06c87e2b809b7acca19373).
All backends remain unpublished experiments. English is canonical; identifiers
and invariant IDs retain their exact spelling.

## Reading paths

| Question | Start here |
| --- | --- |
| What resources does ABPS conserve? | [Concepts and transitions](concepts.md) |
| What behavior is required? | [Canonical contract](../CONTRIBUTING.md) |
| Why should the unsafe implementation be valid? | [Safety case](../SAFETY_CASE.md) and [final-refund correction](final-refund.md) |
| Which backend fits an experiment? | [Reference](reference-spike.md), [Berry](berry-spike.md), [Mosaic](mosaic-spike.md) |
| How do local families and queues retain memory? | [Local branches](local-branches.md), [accounting and worker fan-out](accounting-fanout-hardening.md) |
| How do domains, shutdown and replacement work? | [Lifecycle and preparation](lifecycle-and-preparation.md) |
| How are claims tested? | [Verification guide](verification.md), [registry](../verification/matrix.json), [unsafe inventory](../verification/unsafe-sites.json) |

CONTRIBUTING defines semantics. Guides explain those semantics and cite their IDs.
The safety case records local arguments, executable checks and remaining gaps.
Reports record what a particular source, toolchain and workload demonstrated.
Reports use `reports/YYYY-MM-DD/topic.md`, with the date only in the directory name.
Current guides remain directly under `docs/`.

The [integration check](reports/2026-10-11/upstream-integration.md) records the documentation
and selected API validation performed against this upstream revision.

## Architecture and measurement history

These pages combine implementation explanations with dated experiments. Their
current-contract notes take precedence over descriptions of earlier revisions;
measurement sections retain the original identities and results.

- [Reference performance audit](performance.md)
- [Admission, domain reuse and I/O](architecture-performance.md)
- [Hard reservations and TCP freshness](reservations-performance.md)
- [Bounded diagnostics, resource plans and batch references](bounded-diagnostics-and-fanout.md)
- [Berry depletion and Mosaic lifetime review](berry-depletion-review.md)

## Verification history

The following reports describe local sources preceding the final-refund fix.
Passing outcomes remain evidence of those executed scenarios. They do not establish
that the old source was free of UB or that the current source has passed the same
campaign. [The correction](final-refund.md) explains the missing borrow obligation.

- [Initial verification program](reports/2026-10-10/verification.md)
- [TSan synchronization variant](reports/2026-10-10/tsan.md)
- [Rights conservation](reports/2026-10-10/rights-verification.md)
- [Contract and harness verification](reports/2026-10-10/contract-verification.md)
- [Batch rights and slot generations](reports/2026-10-10/generation-verification.md)
- [Grants, drain and sidecars](reports/2026-10-10/grants-lifecycle-verification.md)
- [Remote authority](reports/2026-10-10/remote-authority-verification.md)
- [Branch construction](reports/2026-10-10/branch-construction-verification.md)
- [Diagnostics and failed preparation](reports/2026-10-10/diagnostics-verification.md)

The last report describes local-only diagnostics tests and a mutation script that
were not included in upstream `668d2f3`. Their results and patches remain in its
evidence archive; they are not registered current-checkout tests. Historical CI
references likewise do not mean workflows are present in this checkout.

## Evidence storage

Raw logs, detailed JSON records, traces and compressed evidence archives are kept
locally in `docs/evidence/`. That directory is ignored by Git and is not included
in the public repository. Artifact paths in reports are relative to `docs/` and
identify local files, not public download links.

The repository retains the reports, test/model sources and reproduction scripts.
Reports record the source identity, configuration, results and limitations; access
to the original raw run requires the local archive. If an archive is published
separately, add its public URL and SHA-256 to the report.

Local evidence retains its original names and contents. Source hashes, test counts,
excluded attempts, platform limits and measurement denominators must not be
relabeled as results for a later revision.
