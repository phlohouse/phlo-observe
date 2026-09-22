# Changelog

## [4 packages] - 2026-09-22

### Added
- phlo-observe-core: implement observe-core event pipeline and drains
- phlo-observe-core: implement phlo-observer ingestion, persistence, and query service
- phlo-observe-core: add observe replay-spool CLI command
- phlo-observe-core: V2 phase 1 — contracts, schema registry, backends, propagation
- phlo-observe-core: emit canonical entities from integrations
- phlo-observe-core: V2 hardening: equivalence, stress, failure-injection, cross-replica SSE ([#7](https://github.com/phlohouse/phlo-observe/issues/7))
- phlo-observe-core: add generic human-readable event renderer ([#8](https://github.com/phlohouse/phlo-observe/issues/8))
- phlo-observe-core: per-field suppress predicates on Field ([#9](https://github.com/phlohouse/phlo-observe/issues/9))
- phlo-observe-query: add V2 operations — search, SSE stream, alert webhooks, quarantine admin

### Fixed
- phlo-observe-core: harden observer ingestion, auth, and spec compliance for V1
- phlo-observe-core: retention cascade, real OTLP forwarding, stricter auth checks
- phlo-observe-core: stop silent critical-event loss and spool replay stalls
- phlo-observe-core: correlation inheritance, env list parsing, and ingest status accounting
- phlo-observe-core: provision the test database in compose and drop dead rotation branch
- phlo-observe-core: restore OTLP round-trip fidelity and close fail-open gaps
- phlo-observe-core: keep observer image smoke green and pin ReleaseX action
- phlo-observe-core: redact deeply nested error details ([#11](https://github.com/phlohouse/phlo-observe/issues/11))
- phlo-observe-core: retain spool delivery per destination ([#12](https://github.com/phlohouse/phlo-observe/issues/12))
- phlo-observe-core: wait for all pre-flush deliveries ([#14](https://github.com/phlohouse/phlo-observe/issues/14))

### Contributors
Thanks to our contributors for this release:
- @iamgp (25 commits)

---
Full Changelog: https://github.com/phlohouse/phlo-observe/compare/phlo-observer/v0.2.2...v0.4.0

## [4 packages] - 2026-09-19

### Added
- observe-query: add V2 operations — search, SSE stream, alert webhooks, quarantine admin

### Fixed
- observe-core: redact deeply nested error details ([#11](https://github.com/phlohouse/phlo-observe/issues/11))
- observe-core: retain spool delivery per destination ([#12](https://github.com/phlohouse/phlo-observe/issues/12))
- observe-core: wait for all pre-flush deliveries ([#14](https://github.com/phlohouse/phlo-observe/issues/14))
- phlo-observer: preserve lifecycle decisions and identities during rebuild ([#15](https://github.com/phlohouse/phlo-observe/issues/15))
- phlo-observer: publish local notifications only after commit ([#16](https://github.com/phlohouse/phlo-observe/issues/16))
- phlo-observer: track and repair durable projection gaps ([#17](https://github.com/phlohouse/phlo-observe/issues/17))

### Contributors
Thanks to our contributors for this release:
- @iamgp (7 commits)

---
Full Changelog: https://github.com/phlohouse/phlo-observe/compare/phlo-observer/v0.2.1...v0.4.0

## [3 packages] - 2026-09-18

### Added
- observe-core: per-field suppress predicates on Field ([#9](https://github.com/phlohouse/phlo-observe/issues/9))

### Contributors
Thanks to our contributors for this release:
- @iamgp (1 commit)

---
Full Changelog: https://github.com/phlohouse/phlo-observe/compare/phlo-observer/v0.2.0...v0.3.0

## [3 packages] - 2026-09-18

### Added
- observe-core: V2 phase 1 — contracts, schema registry, backends, propagation
- observe-core: emit canonical entities from integrations
- observe-core: V2 hardening: equivalence, stress, failure-injection, cross-replica SSE ([#7](https://github.com/phlohouse/phlo-observe/issues/7))
- observe-core: add generic human-readable event renderer ([#8](https://github.com/phlohouse/phlo-observe/issues/8))
- phlo-observe: add V2 lifecycle and registry endpoints, operator CLIs, archive/restore
- phlo-observe: emit canonical entities from integrations
- phlo-observe: V2 hardening: equivalence, stress, failure-injection, cross-replica SSE ([#7](https://github.com/phlohouse/phlo-observe/issues/7))
- phlo-observe: add generic human-readable event renderer ([#8](https://github.com/phlohouse/phlo-observe/issues/8))
- phlo-observer: V2 phase 1 — contracts, schema registry, backends, propagation
- phlo-observer: add V2 state engine — entity registry, edges, asset projections, rebuild
- phlo-observer: add V2 insight layer — baselines, deterministic rules, incident grouping
- phlo-observer: add V2 query API endpoints and investigation bundle
- phlo-observer: add V2 operations — search, SSE stream, alert webhooks, quarantine admin
- phlo-observer: add V2 lifecycle and registry endpoints, operator CLIs, archive/restore
- phlo-observer: add WAP branch state projection and retention for V2 tables
- phlo-observer: add alert cooldown and dedup
- phlo-observer: serialize retention across instances; document HA and DR
- phlo-observer: V2 hardening: equivalence, stress, failure-injection, cross-replica SSE ([#7](https://github.com/phlohouse/phlo-observe/issues/7))

### Changed
- phlo-observer: batch projection and run-state updates in the ingest hot path

### Fixed
- phlo-observer: add observe-query to the image; fix help assertion on narrow terminals

### Contributors
Thanks to our contributors for this release:
- @iamgp (22 commits)

---
Full Changelog: https://github.com/phlohouse/phlo-observe/compare/phlo-observer/v0.1.0...v0.2.0

## [3 packages] - 2026-09-15

### Added
- observe-core: implement observe-core event pipeline and drains
- observe-core: implement phlo-observer ingestion, persistence, and query service
- observe-core: add observe replay-spool CLI command
- phlo-observe: add phlo-observe SDK with domain contexts and integrations
- phlo-observe: add contract/integration/performance suites, examples, auto-migrate
- phlo-observer: implement phlo-observer ingestion, persistence, and query service
- phlo-observer: add contract/integration/performance suites, examples, auto-migrate
- phlo-observer: runnable deployment + docs for the observer

### Fixed
- observe-core: harden observer ingestion, auth, and spec compliance for V1
- observe-core: retention cascade, real OTLP forwarding, stricter auth checks
- observe-core: stop silent critical-event loss and spool replay stalls
- observe-core: correlation inheritance, env list parsing, and ingest status accounting
- observe-core: provision the test database in compose and drop dead rotation branch
- observe-core: restore OTLP round-trip fidelity and close fail-open gaps
- observe-core: keep observer image smoke green and pin ReleaseX action
- phlo-observe: harden observer ingestion, auth, and spec compliance for V1
- phlo-observe: stop silent critical-event loss and spool replay stalls
- phlo-observe: restore OTLP round-trip fidelity and close fail-open gaps
- phlo-observer: harden observer ingestion, auth, and spec compliance for V1
- phlo-observer: retention cascade, real OTLP forwarding, stricter auth checks
- phlo-observer: stop silent critical-event loss and spool replay stalls
- phlo-observer: correlation inheritance, env list parsing, and ingest status accounting
- phlo-observer: provision the test database in compose and drop dead rotation branch
- phlo-observer: restore OTLP round-trip fidelity and close fail-open gaps
- phlo-observer: keep observer image smoke green and pin ReleaseX action

### Contributors
Thanks to our contributors for this release:
-  @iamgp — first contribution!
