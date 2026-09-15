# Changelog

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
