# Changelog

## [0.1.0] - 2026-09-15

### Added
- implement phlo-observer ingestion, persistence, and query service
- add contract/integration/performance suites, examples, auto-migrate
- runnable deployment + docs for the observer

### Fixed
- harden observer ingestion, auth, and spec compliance for V1
- retention cascade, real OTLP forwarding, stricter auth checks
- stop silent critical-event loss and spool replay stalls
- correlation inheritance, env list parsing, and ingest status accounting
- provision the test database in compose and drop dead rotation branch
- restore OTLP round-trip fidelity and close fail-open gaps
- keep observer image smoke green and pin ReleaseX action

### Contributors
Thanks to our contributors for this release:
-  @iamgp — first contribution!
