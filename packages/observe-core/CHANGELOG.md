# Changelog

## [1.1.0](https://github.com/phlohouse/phlo-observe/compare/observe-core-v1.0.0...observe-core-v1.1.0) (2026-09-15)


### Features

* add observe replay-spool CLI command ([2d8ea06](https://github.com/phlohouse/phlo-observe/commit/2d8ea06326dbf413075899a22868fa35f6a77805))
* implement observe-core event pipeline and drains ([21bad2d](https://github.com/phlohouse/phlo-observe/commit/21bad2d071cfcc618e34c2ac0bee6af7a9ba820a))
* implement phlo-observer ingestion, persistence, and query service ([d1d637f](https://github.com/phlohouse/phlo-observe/commit/d1d637f89e705d6fd6d16f5ac92ffdecbf914a82))


### Bug Fixes

* correlation inheritance, env list parsing, and ingest status accounting ([a6bc156](https://github.com/phlohouse/phlo-observe/commit/a6bc156ce55b13eddb79d7c65da7ca171214c865))
* harden observer ingestion, auth, and spec compliance for V1 ([a0a3d34](https://github.com/phlohouse/phlo-observe/commit/a0a3d34219945645d01655f9db7f048d19302110))
* provision the test database in compose and drop dead rotation branch ([76bf47d](https://github.com/phlohouse/phlo-observe/commit/76bf47d543762dc15af01dae777cc82e34ce9f31))
* restore OTLP round-trip fidelity and close fail-open gaps ([f38432b](https://github.com/phlohouse/phlo-observe/commit/f38432bd7089c58ab39465e269dbc020d3756bc4))
* retention cascade, real OTLP forwarding, stricter auth checks ([a4033f1](https://github.com/phlohouse/phlo-observe/commit/a4033f1e36f8f9b91c2672fcdf9a7bf01ab0e3c4))
* stop silent critical-event loss and spool replay stalls ([b9628cc](https://github.com/phlohouse/phlo-observe/commit/b9628ccec61e98150d8da365e3b9827fa481563a))
