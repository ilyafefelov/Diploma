# Version 1.2 OREE/V13 Source-Gate Audit

Date: 2026-07-13

## Outcome

The public acquisition route was exhausted but did not close the V13 receipt
gate. This is an externally blocked source-evidence result, not a modeling
failure and not permission to weaken the contract.

- OREE DAM returned all 24 hourly rows for delivery date 2026-07-13.
- The first successful observation was 2026-07-12 23:27:17 UTC.
- The rows did not contain a source publication timestamp.
- Monthly public probes for June and July 2026 exposed neither row-level
  publication metadata nor an HTTP `Last-Modified` header.
- HTTP `Date` was retained as retrieval metadata only.
- Consequently `can_satisfy_v13_explicit_receipts=false`,
  `promotable_v13_permitted_training_rows=0`, and
  `market_execution_enabled=false`.

Machine-readable evidence is stored in
`runs/v1_2_oree_source_gate/public_probe_summary.json`.

## Official interface evidence

The OREE public pages expose DAM/IDM result and participation surfaces. The
official XMtrade/PXS external-interface specification describes authenticated,
WS-Security-signed requests and responses. Its daily-result flow uses request
message 961 and response/data messages 962/963. The message header contains a
UTC `date-time` defined as the time of message sending, and WS-Security includes
a signed `Created` timestamp.

That specification identifies a defensible next acquisition route: obtain an
authenticated source-signed 963 response/export and validate its row-level
evidence against the existing V13 contract. It does not justify treating a
later query-response timestamp as the historical publication time of rows that
were already public. Credentials or a valid signed export are still required.

## Non-negotiable boundary

Retrieval time, HTTP `Date`, a market deadline, and first-seen observation time
must not be renamed `source_publication_timestamp`. Research-shadow DFL/DT work
may continue with explicit non-promotion labels, but V13 remains open until the
external signed-source evidence is obtained and passes validation.
