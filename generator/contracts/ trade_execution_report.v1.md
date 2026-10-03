# Trade Execution Report, v1

Topic: market.fix.raw

## Kafka record

- Key: metal_id
- Value: FIX-like message, UTF-8 string
- Headers:
  - `event_type`: `trade_execution_report`
  - `schema_version`: `1`
  - `source`: `synthetic_fix`
  - `content_type`: `text/plain`
  - `idempotency_key`: execution ID (FIX 17)

## Payload

Fields are separated by `|`. The trailing `|` is present.

Required tags:

- `8`: protocol version; value `FIX.4.4`
- `35`: message type; value `8` (Execution Report)
- `34`: message sequence number; positive integer
- `17`: execution ID; non-empty and unique per execution
- `55`: instrument code; e.g. `CA`, `AH`
- `54`: side; `1` (Buy) or `2` (Sell)
- `32`: quantity; positive number
- `31`: price; positive number
- `52`: message creation time in UTC
- `60`: trade time in UTC