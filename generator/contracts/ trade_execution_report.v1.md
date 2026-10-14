# Trade Execution Report, v1

Topic: market.fix.raw

## Kafka record

- Key: metal_id — must be a known instrument code (see below)
- Value: FIX-like message, UTF-8 string
- Headers:
  - `event_type`: `trade_execution_report`
  - `schema_version`: `1`
  - `source`: `synthetic_fix`
  - `content_type`: `text/plain`
  - `idempotency_key`: execution ID (FIX tag 17), format `EX-NNNNNN`
  - `schema_id`: `2`

## Payload

Fields are separated by `|`. The trailing `|` is present.

Required tags:

| Tag | Field | Rules |
|-----|-------|-------|
| `8` | protocol version | `FIX.4.4` |
| `35` | message type | `8` (Execution Report) |
| `34` | message sequence number | positive integer |
| `17` | execution ID | format `EX-NNNNNN`; unique per execution |
| `55` | instrument code | known metal code |
| `54` | side | `1` (Buy) or `2` (Sell) |
| `32` | quantity | positive integer ≥ 1 |
| `31` | price | positive number > 0.01 |
| `52` | message creation time | UTC, format `YYYYMMDD-HH:MM:SS.mmm` |
| `60` | trade time | UTC, format `YYYYMMDD-HH:MM:SS.mmm` |

## Known instruments

| Code | Metal |
|------|-------|
| `CA` | Copper |
| `AH` | Aluminium |
| `ZS` | Zinc |
| `NI` | Nickel |
| `PB` | Lead |
| `SN` | Tin |

## Validation routing

| Failure | Destination |
|---------|-------------|
| Missing `schema_id` header | DLQ `missing_schema_id` |
| Unknown `schema_id` | DLQ `unknown_schema_id:<id>` |
| `idempotency_key` absent or wrong format | DLQ `missing_header:idempotency_key` / `invalid_header:idempotency_key` |
| `event_type` not in known list | DLQ `invalid_header:event_type:<value>` |
| Key (metal) not in known instruments | DLQ `invalid_header:metal:<value>` |
| Missing required FIX tag | Quarantine + DLQ `invalid_content:missing_field:<tag>` |
| Non-numeric price or quantity | Quarantine + DLQ `invalid_content:wrong_type:<tag>` |
| Side not `1` or `2` | Quarantine + DLQ `invalid_content:invalid_value:54` |
| Price ≤ 0 | Quarantine + DLQ `invalid_content:below_min:31` |

Unknown headers and extra FIX tags are tolerated (forward compatibility).

## Error injection (testing)

The generator corrupts ~5% of messages to exercise validation end-to-end. For this topic, only header-level errors are injected (FIX content is a raw string — content errors are not applicable):

- `missing_idempotency_key` — removes `idempotency_key` header
- `unknown_metal` — sets key to `XX`
- `unknown_event_type` — sets `event_type` to `invalid_event`
- `unknown_header` — adds `unknown_header_xyz` (passes through, tests forward compatibility)
