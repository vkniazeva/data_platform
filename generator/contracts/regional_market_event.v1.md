# Regional Market Event v1

data type: semistructured

## Kafka record

topic: market.events
key: `{metal_id}_{region_code}` — both parts must be known values
value: JSON, UTF-8 encoded

### Kafka Headers

| Header            | Required | Example                     | Description                               |
|-------------------|----------|-----------------------------|-------------------------------------------|
| `event_type`      | yes      | `regional_price_quote`      | Event type; must equal `value.event_type` |
| `schema_version`  | yes      | `1`                         | Must equal `value.schema_version`         |
| `source`          | yes      | `synthetic_regional_market` | Event producer                            |
| `content_type`    | yes      | `application/json`          | Kafka value format                        |
| `idempotency_key` | yes      | `EX-ME-0001`                | Format `EX-{region}-NNNN`                 |
| `schema_id`       | yes      | `1`                         | Schema Registry ID                        |

### Event value

```json
{
  "event_type": "regional_price_quote",
  "schema_version": 1,
  "execution_id": "EX-EU-0001",
  "instrument": "CA",
  "side": "sell",
  "quantity": 25,
  "price": 10120.5,
  "currency": "USD",
  "event_timestamp": "20261003-14:30:05.123",
  "conditions": {
    "region": "Europe",
    "warehouse": {
      "warehouse_id": "WH_RTM",
      "city": "Rotterdam"
    },
    "delivery": {
      "period": 15,
      "period_type": "days"
    }
  }
}
```

#### Common fields

| Field             | Type    | Rules                                         |
|-------------------|---------|-----------------------------------------------|
| `event_type`      | string  | `regional_price_quote`                        |
| `schema_version`  | integer | `1`                                           |
| `execution_id`    | string  | Unique; format `EX-{region}-NNNN`             |
| `instrument`      | string  | Known metal code                              |
| `side`            | string  | `buy` or `sell`                               |
| `quantity`        | integer | ≥ 1                                           |
| `price`           | number  | > 0.01                                        |
| `currency`        | string  | `USD`, `EUR`, or `GBP`                        |
| `event_timestamp` | string  | UTC timestamp, format `YYYYMMDD-HH:MM:SS.mmm` |
| `conditions`      | object  | Structure depends on region (see below)       |

#### Known instruments

`CA` (copper), `AH` (aluminium), `ZS` (zinc), `NI` (nickel), `PB` (lead), `SN` (tin)

#### Region constraints

| Region code | Region name | Required condition structure |
|-------------|-------------|------------------------------|
| `ME`        | Middle East | `warehouse`                  |
| `AS`        | Asia        | `warehouse`                  |
| `EU`        | Europe      | `warehouse` and `delivery`   |

##### Conditions: Middle East and Asia

```json
{
  "region": "Middle East",
  "warehouse": {
    "warehouse_id": "WH_DBX",
    "city": "Dubai"
  }
}
```

##### Conditions: Europe

```json
{
  "region": "Europe",
  "warehouse": {
    "warehouse_id": "WH_RTM",
    "city": "Rotterdam"
  },
  "delivery": {
    "period": 15,
    "period_type": "days"
  }
}
```

- `delivery.period`: integer 1–30
- `delivery.period_type`: always `days`
- `warehouse` must belong to the event region

## Validation routing

| Failure | Destination |
|---------|-------------|
| Missing `schema_id` header | DLQ `missing_schema_id` |
| Unknown `schema_id` | DLQ `unknown_schema_id:<id>` |
| Schema contract violation (missing required field, wrong type) | DLQ `schema_validation_error:<msg>` |
| `idempotency_key` absent or wrong format | DLQ `missing_header:idempotency_key` / `invalid_header:idempotency_key` |
| `event_type` not in known list | DLQ `invalid_header:event_type:<value>` |
| Metal part of key not in known instruments | DLQ `invalid_header:metal:<value>` |
| Region part of key not in known regions | DLQ `invalid_header:region:<value>` |
| Missing mandatory content field | Quarantine + DLQ `invalid_content:missing_field:<field>` |
| Wrong field type | Quarantine + DLQ `invalid_content:wrong_type:<field>` |
| Side not `buy` or `sell` | Quarantine + DLQ `invalid_content:invalid_value:side` |
| Price ≤ 0 | Quarantine + DLQ `invalid_content:below_min:price` |
| Unknown currency | Quarantine + DLQ `invalid_content:invalid_value:currency` |

Unknown headers and extra fields are tolerated (forward compatibility).

## Idempotency

- A repeated record with the same `execution_id` represents the same execution
- The consumer stores the original JSON unchanged in bronze before normalising in silver
- Deduplication is explicit in the silver layer via `QUALIFY ROW_NUMBER() OVER (PARTITION BY execution_id ...)`

## Error injection (testing)

The generator corrupts ~5% of messages to exercise validation end-to-end. Both header and content errors are injected for this topic:

**Header errors:**
- `missing_idempotency_key` — removes `idempotency_key` header
- `unknown_metal` — sets key to `XX_ME`
- `unknown_event_type` — sets `event_type` to `invalid_event`
- `unknown_header` — adds `unknown_header_xyz` (passes through, tests forward compatibility)

**Content errors:**
- `missing_mandatory_field` — removes one of `execution_id`, `quantity`, or `price`
- `wrong_field_type` — sets `price` to `"not_a_number"`
- `invalid_business_value` — sets `side` to `"sideways"`
- `negative_price` — sets `price` to `-1.0` (passes schema, caught by business rules → quarantine)
- `unknown_field` — adds `unknown_field_xyz` (passes through, tests forward compatibility)
