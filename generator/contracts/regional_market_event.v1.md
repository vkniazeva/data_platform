# Regional Market Event v1

data type: semistructured

## Kafka record

topic: market.events
key: execution_id
value: JSON, UTF-8 encoded

### Kafka Headers

| Header           | Required | Example                        | Description                                 |
|------------------|----------|--------------------------------|---------------------------------------------|
| `event_type`     | yes      | `regional_market_observation`  | Event type; must equal `value.event_type`   |
| `schema_version` | yes      | `1`                            | Must equal `value.schema_version`           |
| `source`         | yes      | `synthetic_regional_market`    | Event producer                              |
| `content_type`   | yes      | `application/json`             | Kafka value format                          |

### Event value

```json
{
  "event_type": "regional_market_observation",
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

| Field             | Type     | Rules                                          |
|-------------------|----------|------------------------------------------------|
| `event_type`      | string   | `regional_market_observation`                  |
| `schema_version`  | integer  | `1`                                            |
| `execution_id`    | string   | Unique execution identifier; used as Kafka key |
| `instrument`      | string   | Known metal code                               |
| `side`            | string   | `buy` or `sell`                                |
| `quantity`        | integer  | Positive                                       |
| `price`           | number   | Positive                                       |
| `currency`        | string   | `USD`                                          |
| `event_timestamp` | string   | UTC timestamp, format `YYYYMMDD-HH:MM:SS.mmm`  |
| `conditions`      | object   | Structure depends on region                    |

#### Region constraints

Only these regional codes are valid:

| Region code in `execution_id` | Region in `conditions.region` | Required condition structure |
|-------------------------------|-------------------------------|------------------------------|
| `ME `                         | `Middle East`                 | `warehouse`                  |
| `AS`                          | `Asia`                        | `warehouse`                  |
| `EU`                          | `Europe`                      | `warehouse` and `delivery`   |

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

For ME and AS, delivery must be absent

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

For EU:
- delivery.period is an integer from 1 to 30
- delivery.period_type is always days
- warehouse must belong to the event region

## Idempotency and validation

- Kafka key must equal value.execution_id
- Header event_type and schema_version must match the corresponding value fields
- A repeated record with the same execution_id represents the same execution
- The consumer stores the original JSON unchanged before normalising it
- A record with an unknown region, invalid header, invalid schema version, or invalid conditions shape goes to quarantine



