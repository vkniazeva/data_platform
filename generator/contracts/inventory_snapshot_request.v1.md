# Inventory Snapshot API Request v1

## Endpoint

```http
POST /v1/inventory-snapshots
```

### Headers

| Header             | Required | Example                                | Description                                   |
|--------------------|----------|----------------------------------------|-----------------------------------------------|
| `Content-Type`     | yes      | `application/json`                     | Request body format                           |
| `Idempotency-Key`  | yes      | `inv-20261003-000001`                  | Stable unique ID for this snapshot submission |
| `X-Source`         | yes      | `synthetic-inventory-generator`        | Data producer identifier                      |
| `X-Schema-Version` | yes      | `1`                                    | Request contract version                      |
| `X-Request-Id`     | yes      | `6d1a84b6-6cb3-4fa3-acb2-6729c7aa7a2d` | Unique request trace ID                       |

### Request Body
```json
{
  "snapshot_id": "inv-20261003-000001",
  "observed_at": "2026-10-03T14:30:05.123Z",
  "items": [
    {
      "location": "WH_RTM",
      "location_type": "warehouse",
      "available_stock": [
        {
          "metal_id": "CA",
          "quantity": 1250
        }
      ],
      "reserved_stock": [
        {
          "metal_id": "CA",
          "quantity": 150
        }
      ]
    }
  ]
}
```

### Field rules

| Field             | Type     | Rules                                          |
|-------------------|----------|------------------------------------------------|
| `snapshot_id`     | string   | Must equal `Idempotency-Key` for v1            |
| `observed_at`     | string   | UTC timestamp in ISO 8601 format               |
| `items`           | array    | Contains 1 or more locations                   |
| `location`        | string   | Known warehouse or barge ID                    |
| `location_type`   | string   | `warehouse` or `barge`                         |
| `available_stock` | array    | May be empty                                   |
| `reserved_stock`  | array    | May be empty                                   |
| `metal_id`        | string   | Known metal code, for example `CA`, `AH`, `NI` |
| `quantity`        | integer  | Non-negative                                   |


### Business rules

- One request may contain one or multiple locations
- A location may appear only once in items
- A metal_id may appear only once within each stock array for one location
- For every location + metal_id, reserved quantity must not exceed available quantity
- Free stock is calculated by the receiver:

free_quantity = available_quantity - reserved_quantity

### Idempotency
The receiver must treat repeated requests with the same Idempotency-Key and identical body as the same snapshot.
The same key with a different body must be return the same response: for all next requests no operations should be performed