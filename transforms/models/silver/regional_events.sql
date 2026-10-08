select
	execution_id as execution_id,
	instrument as instrument,
	side as side,
	toInt32(quantity) as quantity,
	toFloat64(price) as price,
	currency as currency,
	parseDateTime(substring(event_timestamp, 1, 17), '%Y%m%d-%H:%i:%S', 'UTC') as event_timestamp,
	JSONExtractString(conditions, 'region') as region,
	JSONExtractString(JSONExtractRaw(conditions, 'warehouse'), 'warehouse_id') as warehouse_id,
	JSONExtractString(JSONExtractRaw(conditions, 'warehouse'), 'city') as warehouse_city,
	source_file as source_file,
	pipeline_version as pipeline_version
from bronze.regional_events
qualify row_number() over (partition by execution_id order by event_timestamp desc) = 1
