select
	"17" as execution_id,
	toInt32("34") as message_number,
	"55" as instrument,
	toInt8("54") as side,
	toInt32("32") as quantity,
	toFloat64("31") as price,
	parseDateTime(substring("52", 1, 17), '%Y%m%d-%H:%i:%S', 'UTC') as event_timestamp,
	parseDateTime(substring("60", 1, 17), '%Y%m%d-%H:%i:%S', 'UTC') as trade_timestamp,
	source_file,
	pipeline_version
from bronze.fix_events
qualify row_number() over (partition by "17" order by "34" desc) = 1
