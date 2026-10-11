select
    toDate(event_timestamp)         as event_date,
    toHour(event_timestamp)         as event_hour,
	multiIf(
	    toHour(event_timestamp) < 6, 'night',
	    toHour(event_timestamp) < 12, 'morning',
	    toHour(event_timestamp) < 18, 'afternoon',
	    'evening'
	)                               as time_of_day,
    instrument,
    side,
    region,
    warehouse_city,
    count(execution_id)             as number_of_deals,
    sum(quantity)                   as total_quantity,
    round(avg(quantity),2)          as avg_quantity,
    min(price)                      as min_price,
    round(avg(price),2)             as avg_price,
    round(quantile(0.5)(price), 2)  as median_price,
    max(price)                      as max_price
from {{ref('regional_events')}}
group by event_date, event_hour, time_of_day,  instrument, side, region, warehouse_city

