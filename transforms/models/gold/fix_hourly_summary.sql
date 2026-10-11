select
	toDate(trade_timestamp)         as trade_date,
	toHour(trade_timestamp)         as trade_hour,
	multiIf(
	    toHour(trade_timestamp) < 6, 'night',
	    toHour(trade_timestamp) < 12, 'morning',
	    toHour(trade_timestamp) < 18, 'afternoon',
	    'evening'
	)                               as time_of_day,
	instrument,
	if(side=1, 'buy', 'sell')       as side,
	count(execution_id)             as number_of_deals,
	sum(quantity)                   as total_quantity,
	round(avg(quantity),2)          as avg_quantity,
	min(price)                      as min_price,
	round(avg(price),2)             as avg_price,
	round(quantile(0.5)(price), 2)  as median_price,
	max(price)                      as max_price
from {{ref('fix_events')}}
group by trade_date, trade_hour, time_of_day, instrument, side
