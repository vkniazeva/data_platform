select
	toDate(trade_timestamp)         as trade_date,
	instrument,
	if(side=1, 'buy', 'sell')       as side,
	count(execution_id)             as number_of_deals,
	sum(quantity)                   as total_quantity,
	round(avg(quantity),2)          as avg_quantity,
	min(price)                      as min_price,
	round(avg(price),2)             as avg_price,
	round(quantile(0.5)(price), 2)  as median_price,
	max(price)                      as max_price,
	groupUniqArray(_producer_id)    as producers,
	groupUniqArray(_parser_version) as parser_versions
from {{ref('fix_events')}}
group by trade_date, instrument, side
