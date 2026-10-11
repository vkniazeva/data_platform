with world_prices as (
select
        toDate(trade_timestamp)         as trade_date,
        instrument,
        multiIf(side=1, 'buy', 'sell')  as side,
        round(avg(price), 2)            as global_avg_price,
        min(price)                      as global_min_price,
        max(price)                      as global_max_price
from {{ref('fix_events')}}
group by trade_date, instrument, side
),
regional_prices as (
    select
          toDate(event_timestamp)  as trade_date,
          instrument,
          side,
          region,
          round(avg(price), 2)     as regional_avg_price,
          min(price)               as regional_min_price,
          max(price)               as regional_max_price
    from {{ref('regional_events')}}
    group by trade_date, instrument, side, region
)
select
    r.trade_date    as trade_date,
    r.instrument    as instrument,
    r.side          as side,
    r.region        as region,
    r.regional_avg_price,
    r.regional_min_price,
    r.regional_max_price,
    f.global_avg_price,
    f.global_min_price,
    f.global_max_price,
    round(r.regional_avg_price - f.global_avg_price, 2) as price_spread
from world_prices f
left join regional_prices r
	on f.trade_date = r.trade_date
	and f.instrument = r.instrument
	and f.side = r.side
