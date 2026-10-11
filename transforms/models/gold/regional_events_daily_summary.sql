select
    toDate(event_timestamp)         as event_date,
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
group by event_date, instrument, side, region, warehouse_city

