select
    'fix_events' as source_table,
    toDate(event_timestamp) as event_date,
    groupUniqArray(source_file) as raw_files,
    count() as count
from {{ref('fix_events')}}
group by event_date

union all
select
    'regional_events' as source_table,
    toDate(event_timestamp) as event_date,
    groupUniqArray(source_file) as raw_files,
    count() as count
from {{ref('regional_events')}}
group by event_date