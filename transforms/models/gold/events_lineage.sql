select
    'fix_events' as source_table,
    toDate(event_timestamp) as event_date,
    groupUniqArray(source_file) as raw_files,
    groupUniqArray(_producer_id) as producers,
    groupUniqArray(_parser_version) as parser_versions,
    groupUniqArray(_schema_id) as schema_ids,
    count() as count
from {{ref('fix_events')}}
group by event_date

union all
select
    'regional_events' as source_table,
    toDate(event_timestamp) as event_date,
    groupUniqArray(source_file) as raw_files,
    groupUniqArray(_producer_id) as producers,
    groupUniqArray(_parser_version) as parser_versions,
    groupUniqArray(_schema_id) as schema_ids,
    count() as count
from {{ref('regional_events')}}
group by event_date