with source as (select * from {{ source("realestate_sydney", "realestate_analytics") }})

select
    total_results_count,
    pretty_url,
    pagination__page,
    pagination__page_size,
    pagination__more_results_available,
    pagination__max_page_number_available,
    resolved_query__channel,
    resolved_query__page_size,
    resolved_query__page,
    resolved_query__filters__surrounding_suburbs,
    resolved_query__filters__max_sold_age__value,
    resolved_query__filters__max_sold_age__unit,
    channel,
    max_page_number_available,
    _dlt_load_id,
    _dlt_id,
    current_datetime() as dbt_loaded_at_utc,
    '{{ var("job_id") }}' as dbt_job_id
from source
