-- -listings with same address with sold dates within 7 days----
-- -the assumption is that the listing was replaced by a new one by listers---
{{
    config(
        materialized="table",
    )
}}
with
    staging as (
        select
            *,
            (
                address__street_address || ', ' || address__postcode
            ) as unique_property_key,
        from {{ ref("stg_realestate_analytics__tiered_results__results") }}
    ),

    unique_key as (
        select unique_property_key
        from staging
        where address__street_address <> 'Address available on request'
        group by 1
        having count(*) > 1
    ),
    interm as (
        select
            c.*,
            lag(date_sold) over (
                partition by i.unique_property_key order by date_sold
            ) as prev_date_sold,
            lag(price__display) over (
                partition by i.unique_property_key order by date_sold
            ) as prev_price__display,
            lag(listing_id) over (
                partition by i.unique_property_key order by date_sold
            ) as prev_listing_id,
            row_number() over (
                partition by i.unique_property_key order by date_sold
            ) as listing_id_seq
        from unique_key i
        inner join staging c on i.unique_property_key = c.unique_property_key
    ),
    final as (
        select
            date_diff(date_sold, prev_date_sold, day) as day_gap,
            case
                when price__display is null or prev_price__display is null
                then null  -- the previous record being replaced by new one
                else price__display - prev_price__display
            end as value_gap,
            case
                when date_diff(date_sold, prev_date_sold, day) <= 7
                then 'two listings with sold dates within a week'
                when date_diff(date_sold, prev_date_sold, day) > 7
                then 'two listings with sold dates more than a week'
                when prev_date_sold is null
                then 'first listing'
                else 'N/A'
            end as cat,
            *
        from interm
    )

select
    *,
    case
        when day_gap = 0
        then 'Duplicate'
        when value_gap = 0
        then 'Duplicate'
        when date_diff(date_sold, prev_date_sold, day) <= 7
        then 'Duplicate'
        else cat
    end as dup_indicator
from final
