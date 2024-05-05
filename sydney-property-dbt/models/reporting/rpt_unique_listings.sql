{{
    config(
        materialized="table",
    )
}}
with
    dup as (
        -- --------Duplicates that removed in next stage----------
        select r.listing_id
        from {{ ref("stg_realestate_analytics__tiered_results__results") }} r
        left join
            {{ ref("int_realestate_duplicates") }} i on r.listing_id = i.prev_listing_id
        where i.dup_indicator in ('Duplicate')
    ),
    source as (
        select
            r.listing_id,
            r.advertising__region,
            r.pretty_url,
            r.date_sold,
            case
                when i.dup_indicator in ('Duplicate')
                then coalesce(r.price__display, i.prev_price__display)
                else r.price__display
            end as price__display,
            r.property_type,
            r.land_size__value,
            r.address__postcode,
            r.address__locality,
            r.features__general__bedrooms,
            r.features__general__bathrooms,
            r.features__general__parking_spaces,
            r.address__street_address,
            r.lister__name,
            case
                when contains_substr(r.lister__email, ',')
                then
                    replace(
                        left(r.lister__email, strpos(r.lister__email, ',')), ',', ''
                    )
                else r.lister__email
            end as lister__email
        from {{ ref("stg_realestate_analytics__tiered_results__results") }} r
        left join dup d on d.listing_id = r.listing_id
        left join
            {{ ref("int_realestate_duplicates") }} i on r.listing_id = i.listing_id
        where d.listing_id is null
    ),
    dedup as (
        select postcode, locality, sa4_name_2016, lgaregion
        from {{ ref("ASGS_NSW") }}
        group by 1, 2, 3, 4
    )

select r.*, d.sa4_name_2016, d.lgaregion
from source r
left join
    dedup d
    on lower(d.locality) = lower(r.address__locality)
    and cast(d.postcode as string) = r.address__postcode
