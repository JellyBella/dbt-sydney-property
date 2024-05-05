with source as (

    select * from {{ source('realestate_sydney', 'realestate_analytics__tiered_results') }}

),

renamed as (

    select
        count,
        tier,
        _dlt_parent_id,
        _dlt_list_idx,
        _dlt_id,
        tier_description__precision,
        title

    from source

)

select * from renamed