----dedup logic included----
----the same listings can be returned from different API calls----
----as results of surrunding areas (due to imperfect API response)----
{{config(materialized='table')}}

with source_data as(

    select * from {{ source('realestate_sydney', 'realestate_analytics__tiered_results__results') }}

),

data_with_row_num as(

    select 
        a.*,
        row_number() over (partition by a.listing_id order by gp._dlt_load_id) as row_number
        
    from source_data a
        left join {{ source('realestate_sydney', 'realestate_analytics__tiered_results') }} p ON p._dlt_id = a._dlt_parent_id
        left join {{ source('realestate_sydney', 'realestate_analytics') }} gp ON gp._dlt_id = p._dlt_parent_id

), 

dedup as (
    select *
    from data_with_row_num
    where row_number=1
)

select 
    channel,
    listing_id,
    description,
    agency__agency_id,
    agency__website,
    agency__address__postcode,
    agency__address__state,
    agency__address__street_address,
    agency__address__suburb,
    agency__phone_number,
    agency__branding_colors__primary,
    agency__branding_colors__text,
    agency__name,
    agency__logo__links__small,
    agency__logo__links__hero_image,
    agency__logo__links__default,
    agency__logo__links__large,
    agency__email,
    agency__branded,
    case 
        when price__display = 'Contact agent' 
        then null 
        ---if range, take max---
        when CONTAINS_SUBSTR(price__display,'-') 
        then cast(REGEXP_REPLACE(replace(replace(replace(price__display,'$',''),',',''),'Range: ',''), r'.* - ', '')as int)
        else cast(replace(replace(price__display,'$',''),',','') as int)
    end as price__display,
    price__abbreviated,
    construction_status,
    modified_date__value,
    property_type,
    general_features__parking_spaces__label,
    general_features__parking_spaces__type,
    general_features__parking_spaces__value,
    general_features__bathrooms__label,
    general_features__bathrooms__type,
    general_features__bathrooms__value,
    general_features__bedrooms__label,
    general_features__bedrooms__type,
    general_features__bedrooms__value,
    property_type_group,
    date_sold__display,
    parse_date('%Y-%m-%d',date_sold__value) as date_sold,
    address__street_address,
    address__postcode,
    address__locality,
    address__suburb,
    address__location__latitude,
    address__location__longitude,
    address__post_code,
    address__state,
    address__subdivision_code,
    address__show_address,
    land_size__display_app_abbreviated,
    land_size__display_app,
    land_size__unit,
    land_size__value,
    land_size__display,
    title,
    show_agency_branding_on_standard,
    building_size__value,
    building_size__unit,
    product_depth,
    main_image__uri,
    main_image__name,
    main_image__server,
    advertising__region,
    advertising__price_range,
    pretty_url,
    show_agency_logo,
    status__type,
    status__label,
    calculator__branding_colors__text,
    calculator__branding_colors__primary,
    calculator__subtitle,
    calculator__title,
    standard,
    featured,
    midtier,
    signature,
    property_type_id,
    property_type_display,
    features__general__bedrooms,
    features__general__bathrooms,
    features__general__parking_spaces,
    is_sold_channel,
    is_buy_channel,
    is_rent_channel,
    classic_project,
    signature_project,
    lister__mobile_phone_number,
    lister__website,
    lister__phone_number,
    lister__power_profile,
    lister__job_title,
    lister__name,
    lister__main_photo__server,
    lister__main_photo__name,
    lister__main_photo__uri,
    lister__id,
    lister__email,
    _dlt_parent_id,
    _dlt_list_idx,
    _dlt_id,
    lister__agent_id,
    price__disclaimer_type,
    frontpage_image__uri,
    frontpage_image__name,
    frontpage_image__server,
    property_id
from dedup