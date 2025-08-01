{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

{%- set granularities = ['day', 'week', 'month', 'quarter', 'year'] -%}

with tiktok_data as (
    SELECT 
    campaign_name,
    campaign_id::varchar as campaign_id,
    campaign_status,
    campaign_type_default,
    adgroup_name,
    adgroup_id::varchar as adgroup_id,
    adgroup_status,
    audience,
    ad_name,
    ad_id::varchar as ad_id,
    ad_status,
    visual,
    date,
    date_granularity,
    cost as spend,
    impressions,
    clicks,
    CASE WHEN campaign_name ~* 'TTS' THEN conversion ELSE complete_payment END as purchases,
    CASE WHEN campaign_name ~* 'TTS' THEN total_onsite_shopping_value ELSE total_complete_payment_rate END as revenue,
    CASE WHEN campaign_name ~* 'TTS' THEN onsite_on_web_cart ELSE web_event_add_to_cart END as atc
    FROM {{ ref('tiktok_performance_by_ad') }}
)


{% for granularity in granularities %}
, {{ granularity }}_agg_gmv as (
    select 
        campaign_name,
        campaign_id::varchar,
        '(not set)' as campaign_status,
        '(not set)' as campaign_type_default,
        '(not set)' as adgroup_name,
        '(not set)' as adgroup_id,
        '(not set)' as adgroup_status,
        '(not set)' as audience,
        '(not set)' as ad_name,
        '(not set)' as ad_id,
        '(not set)' as ad_status,
        '(not set)' as visual,
        date_trunc('{{ granularity }}', date) as date,
        '{{ granularity }}' as date_granularity,
        sum(spend) as spend,
        sum(0) as impressions,
        sum(0) as clicks,
        sum(purchases) as purchases,
        sum(revenue) as revenue,
        sum(0) as atc
    from {{ source('gsheet_raw','tiktok_gmv_insights') }}
    group by 
        campaign_name,
        campaign_id,
        date_trunc('{{ granularity }}', date)
)
{% endfor %}

select * from day_agg_gmv
{% for granularity in granularities[1:] %}
union all
select * from {{ granularity }}_agg_gmv
{% endfor %}
union all
select * from tiktok_data
