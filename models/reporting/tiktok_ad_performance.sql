{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id::varchar,
campaign_status,
campaign_type_default,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
CASE WHEN campaign_name ~* 'TTS' THEN conversions ELSE complete_payment END as purchases,
complete_payment_rate as revenue,
web_add_to_cart_events as atc
FROM {{ ref('tiktok_performance_by_ad') }}
