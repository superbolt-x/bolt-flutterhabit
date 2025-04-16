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
CASE WHEN campaign_name ~* 'TTS' THEN conversion ELSE complete_payment END as purchases,
CASE WHEN campaign_name ~* 'TTS' THEN total_onsite_shopping_value ELSE total_complete_payment_rate END as revenue,
web_event_add_to_cart as atc
FROM {{ ref('tiktok_performance_by_ad') }}
