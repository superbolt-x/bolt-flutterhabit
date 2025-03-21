{{ config (
    alias = target.database + '_tiktok_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id::varchar,
campaign_status,
campaign_type_default,
date,
date_granularity,
cost as spend,
impressions,
clicks,
CASE WHEN campaign_name ~* 'TTS' THEN conversions ELSE complete_payment_events END as purchases,
CASE WHEN campaign_name ~* 'TTS' THEN total_onsite_shopping_value ELSE complete_payment_value END as revenue,
web_add_to_cart_events as atc
FROM {{ ref('tiktok_performance_by_campaign') }}
LEFT JOIN 
    (SELECT stat_time_day::date as date, campaign_id::varchar as campaign_id, COALESCE(SUM(total_onsite_shopping_value),0) as total_onsite_shopping_value
    FROM {{ source('tiktok_raw','campaign_report_daily') }} 
    WHERE campaign_id = '1809207092406273'
    GROUP BY 1,2) USING(campaign_id, date)
