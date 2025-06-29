{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
clicks,
purchases_adj as purchases, 
revenue_adj as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign') }}
LEFT JOIN
    (SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity, 
    customer_id as account_id, name as campaign_name, id as campaign_id,
    COALESCE(SUM(conversions),0) as purchases_adj, COALESCE(SUM(conversions_value),0) as revenue_adj
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    WHERE conversion_action_name = 'Google Shopping App Purchase (1)'
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity, 
    customer_id as account_id, name as campaign_name, id as campaign_id,
    COALESCE(SUM(conversions),0) as purchases_adj, COALESCE(SUM(conversions_value),0) as revenue_adj
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    WHERE conversion_action_name = 'Google Shopping App Purchase (1)'
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity, 
    customer_id as account_id, name as campaign_name, id as campaign_id,
    COALESCE(SUM(conversions),0) as purchases_adj, COALESCE(SUM(conversions_value),0) as revenue_adj
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    WHERE conversion_action_name = 'Google Shopping App Purchase (1)'
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity, 
    customer_id as account_id, name as campaign_name, id as campaign_id,
    COALESCE(SUM(conversions),0) as purchases_adj, COALESCE(SUM(conversions_value),0) as revenue_adj
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    WHERE conversion_action_name = 'Google Shopping App Purchase (1)'
    GROUP BY 1,2,3,4,5
    UNION ALL
    SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity, 
    customer_id as account_id, name as campaign_name, id as campaign_id,
    COALESCE(SUM(conversions),0) as purchases_adj, COALESCE(SUM(conversions_value),0) as revenue_adj
    FROM {{ source('googleads_raw','campaign_convtype_performance_report') }}
    WHERE conversion_action_name = 'Google Shopping App Purchase (1)'
    GROUP BY 1,2,3,4,5
    )
USING (date,date_granularity,account_id,campaign_name,campaign_id)
