

WITH
  events AS (
  SELECT
    user_id,
    ga_session_id,
    event,
    event_counter,
    event_ts,
    # update the below fields for adding or removing information in the model
    device,
    geo,
    traffic_source, # you can also use collected_traffic_source or session_source_last_click from GA4 depending on use case
    event_params
  FROM
    `temp_aadi.events_all_v0_2` 
    ## this will be replaced by events for past 3 days.
    ),
  existing_sessions as (
    select * from `temp_aadi.sessions_v0_1`
    where
    date_ist between date_sub(current_date(),interval 4 day) and date_sub(current_date(),interval 3 day)
  # here we take 3-4 days old sessions. Because essentially we care about the midnight spanning sessions only. not older or newer sessions in this part of query.
  ),
  event_details AS (
  SELECT
    user_id,
    ga_session_id,
    event,
    event_counter,,
    event_ts,
    MAX(CASE
        WHEN device.category IS NOT NULL THEN device.category
    END
      ) AS device_category,
    MAX(CASE
        WHEN device.browser IS NOT NULL THEN device.browser
    END
      ) AS device_browser,
      MAX(CASE
        WHEN device.web_info.browser IS NOT NULL THEN device.web_info.browser
    END
      ) AS device_web_info_browser,
    MAX(CASE
        WHEN geo.country IS NOT NULL THEN geo.country
    END
      ) AS geo_country,
    MAX(CASE
        WHEN geo.region IS NOT NULL THEN geo.region
    END
      ) AS geo_region,
    MAX(CASE
        WHEN geo.city IS NOT NULL THEN geo.city
    END
      ) AS geo_city,
    MAX(CASE
        WHEN traffic_source.name IS NOT NULL THEN traffic_source.name
    END
      ) AS traffic_name,
    MAX(CASE
        WHEN traffic_source.medium IS NOT NULL THEN traffic_source.medium
    END
      ) AS traffic_medium,
    MAX(CASE
        WHEN traffic_source.source IS NOT NULL THEN traffic_source.source
    END
      ) AS traffic_source,
    MAX(CASE
        WHEN KEY = 'source' THEN value.string_value
    END
      ) AS ep_source,
    MAX(CASE
        WHEN KEY = 'page_location' THEN value.string_value
    END
      ) AS landing_page,
    MAX(CASE
        WHEN KEY = 'medium' THEN value.string_value
    END
      ) AS ep_medium,
    MAX(CASE
        WHEN KEY = 'campaign' THEN value.string_value
    END
      ) AS ep_campaign,
    MAX(CASE
        WHEN KEY = 'ecomm_totalvalue' THEN COALESCE(value.int_value, value.double_value, value.float_value)
    END
      ) AS ecomm_totalvalue,
     MAX(CASE
        WHEN KEY = 'value' THEN COALESCE(value.int_value, value.double_value,value.float_value)
    END
      ) AS value,
    MAX(CASE
        WHEN KEY = 'campaign_id' THEN value.string_value
    END
      ) AS ep_campaign_id
  FROM
    events,
    UNNEST(event_params)
  GROUP BY
    1,
    2,
    3,
    4,
    5),
  session_details AS (
  SELECT
    *,
    event_ts as session_start_ts,
    ROW_NUMBER() OVER(PARTITION BY user_id, ga_session_id ORDER BY event_ts ASC) AS first_session_event
  FROM
    event_details
  QUALIFY
    first_session_event = 1),
  session_key_events AS (
  SELECT
    user_id,
    ga_session_id,
    MAX(event_ts) AS session_max_ts,
    max(case when event = 'view_item' then 1 else 0 end) as session_with_item_view,
    max(case when event = 'purchase' then 1 else 0 end) as session_with_purchase,
    max(case when event = 'purchase' then value end) as purchase_value,
    max(case when event = 'add_to_cart' then 1 else 0 end) as session_with_adds_to_cart,
    max(case when event = 'add_to_cart' then value end) as add_to_cart_value,
    max(case when event = 'begin_checkout' then 1 else 0 end) as session_with_checkouts,
    max(case when event = 'begin_checkout' then value end) as checkout_value
  FROM
    event_details
  GROUP BY
    1,
    2),
new_sessions as (
  SELECT
  t1.user_id,
  t1.ga_session_id,
  t1.event_counter,
  t1.event,
  # Session Type is client specific
  # Before finalizing this, check with client for requirements. 
  CASE
    WHEN ( (t1.ep_medium = 'cpc' AND t1.ep_source = 'google' ) OR (t1.ep_medium = 'cpc' AND t1.ep_source = 'facebook') OR LOWER(t1.ep_medium) IN ('nbsearch', 'paid', 'psocial') ) THEN 'paid'
    WHEN ( LOWER( t1.ep_source) LIKE '%limechat%'
    OR LOWER(t1.ep_medium) LIKE '%limechat%'
    OR LOWER(t1.ep_medium) IN ('influencer',
      'zalo',
      'whatsapp_campaign') ) THEN 'Internal channel'
    ELSE 'organic'
END
  AS session_type,
  # landing page is not CLEANED. It is as available in the event params.
  t1.landing_page,
# This session_start_ts corresponds to the first event with the session_id.
# This can have issues occasionally. Need to add checks.
  session_start_ts,
  DATE(TIMESTAMP_MICROS(session_start_ts),'Asia/Kolkata') AS date_ist,
device_category,
-- device_browser,
device_web_info_browser,
geo_country as country,
geo_city as city,
geo_region as region,
  t1.traffic_source,
  t1.traffic_medium,
  t1.traffic_name,
  t1.ep_source,
  t1.ep_medium ,
  t1.ep_campaign,
  t1.ep_campaign_id,
  t2.session_max_ts as session_end_ts,
  # Below fields are key events for a session. Can add more as needed.
  session_with_item_view,
  session_with_purchase,
  purchase_value,
  session_with_adds_to_cart,
  add_to_cart_value,
  session_with_checkouts,
  checkout_value,
# session duration is calculated from first and last events of a session. Can have issues. 
  (t2.session_max_ts - t1.session_start_ts) AS session_duration_micros
FROM
  session_details t1
JOIN
  session_key_events t2
ON
  t1.user_id = t2.user_id
  AND t1.ga_session_id = t2.ga_session_id)


 ( select t1.* from 
  (select * from new_sessions)t1 
  left join (select user_id, ga_session_id from existing_sessions)t2 
  on t1.user_id = t2.user_id and t1.ga_session_id = t2.ga_session_id
  where
  t2.user_id is null)
  union all
  (
    select 
    t1.user_id, t1.ga_session_id,   t1.event_counter,
  t1.event,
 t1.session_type, t1.landing_page, t2.session_start_ts, t2.date_ist,
    t1.device_category,
     -- t1.device_browser,
     t1.device_web_info_browser, t1.country, t1.city, t1.region,
  t1.traffic_source,
  t1.traffic_medium,
  t1.traffic_name,
  t1.ep_source,
  t1.ep_medium ,
  t1.ep_campaign,
  t1.ep_campaign_id,
  t1.session_end_ts as session_end_ts,
  # Why am i taking values as it is? because the existing sessions table has complete detail of sessions.
  t2.session_with_item_view,
  t2.session_with_purchase,
  t2.purchase_value,
  t2.session_with_adds_to_cart,
  t2.add_to_cart_value,
  t2.session_with_checkouts,
  t2.checkout_value,
 (t1.session_end_ts - t2.session_start_ts) AS session_duration_micros
     from 
  (select * from new_sessions)t1
   join
  (select * from existing_sessions)t2 
  on t1.user_id = t2.user_id and t1.ga_session_id = t2.ga_session_id

)

  
