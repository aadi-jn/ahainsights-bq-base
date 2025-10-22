# Base query - getting all the useful raw data into a model
# temp_aadi.events_all


# this query is taking the entire table more or less.
# it is changing the names to make subsequent queries easier
# it is adding fields by extracting information from event params
# it can add more fields by getting info from items array.
  
select 
* ,
CASE
    #this regex statement should capture home page without issue. The $ sign at the end means it wont recognise homepages with trailing information like utm sources. Need to give cleaned page location here.
    when regexp_contains(page_location, r'^https?:\/\/[^\/]+\/(?:\?|#|$)') then 'home'
    WHEN page_location LIKE '%/products/%' or page_location like '%/product/%' THEN 'products'
    WHEN page_location LIKE '%/collections/%' THEN 'collection'
    WHEN page_location LIKE '%/blogs/%' THEN 'blogs'
    WHEN page_location LIKE '%/account%' THEN 'account'
    WHEN page_location LIKE '%/cart%' THEN 'cart_page'
    WHEN page_location LIKE '%/checkouts/%' THEN 'checkouts'
    WHEN page_location LIKE '%search%' THEN 'search'
    WHEN page_location LIKE '%/policies/%' THEN 'policies'
    WHEN page_location LIKE '%/pages/%' THEN 'generic_page'
    -- WHEN page_location in ('https://keralaayurveda.biz/','https://keralaayurveda.store/') THEN 'homepage'
    ELSE 'other'
END as page_type,
case when page_referrer like '%keralaayurveda%' then TRUE else FALSE end as external_page_referral,
row_number() over(partition by user_id, date_ist order by event_ts asc) as event_counter
# event_counter helps to distinguish between multiple instances of an event logging at the same time.

from 
(
  SELECT 
  date(timestamp_micros(event_timestamp),'Asia/Kolkata') as date_ist,
  user_pseudo_id as user_id,
  user_first_touch_timestamp,
  event_name as event,
  event_timestamp as event_ts,
 event_value_in_usd,
  (SELECT max(params.value.int_value) from unnest(event_params) params where params.key = 'ga_session_id') as ga_session_id,

  (SELECT max(COALESCE(params.value.int_value,params.value.float_value, params.value.double_value)) from unnest(event_params) params where params.key = 'ecomm_totalvalue') as ecomm_totalvalue,
    (SELECT max(COALESCE(params.value.int_value,params.value.float_value, params.value.double_value)) from unnest(event_params) params where params.key = 'value') as value,

  (SELECT max(params.value.int_value) from unnest(event_params) params where params.key = 'ga_session_number') as ga_session_number,
 
  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'page_location') as page_location,
   (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'ecomm_prodid') as ecomm_prodid,

   (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'currency') as currency,

  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'page_path') as page_path,
  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'page_referrer') as page_referrer,
  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'ignore_referrer') as ignore_referrer,
  
  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'page_title') as page_title,
  (SELECT max(params.value.string_value) from unnest(event_params) params where params.key = 'ecomm_pagetype') as ecomm_pagetype, 
(SELECT max(params.value.int_value) from unnest(event_params) params where params.key = 'engagement_time_msec') as engagement_time_msec,
 event_params,
  items,
  ecommerce,
  device,
  geo,
  traffic_source,
  session_traffic_source_last_click,
  collected_traffic_source,
  

  FROM
    `kerala-ayurveda-wh.analytics_469068251.events_20250916` 
  WHERE
  event_name in
 (
  'session_start'
  ,'first_visit'
  ,'page_view'
  ,'user_engagement'
  ,'click'
  ,'view_item'
  ,'add_shipping_info'
  ,'view_cart'
  ,'add_to_cart'
  ,'begin_checkout'
  ,'purchase'
  ,'add_payment_info'
  ,'search'
  ,'view_search_results'
  ,'scroll'
  ,'video_start'
  ,'video_complete'
  ,'video_progress'
 ))
