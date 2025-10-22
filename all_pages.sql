# Query for all_pages
# This creates a list of all pages with events.
# The query adds dummy events to account for page changes without page view events preceding them
# Result - is a query mapping users movement on the store in a session.

# For midnight spanning sessions, this query will add an extra row of 'new page' against a session. This will be the page at the midnight
# this page will be added when the first half of the session will be disregarded (the date range will not include that day anymore)
# this will lead to repeat rows. Check when adding in DBT.


?? How does this query handle checkout pages? Anything needed on that front?

with pageviews as 
(SELECT
  event,
  date_ist,
  event_ts,
  user_id,
  page_type,
  page_location,
  ga_session_id,
  page_referrer,
  event_counter
FROM
  `temp_aadi.events_all_v0_2`
),
url as (
select user_id, date_ist, ga_session_id, event, event_ts, event_counter, page_location, page_referrer, page_type
 from pageviews
),
pageviews_cleaned as 
(
 select
 user_id, date_ist, ga_session_id, event, event_ts, event_counter,page_type,
 case when variant_id is not null then concat(LEFT(page_location, IF(STRPOS(page_location, '#') = 0, LENGTH(page_location), STRPOS(page_location, '#') - 1)),"?variant_id = ", variant_id) else LEFT(page_location, IF(STRPOS(page_location, '#') = 0, LENGTH(page_location), STRPOS(page_location, '#') - 1)) end as page_location,
 variant_id,
 page_location_og,
 case when variant_id_page_referrer is not null then concat(LEFT(page_referrer, IF(STRPOS(page_referrer, '#') = 0, LENGTH(page_referrer), STRPOS(page_referrer, '#') - 1)),"?variant_id = ", variant_id_page_referrer) else LEFT(page_referrer, IF(STRPOS(page_referrer, '#') = 0, LENGTH(page_referrer), STRPOS(page_referrer, '#') - 1)) end as page_referrer,
variant_id_page_referrer, 
page_referrer_og

from
(
select
user_id, date_ist, ga_session_id, event, event_ts, event_counter,page_type,
  LEFT(page_location, IF(STRPOS(page_location, '?') = 0, LENGTH(page_location), STRPOS(page_location, '?') - 1)) AS page_location,
    case when page_location like '%/products/%' then REGEXP_EXTRACT(page_location, r'variant=(\d+)') end as variant_id,
  page_location as page_location_og,

   LEFT(page_referrer, IF(STRPOS(page_referrer, '?') = 0, LENGTH(page_referrer), STRPOS(page_referrer, '?') - 1)) AS page_referrer,
    case when page_referrer like '%/products/%' then REGEXP_EXTRACT(page_referrer, r'variant=(\d+)') end as variant_id_page_referrer,
  page_referrer as page_referrer_og


  from url
) 
)

select *, 
case when prev_page is null then 1 when page_location = prev_page then 0 else 1 end as new_page ,
case when prev_page is null then 1 else 0 end as landing_page,
case when next_page is null then 1 else 0 end as exit_page
from
(select *, 
lag(page_location) over(partition by user_id, ga_session_id order by event_counter asc) as prev_page,
lead(page_location) over(partition by user_id, ga_session_id order by event_counter asc) as next_page
# this partition by statement will cause issues for midnight spanning pages.
from pageviews_cleaned)
