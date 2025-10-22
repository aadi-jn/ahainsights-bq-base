
with all_urls as (select 
  * 
from 
temp_aadi.all_pages_v0_1
),
new_urls as (
  select * 
    from 
  all_urls 
    where
  new_page = 1
  # this includes all landing pages by default.
),
repeat_urls as (
  select
 * 
from 
  all_urls 
where
  new_page = 0
)

# For a user, in a session, a page will have multiple events
# all_pages will have an entry for each of these
# I want to get the first instance of a page in sequence - which new_page gets
# I want to assign all the events after this to the new_page
# And adjust the values - of next page, referrer, time based on this.

## window - user_id, session_id,page_window (the pages between two new pages..)

select
 *
  from 
  (
    select 
  user_id, ga_session_id, event_ts, event, page_type, page_location, page_referrer, event_counter,page_num,
  first_value(date_ist) over(partition by user_id, ga_session_id, page_num order by event_counter asc) as date_ist,
  first_value(event) over(partition by user_id, ga_session_id, page_num order by event_counter asc) as first_event,
  first_value(event_ts) over(partition by user_id, ga_session_id, page_num order by event_counter asc) as first_event_ts,
  first_value(page_referrer) over(partition by user_id, ga_session_id, page_num order by event_counter asc ) as first_page_ref,
    first_value(new_page) over(partition by user_id, ga_session_id, page_num order by event_counter asc ) as new_page,
    first_value(landing_page) over(partition by user_id, ga_session_id, page_num order by event_counter asc ) as landing_page,

  first_value(event_ts) over(partition by user_id, ga_session_id, page_num order by event_counter desc ) as last_event_ts,
  first_value(next_page) over(partition by user_id, ga_session_id, page_num order by event_counter desc ) as next_page_location,
  first_value(exit_page) over(partition by user_id, ga_session_id, page_num order by event_counter desc ) as exit_page,
  -- first_value(next_page) over(partition by user_id, ga_session_id, page_num order by event_counter desc ) as next_page_location,
  -- first_value(next_page_ts) over(partition by user_id, ga_session_id, page_num order by event_counter desc ) as next_page_ts,

  from
    (select date_ist, user_id, ga_session_id, event_ts, event, page_type, page_location, page_referrer, event_counter, new_page, landing_page, exit_page,
    next_page,
    sum(new_page) over(partition by user_id, ga_session_id ORDER BY event_counter asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  ) as page_num
  from
    all_urls
    )
    )
    order by user_id, event_ts asc

# I want the total time spent on the page in this table. unique_page_inc doesn't have it. It relies on next_page_ts instead which fails for cases where user exits the site from the page.
