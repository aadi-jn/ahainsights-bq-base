## User querys tricky parts
## Getting all users - those without first_visit event as well
## Without parsing the entire table.

# Cases
## Good - single first visit event, user_first_touch_timestamp matches event ts of first visit (99%)
## Missing first visit event, user_first_touch_timestamp available
## first visit event, but  user_first_touch_timestamp is of an older date!
## missing both

## user table on top of sessions table..
## new user from first visit
## new user without first visit - based on user first touch timestamp available in all events
## returning user with no prior entry in data - because we dont have data since forever + earlier events could be missing


with users as 
(
select
  user_id, 
  count(distinct ga_session_id) as total_sessions,
  min(ga_session_id) as min_session_id, #why min? because we want details corresponding to this from session details table.
  min(user_first_touch_timestamp) as first_touch_ts,
  max(case when event = 'first_visit' then 1 else 0 end) as first_visit_event_flag
 from
  `temp_aadi.events_all_v0_2`
 group by user_id
),
# we are taking user details for all users - but will only retain for new users. This will be handled in the outermost query
user_details as (
  select us.first_touch_ts, us.first_visit_event_flag, t1.* from (select user_id, ga_session_id, traffic_source, traffic_medium, traffic_name, device_category from `temp_aadi.sessions_v0_1`)t1
  join users us on t1.user_id = us.user_id and t1.ga_session_id = us.min_session_id
)

select * from user_details
