
select 
date_ist,
 user_id, 
 ga_session_id,  
 event,
 event_counter,
 event_ts as page_load_ts,
 page_type,
 page_location, 
 first_page_ref as page_referrer,
 page_num as unique_page_num,
  next_page_location,
  last_event_ts,
  new_page,
  landing_page,
  exit_page
 from
(SELECT
*,
 row_number() over(partition by user_id, ga_session_id, page_num order by event_counter asc) as first_page_cnt
FROM 
  `kerala-ayurveda-wh.temp_aadi.events_mapped_v0_2` 
)
where first_page_cnt = 1
order by user_id, ga_session_id, page_load_ts asc
