with user_client_percentage as 
(select
distinct on (client_id,user_id)
client_id,user_id,
count(*) filter(where event_type in('video call received', 'video call sent', 'voice call received', 'voice call sent') )
 over(partition by client_id,user_id)*100/count(*) over(partition by client_id,user_id)  as pct_video_voice
from fact_events)
--select * from user_client_percentage
select  client_id,count(*) from user_client_percentage
where pct_video_voice >= 50
group by client_id



---------------------------------------------------
SELECT client_id FROM (SELECT client_id, rank() over (order by count(*) DESC)
FROM fact_events WHERE user_id in
(SELECT user_id FROM fact_events GROUP BY user_id HAVING avg(CASE WHEN event_type
in ('video call received', 'video call sent', 'voice call received', 'voice call sent')
THEN 1 ELSE 0 END) >=0.5) GROUP BY client_id) a WHERE rank = 1

