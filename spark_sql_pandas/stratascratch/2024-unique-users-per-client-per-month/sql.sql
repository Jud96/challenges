select 
client_id,Extract(month from time_id),count(distinct user_id)
from fact_events
group by
client_id,Extract(month from time_id)