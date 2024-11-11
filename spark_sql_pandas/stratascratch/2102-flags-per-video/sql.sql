select video_id,count(distinct concat(user_firstname,user_lastname)) from user_flags
where flag_id is not null
group by video_id
