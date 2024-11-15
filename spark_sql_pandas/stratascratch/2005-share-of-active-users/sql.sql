
SELECT active_users / total_users::float AS active_users_share
FROM
  (SELECT count(user_id) total_users,
          count(CASE
                    WHEN status = 'open' THEN 1
                    ELSE NULL
                END) AS active_users
   FROM fb_active_users
   WHERE country = 'USA') subq

   


select (select  
count(*) count_open_in_usa
from fb_active_users
where country = 'USA' and status='open')*1.0/(select  
count(*) count_in_usa
from fb_active_users
where country = 'USA')