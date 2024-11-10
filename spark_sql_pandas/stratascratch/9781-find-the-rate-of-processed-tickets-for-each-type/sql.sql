select
type,
sum(
case when processed then 1 else 0 end 
)*1.0 /count(*)
from facebook_complaints
group by type
;