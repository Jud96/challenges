select student_id from sat_scores
where
sat_writing = (
select PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY sat_writing)
from sat_scores
)