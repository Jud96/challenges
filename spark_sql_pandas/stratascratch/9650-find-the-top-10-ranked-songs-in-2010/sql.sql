select distinct on(year_rank,group_name,song_name)year_rank,group_name,song_name
from billboard_top_100_year_end
where year = '2010'
limit 10