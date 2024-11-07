SELECT winery
FROM 
(
  SELECT winery, regexp_split_to_array(lower(description), '[,;\-\.\/ ]+') AS sep_words 
  FROM winemag_p1
) a
WHERE 'plum' = ANY(a.sep_words) or 
      'cherry' = ANY(a.sep_words) or
      'rose' = ANY(a.sep_words) or
      'hazelnut' = ANY(a.sep_words)
order by winery
      
