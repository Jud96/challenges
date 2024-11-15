WITH RECURSIVE cte AS (
    SELECT 0 AS left_page_number, NULL AS left_title, NULL AS right_title
    UNION ALL
    SELECT left_page_number + 1, right_title, NULL
    FROM cte
    WHERE left_page_number < (SELECT MAX(page_number) FROM cookbook_titles)
) select cte.left_page_number,
cr2.title as left_title,
cr.title as right_title from cte
left join cookbook_titles cr on cte.left_page_number = cr.page_number - 1 
left join cookbook_titles cr2
 on 
cte.left_page_number = cr2.page_number 
 where left_page_number%2 = 0
 order by left_page_number