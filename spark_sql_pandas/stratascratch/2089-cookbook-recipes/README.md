You are given the table with titles of recipes from a cookbook and their page numbers. You are asked to represent how the recipes will be distributed in the book.

Produce a table consisting of three columns: left_page_number, left_title and right_title. The k-th row (counting from 0), should contain the number and the title of the page with the number 2×k2×k in the first and second columns respectively, and the title of the page with the number 2×k+12×k+1 in the third column.

Each page contains at most 1 recipe. If the page does not contain a recipe, the appropriate cell should remain empty (NULL value). Page 0 (the internal side of the front cover) is guaranteed to be empty.

```sql
WITH RECURSIVE cte AS (
    SELECT 0 AS left_page_number, NULL AS left_title, NULL AS right_title
    UNION ALL
    SELECT left_page_number + 1, right_title, NULL
    FROM cte
    WHERE left_page_number < (SELECT MAX(page_number) FROM cookbook_recipes)
) select left_page_number, left_title, right_title from cte
left join cookbook_recipes
on left_page_number*2 = page_number