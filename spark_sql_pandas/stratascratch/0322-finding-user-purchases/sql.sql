SELECT DISTINCT 
    user_id
FROM (
    SELECT 
        user_id,
        created_at,
        LAG(created_at, 1) OVER (PARTITION BY user_id ORDER BY created_at ASC) AS prev_created_at,
        DATEDIFF(created_at, LAG(created_at, 1) OVER (PARTITION BY user_id ORDER BY created_at ASC)) AS days_diff
    FROM 
        amazon_transactions
) t
WHERE 
    days_diff IS NOT NULL AND days_diff <= 7;
-- rank 2 - rank 1 by user created_at if less then 7 day 