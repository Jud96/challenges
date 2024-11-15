WITH CTE AS (
    SELECT
        USER_ID,
        PRODUCT_ID,
        CREATED_AT,
        CASE 
            WHEN MIN(CREATED_AT) OVER(PARTITION BY USER_ID) = CREATED_AT 
            THEN TRUE 
            ELSE FALSE 
        END AS FIRST_PURCHASE
    FROM MARKETING_CAMPAIGN
),
F_PURCHASE AS (
    SELECT 
        USER_ID,
        ARRAY_AGG(PRODUCT_ID) AS FIRST_PRODUCT_IDS
    FROM CTE
    WHERE FIRST_PURCHASE = True
    GROUP BY USER_ID
)
SELECT  count(distinct F_PURCHASE.USER_ID)
FROM (SELECT * FROM CTE WHERE FIRST_PURCHASE = False) NFP
Right JOIN F_PURCHASE
    ON F_PURCHASE.USER_ID = NFP.USER_ID
WHERE NFP.PRODUCT_ID != ALL(F_PURCHASE.FIRST_PRODUCT_IDS)




-- another solution
SELECT DISTINCT user_id
FROM marketing_campaign
WHERE user_id in
    (SELECT user_id
     FROM marketing_campaign
     GROUP BY user_id
     HAVING count(DISTINCT created_at) > 1
     AND count(DISTINCT product_id) > 1)
  AND concat((user_id),'_', (product_id)) not in
    (SELECT user_product
     FROM
      (SELECT *,
              rank() over(PARTITION BY user_id
                          ORDER BY created_at) AS rn,
              concat((user_id),'_', (product_id)) AS user_product
        FROM marketing_campaign) x
     WHERE rn = 1 )