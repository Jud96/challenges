WITH
	MERGED AS (
		SELECT
			LANGUAGE,
			DEVICE,
			USER_ID
		FROM
			PLAYBOOK_EVENTS
			JOIN PLAYBOOK_USERS USING (USER_ID)
	)
SELECT
	LANGUAGE,
	COALESCE(N_APPLE_USERS, 0),
	COALESCE(N_TOTAL_USERS, 0)
FROM
	(
		SELECT
			LANGUAGE,
			COUNT(DISTINCT USER_ID) AS N_APPLE_USERS
		FROM
			MERGED
		WHERE
			DEVICE IN ('macbook pro', 'iphone 5s', 'ipad air')
		GROUP BY
			LANGUAGE
	) T
	FULL OUTER JOIN (
		SELECT
			LANGUAGE,
			COUNT(DISTINCT USER_ID) N_TOTAL_USERS
		FROM
			MERGED
		GROUP BY
			LANGUAGE
	) T2 USING (LANGUAGE)
ORDER BY
	N_TOTAL_USERS DESC