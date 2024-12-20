SELECT
	USER_ID,
	AVG(SESSION)
FROM
	(
		SELECT DISTINCT
			ON (USER_ID, TIMESTAMP::DATE) USER_ID,
			TIMESTAMP::DATE,
			MIN(TIMESTAMP) FILTER (
				WHERE
					ACTION = 'page_exit'
			) OVER (
				PARTITION BY
					USER_ID,
					TIMESTAMP::DATE
			) - MAX(TIMESTAMP) FILTER (
				WHERE
					ACTION = 'page_load'
			) OVER (
				PARTITION BY
					USER_ID,
					TIMESTAMP::DATE
			) AS SESSION
		FROM
			FACEBOOK_WEB_LOG
	) T
WHERE
	SESSION IS NOT NULL

