SELECT
	DATE,
	SUM(ACCEPTED) * 1.0 / COUNT(*) AS PERCENTAGE_ACCEPTANCE
FROM
	(
		SELECT
			USER_ID_SENDER,
			USER_ID_RECEIVER,
			MAX(
				CASE
					WHEN ACTION = 'accepted' THEN 1
					ELSE 0
				END
			) AS ACCEPTED,
			MIN(DATE) AS DATE
		FROM
			FB_FRIEND_REQUESTS
		GROUP BY
			USER_ID_SENDER,
			USER_ID_RECEIVER
	) T
GROUP BY
	DATE